/** Backup manager (c) Brukhanov Alexander
Программа для резервного копирования файлов.
Общие сведения:

Условно, можно разделить на три части:
1. Клиентская часть. Отвечает за получение списка файлов и, если это необходимо, данных которые в них содержатся
2. Серверная часть. На основе данных полчаемых от клиентской части программы создает резервные копии файлов.
3. Третья часть отвечает за извлечение файлов из резервных копий.

Взаимодействие клиента и сервера построено так, чтобы можно было реализовать через один поток (pipe).

Клиент отправлет в поток список файлов. Если он встречает файл ненулевой длинны, то он ждет на входе ответ.
Если ответом будет строка "data\n", следом за информацией о файле будет записано его содерживое в формате:
[<N = размер блока (4 байта)> <N байтов данных>] ...
Последний блок должен иметь нулевой размер. Если размер данных будет отличаться от заявленого размера файла -
размер файла будет скоректированн.

Сервер, при получении в списке файла с ненулевой длинной, ищет в предыдущей резервной копии файл с таким же
размером и датой модификации. Если файл найден, в результирующий листинг может быть вставлена ссылка на этот
файл в предыдущей резервной копии или же файл будет взят из этой копии и перепакован заново. Сервер сохраняет
запакованные при помощи gzip данные в различные файлы. Смещение файла в файле данных не может превышать MAXOFFSET
байт. Если данных больше - создается новый файл хранилища. Файлы с размерами большими MINSIZE пакуются отдельно.
Если посленяя резервная копия использует файл хранилища целиком, то во вновь созденном хранилище создается
жесткая ссылка (hardlink) на этот файл. Если файл используется частично, то результирующий листинг может быть
помещена ссылка на этот файл в предыдущем хранилище (если его использование разрешено) или перепакован в новое
хранилище из старого.

Общие параметры:
-g <файл>	Путь до файла конфигурации. В этот файл могут быть помещены параметры: exclude, archives,
		minsize, maxoffset and timeout. Файл не должен содержать пустых строк, лишних пробелов и комментариев.
		(По умолчанию /usr/local/ispmgr/etc/ispbackup.conf) Пример:
		exclude=./var/spool/mqueue
		archives=.tar.gz
		archives=.rar
		minsize=1048576
		maxoffset=100000000
		timeout=60

-C <каталог>	Изменяет рабочий каталог программы. Каталог изменяется сразу, поэтому очень важно в какой части
		списка параметров он использован.
-D <файл>	Путь до файла, куда будет записана отладочная информация. (По умолчанию stderr)
-d <уровень>	Debug level. 0 - минимальный (только критические ошибки), 9 - максимальный. (По умолчанию: 1)

-o <файл>	Использовать этот файл вместо stdout
-i <файл>	Использовать этот файл вместо stdin
-u <секунд>	timeout для операций чтения. 0 - отключает timeout (По умолчанию: 60)
-8		Не исключать приватные файлы из архивной копии. Если этот параметр не задан, ispbackup будет игнорировать
		файлы с флагом NODUMP, а при восстановлении всегда предварительно удалять существующий каталог, если у
		каталога стоял флаг OPAQUE

Параметры клиента:
-c		Выполнять программу в режиме клиента (для получения листинга файлов и данных)
-f		Не ждать ответа от сервера даже если размер файла больше 0.
-x <путь>	Иключить файлы, имя которых начинается с этой строки из листинга (exclude в файле конфигурации)
		Параметр может быть использован несколько раз.

Параметры сервера:
-s <file>	Выполнять программу в режиме  сервера. File - путь до коммандного потока (должен быть использован
		как входной поток для клиента)
-t <ext>	Расширение файлов листинга
-l <path>	Путь до листинга от предыдущей резервной копии. Если это каталог, будут проверяться все файлы .lst
		в нем и использован последний (самый свежий). Если файл - он будет использован как листинг.
-n <file>	Путь до первого файла хранилища. Для получения имени следующего файла цифры в имени будут увеличины
		на 1. Например: 0000/99 -> 0001/00 . Если в пути будет использованна последовательность "//",
		то никаких ссылок на предыдущие хранилища не будут сделаны, если начало их пути не совпадает с началом
		этого пути взятого до этой последовательности.
-S <size>	maxoffset. Максимальное смещение файла в файле хранилища.
-m <size>	minsize. Минимальный размер файла, которых может быть сохранен как отдельный файл в хранилище.
-A <ext>	Расширения файлов, которые не надо запаковывать (archive в файле конфигурации).

-R <file>	Удалить старую резервную копию. File - путь до листинга. Никакие ссылки не будут сделаны на файлы
		хранилища этой резервной копии. Этот параметр может быть использован отдельно, для безопасного
		освобождения места.
-r <count>	Оставить только count резевных копий (включая новую). Все старые разарвные копии будут удалены (см. -R)
-T		Режим проверки хранилища. Никакие данные из предыдущих копий не будут использованы, но все
		файлы для которых совпадут имя, время модификации и размер будут сравнены.

Если не использованно ни -c ни -s программа будет работать в режиме восстановления.
Первый параметр - это должен быть путь до листинга.
-F		Не истанавливаться при ошибках.
-B <dir>	Добавлять к именам файлов хранилища указанную строку. Это может быть путь или URL.
-X <file>	Извеченные из резервной копии данные будут помещены в tar архив с именем file.
-z		Tar архив (см. -X) будте запакован при помощи gzip
-q		Выдавать запрос на перезапись существующих файлов
-Q		Перезаписывать существующие файлы
-U		Удалять существующие файлы
-K		Оставлять существующие файлы без изменений

Пример:
	mkfifo cmd
	cat cmd | ssh server '/usr/local/ispmgr/sbin/ispbackup -c -C /vs/private/1.1.1.1 .' |
		 /usr/local/ispmgr/sbin/ispbackup -s cmd > 2009-01-01.lst
	Сделает резервную копию каталога /vs/private/1.1.1.1 на сервере server. Листинг будет сохранен в файл
	2009-01-01.lst. Файлы хранилища будут также сохранены в текущий каталог с именами 00000001, 00000002 и т.д.

	/usr/local/ispmgr/sbin/ispbackup -B backup.tgz -z 2009-01-01.lst ./home
	Извлечет каталог home из резервной копии с листингом 2009-01-01.lst и поместит денные в tar архив backup.tgz
 */

#undef _FILE_OFFSET_BITS
#include <sys/types.h>
#include <sys/stat.h>
#include <fts.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/param.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include <dirent.h>

#include <sys/time.h>
#include <signal.h>

#include <zlib.h>
#include <curl/curl.h>
#include <regex.h>

#include <map>
#include <set>
#include <string>
#include <iostream>
#include "classes.h"
#include <limits.h>
using std::string;

typedef std::multimap<time_t, string> StringMMap;
typedef StringMMap::reverse_iterator rsmmPtr;

typedef std::set<string> StringSet;
typedef StringSet::iterator ssPtr;

typedef std::map<string, string> StringMap;
typedef StringMap::iterator smPtr;

struct LinkData {
	string name;
	int links;
};

typedef std::map<ino_t, LinkData> LinkMap;
typedef LinkMap::iterator ldPtr;

struct CacheData {
	string key;
	string file;
	long long pos;
	long long size;
};

typedef std::map<string, CacheData> CacheMap;
typedef CacheMap::iterator cdPtr;

struct OldFiles {
	string name;
	long long used;
	long long size;
	int count;
};

typedef std::map<string, OldFiles> FilesMap;
typedef FilesMap::iterator fmPtr;

const char *USAGE = "Files backup program (c) Brukhanov Alexander\nUsage: %s [options]\n\n\
General information:\n\
It can be conventionally divided into:\n\
1.Client part. It is  used to get files list and included files, if necessary.\n\
2.Server part. Creates file backups based on the data  from the client part . \n\
3.Provides files extraction from the backup copies.\n\
\n\
Interconnection of client and server is represented in the way of one stream - a pipe.\n\
\n\
A client sends the file list in to the pipe. If it meets a nonzero length file, it\n\
waits for an answer on the output. If the answer is  the \"data\\n\" line, file data\n\
will be followed by its content description in the format\n\
[<N = size block (4 b)> <N byte data>] ...\n\
The last block must be zero-sized. If the data size is different from the stated file\n\
size, the file size will be corrected.\n\
\n\
When getting nonzero length file in the list, server looks for the file of the same size\n\
and modification date in the previous backup copy.  If the file is found, the resulting\n\
listing may include a link to this file in the previous backup copy or the file will be\n\
taken from this copy and repacked. Server stores data packed by gzip in different files.\n\
File offset in the data file cannot exceed MAXOFFSET byte. In case of larger amount of\n\
data a new storage file is created. Files with size more than MINSIZE are packed separately.\n\
If the last backup copy uses the whole storage file, a hardlink to this file is created\n\
in the new storage. If the file is used partially, the resulting listing may contain a\n\
link to this file in the previous storage (if its usage is permitted) or repacked from\n\
the previous storage to the current one.\n\
\n\
Common parameters:\n\
  -g <file>	Path to configuration file. This file may include: exclude, archives,\n\
		minsize, maxoffset and timeout. It must not contain empty lines, extra\n\
		gaps and comments. (By default /usr/local/ispmgr/etc/ispbackup.conf)\n\
		For example:\n\
			exclude=./var/spool/mqueue\n\
			archives=.tar.gz\n\
			archives=.rar\n\
			minsize=1048576\n\
			maxoffset=100000000\n\
			max_dirsize=100000\n\
			timeout=60\n\
\n\
  -C <folder>	Changes program work folder. The folder changes immediately, hence it is\n\
		extremely important in which part of the parameters list it was  used.\n\
  -D <file>	Path to file, where debugging information will be put to. (By default: stderr)\n\
  -d <level>	Debug level. 0 - minimum (fatal errors only), 9 - maximum. (By default: 1)\n\
  -o <file>	Use this file instead of stdout\n\
  -i <file>	Use this file instead of stdin\n\
  -u <seconds>	Timeout for reading operation. 0 - enables timeout (By default: 60)\n\
  -8		Do not exclude private files from the backup. If this parameters is not\n\
		specified, ispbackup will ignore the NODUMP files. When restoring it will\n\
		always delete the existing directory, if it was checked as OPAQUE.\n\
\n\
Clients parameters:\n\
  -c		Run the program in the client mode (for getting file and data listing)\n\
  -f		Do not wait for server response even if the file size is more than 0.\n\
  -x <path>	Exclude files, which names begin with this line from the listing (exclude\n\
		in configuration file). The parameter can be used several times.\n\
\n\
Server parameters:\n\
  -s <file>	Run the program in the server mode. File — path to command pipe (should be\n\
		used as the input pipe for the client)\n\
  -t <ext>	Listing files extension (By default: .lst)\n\
  -l <path>	Path to the listing of the previous backup copy. In case of a folder, all\n\
		the .lst files  included into it will be checked and with the latest used.\n\
		In case of file, it will be used as listing.\n\
  -n <file>	Path to the first storage file. To obtain the name of the following file,\n\
		digits in the name will be increased by 1. For example: 0000/99 -> 0001/00\n\
		If the consequence  \"//\" is used in the path, no links to the previous\n\
		storages will be created, if their  path beginning does not coincide with\n\
		the beginning of this path taken before its consequence. (By default: 00000000)\n\
  -S <size>	Maxoffset. Maximum file offset in the file storage.\n\
  -m <size>	Minsize. Minimum file size which can be stored as a separate file in the storage.\n\
  -A <ext>	Files extension which shouldn't be packed  (archive in config file).\n\
  -R <file>	Delete previous backup copy. File — path to the listing. No links will be\n\
		created to the storage files  of this backup copy. This  parameter can be\n\
		used separately for save space release.\n\
  -r <count>	Leave only count backup copies (new included). All the previous backup copies\n\
		well be removed  (see -R)\n\
  -T		Storage check mode. No data from the previous copies will be used, but all the\n\
		files for which names , modification time and size  coincide will be compared.\n\
\n\
If not, neither  – с nor  -c  program will work in the recovery mode. The first parameter\n\
should be the path to listing. Any common or following options can be used before first parameter.\n\
  -F		Not stop in case of errors.\n\
  -B <dir>	Add a specified line to archive file names. It can be a path or URL.\n\
		You can use BACKUP_BASEDIR enviroument variable instead of this parameter.\n\
  -X <file>	Data extracted from the backup copy will be put to the tar archive with\n\
		this file name.\n\
  -z		The tar archive (see -X) will be compressed with gzip\n\
  -q		Send a request  to rewrite existing files\n\
  -Q		Rerwrite existing files\n\
  -U		Delete existing files\n\
  -K		Keep existing files untouched\n\
\n\
Examples:\n\
	mkfifo cmd\n\
	cat cmd | ssh server '/usr/local/ispmgr/sbin/ispbackup -c -C /vs/private/1.1.1.1 .' | \\\n\
			/usr/local/ispmgr/sbin/ispbackup -s cmd > 2009-01-01.lst\n\
\n\
	Creates backup copy of the folder  /vs/private/1.1.1.1 on the server. The listing will be\n\
	saved to the file 2009-01-01.lst. Storage files will also be saved in the current field\n\
	with the names  00000001, 00000002, etc.\n\
\n\
	/usr/local/ispmgr/sbin/ispbackup -In backup.tgz -z 2009-01-01.lst ./home\n\
	Extracts the folder ./home from the backup copy with listing 2009-01-01.lst and puts the\n\
	data into the tar archive backup.tgz\n";


long long MAXOFFSET = 100 * 1024 * 1024;
long long MINSIZE = 1024 * 1024;
long long MAXSIZE = 4ll << 30; // 4Gb

static char * rstrchr(const char * str, char ch, char * end = 0)
{
	if (!end)
		end = (char *)str + strlen(str) - 1;

	while (*end != ch && end > str) end--;
	return end;
}

int debuglevel = 0;
FILE * debug = stderr;
enum {
	DL_ERROR = 0,
	DL_WARNING = 1,
	DL_INFO = 2,
	DL_TRACE = 3,
	DL_TRACE1 = 4,
	DL_TRACE2 = 5,
	DL_TRACE3 = 6,
	DL_DEBUG = 9
};

void Debug(int level, const char * format, ...)
{
	if (level > debuglevel)
		return;
	va_list ap;
	va_start(ap, format);
	fprintf(debug, "%llu %d ", (unsigned long long)time(0), level);
	vfprintf(debug, format, ap);
	fprintf(debug, "\n");
}

static string getword(char ** data, char delim = '\t')
{
	char * pos = strchr(*data, delim);
	if (!pos)
		pos = *data + strlen(*data);
	string res;
	res.assign(*data, pos - *data);
	if (*pos)
		pos++;
	*data = pos;
	return res;
}

static string str(unsigned long long dat)
{
	char buf[32];
	snprintf(buf, sizeof(buf), "%llu", dat);
	return buf;
}

static bool timeout_detected = false;
static void timeout(int sig)
{
	timeout_detected = true;
	//Debug(DL_ERROR, "Timeout during reading next file from client");
}

static char * PROG;
class Error {
protected:
	Error() {}
	void mkmsg(const char * format, va_list ap) {
		char * res = 0;
		vasprintf(&res, format, ap);
		if (!res)
			res = (char *)strdup("");
		Message = res;
		free(res);
		if (errno) {
			asprintf(&res, ". Error %d %s", errno, strerror(errno));
			if (res) {
				Message += res;
				free(res);
			}
		}
		Debug(DL_DEBUG, "Error: %s", Message.c_str());
	}

public:
	string Message;
	Error(const char * format, ...) {
		va_list ap;
		va_start(ap, format);
		mkmsg(format, ap);
	}
};

class Dummy : public Error {
public:
	Dummy() {}
};

class Usage : public Error {
protected:
	Usage() {}

public:
	Usage(const char * format, ...) {
		va_list ap;
		va_start(ap, format);
		mkmsg(format, ap);
	}
};

static FILE * openfile(const char * path, const char *mode, FILE * def)
{
	FILE * res;
	if (def != stderr)
		Debug(DL_TRACE, "Open file '%s' with mode %s", path, mode);
	if (def && !strcmp(path, "-"))
		res = def;
	else {
		res = fopen(path, mode);
		if (!res)
			throw Usage("Open file '%s'", path);
	}
	return res;
}


static void removefolder(string path)
{
	if (path.empty())
		return;
	struct stat sb;
	while (lstat(path.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode)) {
		Debug(DL_TRACE, "Remove folder '%s'", path.c_str());
		if (rmdir(path.c_str()) != 0)
			break;
		string::size_type pos = path.rfind('/');
		if (pos == string::npos)
			break;
		path.resize(pos);
	}
}

static void createfolder(string path)
{
	if (path.empty())
		return;
	struct stat sb;
	if (lstat(path.c_str(), &sb) == -1) {
		string::size_type pos = path.rfind('/');
		if (pos != string::npos)
			createfolder(path.substr(0, pos));
		Debug(DL_TRACE, "Create folder '%s'", path.c_str());
		mkdir(path.c_str(), 0751);
	} else if (!S_ISDIR(sb.st_mode))
		throw Error("bad folder '%s'", path.c_str());
}


static int TIMEOUT = 60;
static long long MAX_DIRSIZE = LONG_MAX;
#ifdef FreeBSD
static int tryget(char ** buf, size_t * size, FILE * stream)
{
	if (!*buf) {
		*size = MAXPATHLEN * 2 + 1024;
		*buf = (char *)malloc(*size);
	}
	struct itimerval tv;
	tv.it_interval.tv_sec = 0;
	tv.it_interval.tv_usec = 0;
	tv.it_value.tv_usec = 0;
	tv.it_value.tv_sec = TIMEOUT;
	setitimer(ITIMER_REAL, &tv, 0);
	char * res = fgets(*buf, *size, stream);
	tv.it_value.tv_sec = 0;
	setitimer(ITIMER_REAL, &tv, 0);
	if (timeout_detected) {
		timeout_detected = false;
		throw Error("Timeout during reading");
	}
	if (res) {
		int len = strlen(*buf);
		if ((*buf)[len-1] != '\n')
			throw Error("Incomplete line. Last char %d line size %d", (*buf)[len-1], len);
		return len;
	}
	return -1;
}
#else
static int tryget(char ** buf, size_t * size, FILE * stream)
{
	struct itimerval tv;
	tv.it_interval.tv_sec = 0;
	tv.it_interval.tv_usec = 0;
	tv.it_value.tv_usec = 0;
	tv.it_value.tv_sec = TIMEOUT;
	setitimer(ITIMER_REAL, &tv, 0);
	int len = getline(buf, size, stream);
	tv.it_value.tv_sec = 0;
	setitimer(ITIMER_REAL, &tv, 0);
	if (timeout_detected) {
		timeout_detected = false;
		throw Error("Timeout during reading");
	}
	if (len > 0 && (*buf)[len-1] != '\n')
		throw Error("Incomplete line. Last char %d line size %d", (*buf)[len-1], len);
	return len;
}
#endif

class File;

class FileError : public Error {
public:
	File * file;
	FileError(File * f, const char * format, ...): file(f) {
		va_list ap;
		va_start(ap, format);
		mkmsg(format, ap);
	}
};

class File {
protected:
	class data_t {
	protected:
		string fname;
		long long offset;
		long long fsize;
		int count;
	public:
		data_t(const string & name): fname(name), offset(0), fsize(-1), count(1) {}
		virtual ~data_t() {
			if (!fname.empty())
				Debug(DL_TRACE, "Close file '%s'", fname.c_str());
		}
		virtual int write(const char * buf, int size) = 0;
		virtual int read(char * buf, int size) = 0;
		virtual long long seek(long long offs) = 0;
		friend class File;
	};

	data_t * set(data_t * a) {
		if (dat && !--dat->count)
			delete dat;
		dat = a;
		return dat;
	}

	data_t * dat;

public:
	inline string fname() {
		return dat ? dat->fname : "";
	}
	inline long long offset() {
		return dat ? dat->offset : 0;
	}
	inline long long fsize() {
		return dat ? dat->fsize : -1;
	}
	inline bool good() {
		return dat;
	}
	inline void close() {
		set(0);
	}
	void set(const File & a) {
		*this = a;
	}

	File(): dat(0) {}

	File & operator = (const File & a) {
		if (a.dat)
			a.dat->count++;
		set(a.dat);
		return *this;
	}


	virtual ~File() {
		set(0);
	}

	int write(const char * buf, int size) {
		if (!dat)
			throw FileError(this, "Write to closed file");
		Debug(DL_DEBUG, "write %d bytes", size);
		int done = dat->write(buf, size);
		if (size != done)
			throw FileError(this, "Failed to write data to '%s' need = %d done = %d",
			                dat->fname.c_str(), size, done);
		dat->offset += done;
		return done;
	}

	virtual int read(char * buf, int size, bool hsize) {
		if (!dat)
			throw FileError(this, "Read from closed file");
		struct itimerval tv;
		tv.it_interval.tv_sec = 0;
		tv.it_interval.tv_usec = 0;
		tv.it_value.tv_usec = 0;
		tv.it_value.tv_sec = TIMEOUT;
		setitimer(ITIMER_REAL, &tv, 0);
		int done = dat->read(buf, size);
		tv.it_value.tv_sec = 0;
		setitimer(ITIMER_REAL, &tv, 0);
		if (timeout_detected) {
			timeout_detected = false;
			throw FileError(this, "Timeout reading from '%s'", fname().c_str());
		}
		if (done < 0)
			throw FileError(this, "Read '%s' failed", dat->fname.c_str());
		Debug(DL_DEBUG, "read %d bytes", done);
		if (hsize && size != done)
			throw FileError(this, "Failed to read data from '%s' need = %d got = %d",
			                dat->fname.c_str(), size, done);
		dat->offset += done;
		return done;
	}

	void seek(long long offs) {
		if (!dat)
			throw FileError(this, "Seek in closed file");
		if (offs == dat->offset)
			return;
		if (offs < dat->offset)
			Debug(DL_TRACE, "Seek back in file '%s'", dat->fname.c_str());
		dat->offset = dat->seek(offs);
		if (offs != dat->offset)
			throw FileError(this, "Failed to set position in file '%s'", dat->fname.c_str());
	}

};

class RegFile : public File {
private:
	class rdata_t : public data_t {
	public:
		int fd;
		rdata_t(const string & name): data_t(name), fd(-1) {}
		~rdata_t() {
			if (fd != -1)
				::close(fd);
		}
		int write(const char * buf, int size) {
			return ::write(fd, buf, size);
		}
		int read(char * buf, int size) {
			if (fsize == -1) {
				fsize = lseek(fd, 0, SEEK_END);
				Debug(DL_TRACE, "Got size of file '%s' = %llu", fname.c_str(), fsize);
				lseek(fd, offset, SEEK_SET);
			}
			return ::read(fd, buf, size);
		}
		long long seek(long long offs) {
			return lseek(fd, offs, SEEK_SET);
		}
	};

public:
	RegFile() {}

	void open(const string & name, int flags, int mode = 0) {
		if (name == fname())
			return;
		Debug(DL_TRACE, "Open file '%s' with flags 0x%x and mode %o", name.c_str(), flags, mode);
		rdata_t * dat = new rdata_t(name);
		set(dat);
		dat->fd = ::open(name.c_str(), flags, mode);
		if (dat->fd == -1) {
			set(0);
			throw FileError(this, "Failed to open '%s'", name.c_str());
		}
	}

	void open(int fd) {
		rdata_t * dat = new rdata_t("->" + str(fd));
		set(dat);
		dat->fd = fd;
	}

	int fd() {
		return dat ? ((rdata_t *)(dat))->fd : -1;
	}
};

class GzFile : public File {
protected:
	class gzdata_t : public data_t {
	public:
		gzFile file;
		gzdata_t(const string &name): data_t(name), file(0) {}
		~gzdata_t() {
			if (file)
				gzclose(file);
		}
		int write(const char * buf, int size) {
			return ::gzwrite(file, buf, size);
		}
		int read(char * buf, int size) {
			int res = ::gzread(file, buf, size);
			if (gzeof(file) && res >= 0) {
				fsize = offset + res;
				Debug(DL_TRACE, "Detected size of file '%s' = %llu", fname.c_str(), fsize);
			}
			return res;
		}
		long long seek(long long offs) {
			return gzseek(file, offs, SEEK_SET);
		}
	};

public:
	GzFile() {}

	void open(const string & name, const char * mode) {
		if (name == fname())
			return;
		Debug(DL_TRACE, "Open file '%s' with mode '%s'", name.c_str(), mode);
		gzdata_t * dat = new gzdata_t(name);
		set(dat);
		dat->file = gzopen(name.c_str(), mode);
		if (!dat->file) {
			set(0);
			throw FileError(this, "Failed to open '%s'", name.c_str());
		}
	}

	void open(int fd, const char * mode) {
		gzdata_t * dat = new gzdata_t("->" + str(fd));
		set(dat);
		dat->file = gzdopen(fd, mode);
		if (!dat->file) {
			set(0);
			throw FileError(this, "Failed to open '%s'", fname().c_str());
		}
	}
};

class TmpFile : public File {
public:
	long long got;
	TmpFile(): got(0) {}

	int read(char *buf, int s, bool hsize) {
		int sz = File::read(buf, s, hsize);
		got += sz;
		if (got == fsize())
			close();
		return sz;
	}
};

typedef std::map<string, TmpFile> FileMap;
typedef FileMap::iterator fPtr;

string extension = ".lst";
class Dir {
private:
	DIR * dir;
	string name;

public:
	struct dirent * ent;
	struct stat sb;
	Dir(const string & nm): name(nm) {
		dir = opendir(name.c_str());
		if (!dir)
			throw Error("Failed to open folder '%s'", name.c_str());
		name.push_back('/');
	}
	~Dir() {
		if (dir)
			closedir(dir);
	}

	bool read() {
		while ((ent = readdir(dir)) != 0) {
			int len;
			if (stat((name + ent->d_name).c_str(), &sb) == 0 && S_ISREG(sb.st_mode) && sb.st_size > 0 &&
			        (len = strlen(ent->d_name)) > (int)extension.size() &&
			        extension == (ent->d_name + len - extension.size()))
				return true;
		}
		closedir(dir);
		dir = 0;
		return false;
	}
};

char * nextname(char * dataName)
{
	char * p = dataName + strlen(dataName) - 1;
	while (1) {
		if (p < dataName)
			throw Error("Failed to generate next name for '%s'", dataName);
		if (*p >= '0' && *p <= '8') {
			(*p)++;
			break;
		} else if (*p == '9')
			*p = '0';
		p--;
	}
	char * end = rstrchr(dataName, '/');
	string folder;
	folder.assign(dataName, end - dataName);
	createfolder(folder);

	return dataName;
}

void dataopen(GzFile & data, char * dataName)
{
	if (data.fname().empty() || data.offset() > MAXOFFSET) {
		nextname(dataName);
		Debug(DL_INFO, "New data file '%s'", dataName);
		data.open(dataName, "wb9");
	}
}

int size;
char *line = 0;
size_t linesize = 0;

StringSet toforget;
void removefile(const char * arch)
{
	FILE * in = openfile(arch, "r", stdin);
	if (!in)
		throw Usage("Failed to open file list '%s'", arch);

	string folder = arch;
	string::size_type pos = folder.rfind('/');
	if (pos == string::npos)
		folder = "./";
	else
		folder.resize(pos + 1);
	Debug(DL_INFO, "Backup base data folder '%s'", folder.c_str());

	Debug(DL_TRACE, "Get list files to be deleted");
	StringSet files;
	while ((size = tryget(&line, &linesize, in)) > 0) {
		char * end;
		int type = strtol(line, &end, 10);
		if (type == FTS_F) {
			line[size-1] = 0;
			if (line[size-2] == 'e')
				continue;
			char * pos = rstrchr(line, '\t', line + size - 2);
			pos--;
			end = rstrchr(line, '\t', pos);
			string name;
			name.assign(end + 1, pos - end);
			if (files.insert(name).second)
				Debug(DL_TRACE, "To remove '%s'", name.c_str());
		}
	}
	fclose(in);

	Debug(DL_INFO, "Remove listing '%s'", arch);
	unlink(arch);

	char cwd[MAXPATHLEN];
	getcwd(cwd, sizeof(cwd));
	chdir(folder.c_str());
	Dir dir(".");
	Debug(DL_TRACE, "Get list of used files");
	while (dir.read()) {
		FILE * in = fopen(dir.ent->d_name, "r");
		if (!in)
			continue;

		Debug(DL_INFO, "Read listing from '%s'", dir.ent->d_name);
		while ((size = tryget(&line, &linesize, in)) > 0) {
			char * end;
			int type = strtol(line, &end, 10);
			if (type == FTS_F) {
				line[size-1] = 0;
				if (line[size-2] == 'e')
					continue;
				char * pos = rstrchr(line, '\t', line + size - 2);
				pos--;
				char * pend = rstrchr(line, '\t', pos);
				string name;
				name.assign(pend + 1, pos - pend);
				ssPtr ptr = files.find(name);
				if (ptr != files.end()) {
					toforget.insert(*ptr);
					files.erase(ptr);
					Debug(DL_TRACE, "Leave '%s'", name.c_str());
				}
			}
		}
		fclose(in);
	}

	for (ssPtr i = files.begin(); i != files.end(); i++) {
		Debug(DL_INFO, "Remove data '%s'", i->c_str());
		unlink(i->c_str());
		string folder = *i;
		string::size_type pos = folder.rfind('/');
		if (pos != string::npos) {
			folder.resize(pos);
			removefolder(folder);
		}
	}
	Debug(DL_INFO, "%d old files to forget it", toforget.size());
	chdir(cwd);
}

/** We need to get list always in same order. It make server faster in repack */
#ifdef FreeBSD
typedef const FTSENT * const * ftsent_type;
#else
typedef const FTSENT** ftsent_type;
#endif

static int comparefts(ftsent_type a, ftsent_type b)
{
	//Debug(DL_DEBUG, "compare");
	if ((*a)->fts_info == FTS_ERR || (*b)->fts_info == FTS_ERR)
		return 0;
	return strcmp((*a)->fts_name, (*b)->fts_name);
}

static int CurlFileResult(char *data, size_t size, size_t nmemb, File *writerData)
{
	Debug(DL_DEBUG, "downloaded %d blocks each %d bytes", nmemb, size);
	writerData->write(data, size * nmemb);
	return size * nmemb;
}

static string getword(string & dat)
{
	string::size_type pos = dat.find(' ');
	if (pos == string::npos)
		pos = dat.size();
	string res = dat.substr(0, pos);
	dat.erase(0, pos + 1);
	return res;
}

static size_t CurlHeader(char  *ptr,  size_t  size, size_t  nmemb, File *writerData)
{
	string tmp;
	tmp.assign(ptr, size*nmemb);
	Debug(DL_DEBUG, "Header: '%s'", tmp.c_str());
	string name = getword(tmp);
	if ((name == "HTTP/1.1" || name == "HTTP/1.0") && getword(tmp) != "200")
		throw Error("Download file result error '%s'", name.c_str());

	return size*nmemb;
}

static void copyfile(const string & path, File & out, long long size = -1)
{
	if (path.find(':') != string::npos) {
		Debug(DL_TRACE, "Download file from '%s' to '%s'", path.c_str(), out.fname().c_str());
		CURL * curl;
		CURLcode res = (CURLcode)1;
		curl = curl_easy_init();
		if (curl) try {
				long long offs = out.offset();
				curl_easy_setopt(curl, CURLOPT_URL, path.c_str());
				curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
				curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
				curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlFileResult);
				curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);
				curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, CurlHeader);
				curl_easy_setopt(curl, CURLOPT_HEADERDATA, &out);
				res = curl_easy_perform(curl);
				offs = out.offset() - offs;
				if (size != -1 && offs != size)
					throw Error("Download url '%s' size less then expected %lld bytes", path.c_str(), size - offs);
			} catch (Error & e) {
				curl_easy_cleanup(curl);
				throw e;
			}
		else
			throw Error("Initialize curl error");
		curl_easy_cleanup(curl);
	} else {
		Debug(DL_TRACE3, "Copy file from '%s' to '%s'", path.c_str(), out.fname().c_str());
		char buf[8096];
		RegFile fd;
		fd.open(path, O_RDONLY);
		int got;
		long long rcvd = 0;
		while ((got = out.write(buf, fd.read(buf, sizeof(buf), false))) > 0)
			rcvd += got;
		if (size != -1 && rcvd != size)
			throw Error("Bad file size '%s' '%s' missed %lld\n", out.fname().c_str(), path.c_str(), size - rcvd);
	}

}

struct TarHeader {
	char name[100];
	char mode[8];
	char uid[8];
	char gid[8];
	char size[12];
	char mtime[12];
	char chksum[8];
	char typeflag;
	char linkname[100];
	char magic[6];
	char version[2];
	char uname[32];
	char gname[32];
	char devmajor[8];
	char devminor[8];
	char prefix[155];
	char unused[12];
};

class TarError : public Error {
public:
	TarError(const char * format, ...) {
		va_list ap;
		va_start(ap, format);
		mkmsg(format, ap);
	}
};


static void tarhead(File & out, string name,
                    int mode, int uid, int gid, long long size, time_t t, int type, const char *link = 0)
{
    //TODO: Дописать PaxHeader
	TarHeader head;
	Debug(DL_TRACE3, "Tar file '%s' header size = %d", name.c_str(), sizeof(head));
	bzero(&head, sizeof(head));
	if (type == 5)
		name.push_back('/');

	if (name.size() <= sizeof(head.name)) {
		name.copy(head.name, name.size(), 0);
	} else {
		string::size_type pos = name.size() - sizeof(head.name) - 1;
		while (pos < name.size() && name[pos] != '/') pos++;
		if (pos == name.size() || pos > sizeof(head.prefix))
			throw TarError("Name '%s' too long. File was skiped", name.c_str());
        if (size > MAXSIZE)
            throw TarError("File '%s' too big (%lld bytes). File was skiped", name.c_str(), size);
		name.copy(head.prefix, pos, 0);
		pos++;
		name.copy(head.name, name.size() - pos, pos);
	}

	sprintf(head.mode, "%06o ", mode);
	sprintf(head.uid, "%06o ", uid);
	sprintf(head.gid, "%06o ", gid);
	sprintf(head.size, "%011o ", (unsigned int)size);
	sprintf(head.mtime, "%011o ", (unsigned int)t);
	memset(head.chksum, ' ', 8);
	head.typeflag = '0' + type;
	if (link)
		strncpy(head.linkname, link, sizeof(head.linkname));
	sprintf(head.magic, "ustar");
	head.version[0] = '0';
	head.version[1] = '0';
	//sprintf(head.uname, "root");
	//sprintf(head.gname, "wheel");
	sprintf(head.devmajor, "%06o ", (int)0);
	sprintf(head.devminor, "%06o ", (int)0);
	unsigned char * data = ((unsigned char *) & head) + 512;
	int sum = 0;
	while (data > (void *)&head)
		sum += *--data;
	Debug(DL_TRACE3, "Checksum %d %o", sum, sum);
	snprintf(head.chksum, sizeof(head.chksum), "%06o", sum);
	out.write((const char *)&head, sizeof(head));
}

static string escape(string name)
{
	string::size_type pos = name.find('\\');
	while (pos != string::npos) {
		name.insert(pos, "\\");
		pos = name.find('\t', pos + 2);
	}
	pos = name.find('\t');
	while (pos != string::npos) {
		name.replace(pos, 1, "\\t");
		pos = name.find('\t', pos + 2);
	}
	pos = name.find('\r');
	while (pos != string::npos) {
		name.replace(pos, 1, "\\r");
		pos = name.find('\r', pos + 2);
	}
	pos = name.find('\n');
	while (pos != string::npos) {
		name.replace(pos, 1, "\\n");
		pos = name.find('\n', pos + 2);
	}
	return name;
}

static string unescape(string name)
{
	string::size_type pos = name.find('\\');
	while (pos != string::npos) {
		switch (name[pos]) {
		case 'n':
			name.replace(pos, 2, "\n");
			break;
		case 'r':
			name.replace(pos, 2, "\r");
			break;
		case 't':
			name.replace(pos, 2, "\t");
			break;
		case '\\':
			name.erase(pos, 1);
			break;
		default:
			Debug(DL_WARNING, "Escaped char '%c'", name[pos]);
			name.erase(pos, 1);
		}
		pos = name.find('\\', pos + 1);
	}
	return name;
}

string ask(const string & question)
{
	string res;
	std::cout << question;
	std::cin >> res;
	res.resize(res.size() - 1);
	return res;
}

void removefile(const string & name)
{
	struct stat sb;
	if (lstat(name.c_str(), &sb))
		return;

	if (S_ISDIR(sb.st_mode)) {
		DIR * dir = opendir(name.c_str());
		if (dir == 0)
			return;
		struct dirent * ent;
		readdir(dir);
		readdir(dir);
		while ((ent = readdir(dir)) != 0)
			removefile(name + "/" + ent->d_name);
		closedir(dir);
		rmdir(name.c_str());
	} else
		unlink(name.c_str());
}

typedef std::map<string, regex_t> Exclude;
typedef Exclude::iterator ExcludePtr;
int main(int argc, char * argv[])
{
	if (argc == 2 && !strcmp(argv[1], "-T")) {
		printf ("%s", UPDATE_CONTROL);
		return EXIT_SUCCESS;
	}
	try {
		struct stat sb;
		FILE * listing = 0;
		FILE * command = 0;
		FILE * input = stdin;
		FILE * output = stdout;
		FILE * tarfile = 0;
		bool compress = false;
		bool client = false;
		bool server = false;
		bool waitresp = true;
		bool backup = false;
		bool forse = false;
		bool testmode = false;
		int unlinkfirst = 0;
		char * removearch = 0;
		char * removearchf = 0;
		int removecount = 0;
		char * dataName = 0;
		char * config = (char *)"/usr/local/ispmgr/etc/ispbackup.conf";
		char * basedir = getenv("BACKUP_BASEDIR");
#ifdef FreeBSD
#if OS_VER >= 80000
		bool skipcommon = true;
#else
		bool skipcommon = false;
#endif
#endif
		int ch;
		PROG = argv[0];
		Exclude exclude;
		//StringSet exclude;
		StringSet arch;
		while ((ch = getopt(argc, argv, "g:hHC:D:d:i:o:s:cfan:S:l:x:Fr:R:A:m:t:TB:X:zu:qQUK8")) != -1)
			switch (ch) {
			case 'u':
				TIMEOUT = atoi(optarg);
				break;

			case 'g':
				config = optarg;
				break;

			case 'F':
				forse = true;
				break;

			case 'C':
				if (chdir(optarg) == -1)
					throw Usage("Failed to change working dir to '%s'", optarg);
				break;

			case 'D':
				debug = openfile(optarg, "a", stderr);
				break;

			case 'd':
				debuglevel = atoi(optarg);
				break;

			case 'i':
				input = openfile(optarg, "r", stdin);
				break;

			case 'o':
				output = openfile(optarg, "w", stdout);
				break;

			case 'x':
				//exclude.insert(optarg);
				if (exclude.find(optarg) == exclude.end() && regcomp(&exclude[optarg], optarg, REG_EXTENDED | REG_NOSUB))
					exclude.erase(optarg);
				break;

			case 'h':
			case 'H':
				printf(USAGE, PROG);
				return 0;
			case 'R':
				removearch = optarg;
				break;
#ifdef FreeBSD
			case '8':
				skipcommon = !skipcommon;
				break;
#endif

			default:
				if (client) {
					if (ch == 'f') {
						waitresp = false;
						break;
					} else if (ch == 'a') {
						backup = true;
						break;
					}
				} else if (server) {
					if (ch == 'T') {
						testmode = true;
						break;
					} else if (ch == 'r') {
						if (!removearchf)
							throw Usage("You must use -l to set folder");
						removecount = atoi(optarg);
						if (removecount == 0)
							throw Usage("You must specify positive value archives to leave");
						break;
					} else if (ch == 't') {
						extension = optarg;
						break;
					} else if (ch == 'A') {
						arch.insert(optarg);
						break;
					} else if (ch == 'm') {
						MINSIZE = strtoll(optarg, 0, 10);
						break;
					} else if (ch == 'S') {
						MAXOFFSET = strtoll(optarg, 0, 10);
						break;
					} else if (ch == 'n') {
						dataName = strdup(optarg);
						break;
					} else if (ch == 'l') {
						if (stat(optarg, &sb) != 0)
							throw Usage("File '%s' not found", optarg);

						if (S_ISDIR(sb.st_mode)) {
							Debug(DL_INFO, "Scan folder '%s'", optarg);
							Dir dir(optarg);
							time_t last = 0;
							string file;
							while (dir.read()) {
								if (dir.sb.st_mtime > last) {
									Debug(DL_TRACE, "Latest file '%s' with mtime %llu",
									      dir.ent->d_name, (long long)dir.sb.st_mtime);
									file = dir.ent->d_name;
									last = dir.sb.st_mtime;
								} else
									Debug(DL_TRACE, "Skip file '%s'", dir.ent->d_name);
							}
							if (!file.empty()) {
								Debug(DL_INFO, "Latest file '%s'", file.c_str());
								listing = openfile((string(optarg) + "/" + file).c_str(), "r", 0);
							}
							removearchf = optarg;
						} else if (S_ISREG(sb.st_mode)) {
							listing = openfile(optarg, "r", stdin);
						} else
							throw Usage("Bad file name '%s'", optarg);
						break;
					}
				} else {
					switch (ch) {
					case 'B':
						basedir = optarg;
						break;

					case 'X':
						if (unlinkfirst)
							throw Usage("Only one of -q,-U,-X,-Q,-K options can be used");
						tarfile = openfile(optarg, "w", stdout);
						setbuffer(tarfile, 0, 0);
						break;

					case 'z':
						compress = true;
						break;

					case 's':
						server = true;
						command = openfile(optarg, "w", stdout);
						setbuffer(command, 0, 0);
						break;

					case 'c':
						client = true;
						break;

					case 'K':
					case 'U':
					case 'q':
					case 'Q':
						if (unlinkfirst || tarfile)
							throw Usage("Only one of -q,-U,-X,-Q,-K options can be used");
						unlinkfirst = ch;
						break;

					default:
						throw Usage("Unexpected option '%c'", ch);
					}
					break;
				}
				throw Usage("Unexpected option '%c'", ch);
			}

		Debug(DL_INFO, "Config '%s'", config);
		FILE * conf = fopen(config, "r");
		if (conf) {
			Debug(DL_TRACE, "Parse config");
			while ((size = tryget(&line, &linesize, conf)) > 0) {
				line[size-1] = 0;
				if (strncmp(line, "exclude=", 8) == 0) {
					Debug(DL_INFO, "Exclude '%s'", line + 8);
					if (exclude.find(line + 8) == exclude.end() &&
					        regcomp(&exclude[line+8], line + 8, REG_EXTENDED | REG_NOSUB))
						exclude.erase(line + 8);
					//exclude.insert(line + 8);
				} else if (strncmp(line, "archives=", 9) == 0) {
					Debug(DL_INFO, "Archives '%s'", line + 9);
					arch.insert(line + 9);
                } else if (strncmp(line, "minsize=", 8) == 0) {
					MINSIZE = strtoll(line + 8, 0, 10);
					Debug(DL_INFO, "MinSize %llu", MINSIZE);
				} else if (strncmp(line, "maxoffset=", 10) == 0) {
					MAXOFFSET = strtoll(line + 10, 0, 10);
					Debug(DL_INFO, "MaxOffset %llu", MAXOFFSET);
				} else if (strncmp(line, "timeout=", 8) == 0) {
					TIMEOUT = strtoll(line + 8, 0, 10);
				} else if (strncmp(line, "max_dirsize=", 12) == 0) {
					MAX_DIRSIZE = strtoll(line + 12, NULL, 10);
					Debug(DL_INFO, "MaxDirSize %lld", MAX_DIRSIZE);
				} 
			}
			Debug(DL_TRACE, "Config parsed");
			fclose(conf);
		}

		argc -= optind;
		argv += optind;

		setbuffer(output, 0, 0);
		setbuffer(debug, 0, 0);

		struct sigaction act;
		bzero(&act, sizeof(act));
		act.sa_handler = timeout;
		sigaction(SIGALRM, &act, 0);

		if (client) {
			if (argc == 0)
				throw Usage("You must give me at least one path");
			LinkMap links;
			char ** pathlist = (char **)calloc(sizeof(char *), argc + 1);
			for (int i = 0; i < argc; i++)
				pathlist[i] = argv[i];
			pathlist[argc] = 0;
			FTS * fts = fts_open(pathlist, FTS_COMFOLLOW | FTS_PHYSICAL | FTS_XDEV
#ifdef FreeBSD
			                     | FTS_WHITEOUT
#endif
			                     , comparefts);
			FTSENT * ent;
			Debug(DL_TRACE, "Client started");
			while ((ent = fts_read(fts)) != 0) {
				//Debug(DL_DEBUG, "Read '%s'", ent->fts_path);
				if (ent->fts_info == FTS_DP) {
					//get file size
					struct stat *fstat=ent->fts_statp;
					if (fstat->st_size > MAX_DIRSIZE) {
						fts_set (fts, ent, FTS_SKIP);
					}
					continue;
				
				}
				if (ent->fts_info == FTS_D) {
					//get file size
					struct stat *fstat=ent->fts_statp;
					if (fstat->st_size > MAX_DIRSIZE) {
						Debug(DL_TRACE, "Exclude dir '%s' (size %lld greater than %lld bytes) %d\n"
								, ent->fts_path
								, (long long)ent->fts_statp->st_size
								, (long long)MAX_DIRSIZE
								, ent->fts_info);
						fts_set (fts, ent, FTS_SKIP);
						continue;
					}
				}
				if (!exclude.empty()) {
					//ssPtr i = exclude.begin();
					ExcludePtr i = exclude.begin();
					for (; i != exclude.end(); i++)
						if (regexec(&i->second, ent->fts_path, 0, 0, 0) == 0)
							//if (i->compare(0, i->size(), ent->fts_path) == 0)
							break;
					if (i != exclude.end()) {
						Debug(DL_TRACE, "Exclude file '%s' %d\n", ent->fts_path, ent->fts_info);
						fts_set(fts, ent, FTS_SKIP);
						continue;
					}
				}

				// Escape bad chars
				string name = escape(ent->fts_path);

				Debug(DL_TRACE1, "Try to send '%s' %d\n", name.c_str(), ent->fts_info);

#ifdef FreeBSD
				if (ent->fts_info == FTS_W || S_ISWHT(ent->fts_statp->st_mode)) {
					fprintf(output, "%d\t%s\n", FTS_W, name.c_str());

				} else
#endif
					if (ent->fts_info == FTS_SL) {
						char buf[MAXPATHLEN*2];
						int size = readlink(ent->fts_name, buf, sizeof(buf));
						if (size < 0)
							continue;
						buf[size] = 0;
						string linkname = escape(buf);
						fprintf(output, "%d\t%s\t%o\t%d\t%d\t%08x\t%08x\t%s\n", ent->fts_info, name.c_str(),
						        ent->fts_statp->st_mode&07777, ent->fts_statp->st_uid, ent->fts_statp->st_gid,
						        (unsigned int)ent->fts_statp->st_atime, (unsigned int)ent->fts_statp->st_mtime,
						        linkname.c_str());

					} else if (ent->fts_info == FTS_D) {
#ifdef FreeBSD
						fprintf(output, "%d\t%s\t%o\t%d\t%d\t%x\n", ent->fts_info, name.c_str(),
						        ent->fts_statp->st_mode&07777, ent->fts_statp->st_uid,
						        ent->fts_statp->st_gid, ent->fts_statp->st_flags);
#else
						fprintf(output, "%d\t%s\t%o\t%d\t%d\n", ent->fts_info, name.c_str(),
						        ent->fts_statp->st_mode&07777, ent->fts_statp->st_uid, ent->fts_statp->st_gid);
#endif

					} else if (ent->fts_info == FTS_DEFAULT) {

						struct stat fileInfo;
						if (stat(name.c_str(), &fileInfo ) == 0){
							fprintf(output,
									"%d\t%s\t%o\t%d\t%d\t%d\t%d\n",
									ent->fts_info,
									name.c_str(),
									ent->fts_statp->st_mode,
									ent->fts_statp->st_uid,
									ent->fts_statp->st_gid,
									major(fileInfo.st_rdev),
									minor(fileInfo.st_rdev) );
						}

					} else if (ent->fts_info == FTS_F) {
#ifdef FreeBSD
						if (skipcommon && (ent->fts_statp->st_flags&UF_NODUMP) == UF_NODUMP) {
							Debug(DL_TRACE, "Exclude template file '%s'\n", ent->fts_path);
							continue;
						}
#endif
						if (ent->fts_statp->st_nlink > 1) {
							ldPtr ptr = links.find(ent->fts_statp->st_ino);
							if (ptr != links.end()) {
								ptr->second.links--;
								fprintf(output, "%d\t%s\t%s\n", FTS_NS, name.c_str(), ptr->second.name.c_str());
								if (!ptr->second.links)
									links.erase(ptr);
								continue;
							} else {
								LinkData & dat = links[ent->fts_statp->st_ino];
								dat.links = ent->fts_statp->st_nlink - 1;
								dat.name = name;
							}
						}

						fprintf(output, "%d\t%s\t%08x\t%llu\t%o\t%d\t%d\t%08x\n", ent->fts_info, name.c_str(),
						        (unsigned int)ent->fts_statp->st_mtime,
						        (long long unsigned)ent->fts_statp->st_size,
						        ent->fts_statp->st_mode&07777, ent->fts_statp->st_uid, ent->fts_statp->st_gid,
						        (unsigned int)ent->fts_statp->st_atime);

						if (ent->fts_statp->st_size > 0 && waitresp) {
							Debug(DL_TRACE2, "Waiting for answer");
							if (tryget(&line, &linesize, input) < 0)
								throw Error("Failed to get response");
							Debug(DL_TRACE3, "Got answer '%s'", line);
							if (strncmp(line, "data", 4) == 0) {
								Debug(DL_TRACE2, "Send data");
								int fd = open(ent->fts_name, O_RDONLY);
								char buf[8096*10];
								int size;
								long long total = ent->fts_statp->st_size;
								if (fd != -1) {
									while ((size = read(fd, buf, sizeof(buf))) > 0) {
										if (fwrite(&size, sizeof(size), 1, output) != 1)
											throw Error("Send block size for '%s' failed", name.c_str());
										if (fwrite(buf, size, 1, output) != 1)
											throw Error("Send file '%s' failed", name.c_str());
										total -= size;
									}
									if (size < 0)
										Debug(DL_WARNING, "Read file '%s' filed %d %s", name.c_str(), errno, strerror(errno));
									size = 0;
									if (fwrite(&size, sizeof(size), 1, output) != 1)
										throw Error("Send last block size for '%s' failed", name.c_str());

									close(fd);
								} else {
									size = -1;
									if (fwrite(&size, sizeof(size), 1, output) != 1)
										throw Error("Missed file '%s' report failed", name.c_str());
									Debug(DL_WARNING, "File '%s' was removed", name.c_str());
								}
								if (total != 0)
									Debug(DL_WARNING, "File '%s' size less then reported %lld bytes", name.c_str(), total);
#ifdef FreeBSD
								if (backup)
									chflags(name.c_str(), ent->fts_statp->st_flags&~SF_ARCHIVED);
#endif
							}
						}
					}
			}
			fprintf(output, ".\n");
			fts_close(fts);

		} else if (server) {
			if (!dataName)
				dataName = strdup("00000000");
			char * end = strstr(dataName, "//");
			string dataFolder;
			if (end) {
				dataFolder.assign(dataName, end - dataName);
				for (; *end; end++) *end = end[1];
			}
			Debug(DL_INFO, "Backup base folder '%s'", dataFolder.c_str());
			CacheMap cache;
			StringMap oldfiles;
			if (listing) {
				Debug(DL_TRACE, "Parse prev backup listing");
				FilesMap ofiles;
				OldFiles rec;
				rec.used = 0;
				rec.size = 0;
				rec.count = 0;
				while ((size = tryget(&line, &linesize, listing)) > 0) {
					int type = strtol(line, &end, 10);
					if (type == FTS_F) {
						line[size-1] = 0;
						if (line[size-2] == 'e')
							continue;
						end++;
						string name = getword(&end);
						CacheData & dat = cache[name];
						dat.key = getword(&end);
						dat.size = strtoll(end, &end, 10);
						if (line[size-2] == 'a') {
							end = line + size - 4;
							dat.pos = -1;
						} else {
							long long fs = 0, offs = 1;
							for (end = line + size - 2; *end != '\t'; end--) {
								fs += (*end - '0') * offs;
								offs *= 10;
							}
							end--;
							dat.pos = fs;
						}
						char * pos = rstrchr(line, '\t', end);
						dat.file.assign(pos + 1, end - pos);

						fmPtr ptr = ofiles.insert(std::make_pair(dat.file, rec)).first;
						if (dat.pos == -1) {
							ptr->second.used = MAXOFFSET;
							ptr->second.size = MAXOFFSET;
							ptr->second.count++;
						} else {
							ptr->second.used += dat.size;
							ptr->second.count++;
							if (dat.size + dat.pos > ptr->second.size)
								ptr->second.size = dat.size + dat.pos;
						}
						Debug(DL_TRACE3, "Cache add '%s' with key '%s' located '%s':%llu",
						      name.c_str(), dat.key.c_str(), dat.file.c_str(), dat.pos);
					}
				}
				/** Reuse file if it is 100% used (we can't findout real packed data size) and one of the following:
				 * - its contents grater or equal MAXOFFSET
				 * - it is contain only one file that is grater or equal MINSIZE
				 */
				for (fmPtr ptr = ofiles.begin(); ptr != ofiles.end(); ptr++)
					if (ptr->second.used == ptr->second.size &&
					        (ptr->second.size >= MAXOFFSET || (ptr->second.count == 1 && ptr->second.size >= MINSIZE))) {
						Debug(DL_INFO, "Add file '%s' to files that can be used. With %d records in it",
						      ptr->first.c_str(), ptr->second.count);
						oldfiles.insert(std::make_pair(ptr->first, ""));
					} else
						Debug(DL_TRACE, "File '%s' part used in prev backup %d times. size = %llu, used = %llu (%llu)",
						      ptr->first.c_str(), ptr->second.count, ptr->second.size, ptr->second.used,
						      ptr->second.size - ptr->second.used);
				Debug(DL_INFO, "%d old files with valid data. Cache record count %d", oldfiles.size(), cache.size());
				fclose(listing);
			}

			FileMap tmp;
			GzFile repack;	// for repack it is files with long live time
			GzFile data;	// last backup datafile
			Debug(DL_TRACE, "Waiting for data");
			while ((size = tryget(&line, &linesize, input)) > 0 && line[0] != '.') {
				char * end;
				int type = strtol(line, &end, 10);
				if (type == FTS_F) {
					line[size-1] = 0;
					end++;
					string name = getword(&end);
					string key = getword(&end);
					cdPtr ptr = cache.find(name);

					Debug(DL_TRACE3, "Got '%s'", name.c_str());
					File test;
					long long fs = strtoll(end, &end, 10);
					if (ptr != cache.end()) {
						try {
							CacheData cdat = ptr->second;
							cache.erase(ptr);
							if (key == cdat.key && fs == cdat.size) {
								bool notuse = false;
								if (cdat.pos == -1) {
									if (testmode) {
										std::pair<fPtr, bool> res = tmp.insert(std::make_pair(cdat.file, TmpFile()));
										File & dat = res.first->second;
										if (res.second) {
											RegFile t;
											t.open(cdat.file, O_RDONLY);
											dat = t;
										}
										dat.seek(cdat.pos);
										test = dat;
										throw Dummy();
									}
									if (link(cdat.file.c_str(), nextname(dataName)) != 0)
										throw Error("link from '%s' to '%s' failed", cdat.file.c_str(), dataName);
									Debug(DL_INFO, "Unpacked file '%s' reused as '%s'", cdat.file.c_str(), dataName);
									fprintf(output, "%s\t%s\ta\n", line, dataName);
									fprintf(command, "skip\n");
									continue;
								} else {
									smPtr of = oldfiles.find(cdat.file.c_str());
									if (of != oldfiles.end()) {
										if (testmode) {
											std::pair<fPtr, bool> res = tmp.insert(std::make_pair(of->first, TmpFile()));
											File & dat = res.first->second;
											if (res.second) {
												GzFile t;
												t.open(of->first, "rb");
												dat = t;
											}
											dat.seek(cdat.pos);
											test = dat;
											throw Dummy();
										}
										if (of->second.empty()) {
											if (link(of->first.c_str(), nextname(dataName)) != 0)
												throw Error("link from '%s' to '%s' failed",
												            of->first.c_str(), dataName);
											of->second = dataName;
											Debug(DL_INFO, "File '%s' reused as '%s'", of->first.c_str(), dataName);
										}
										fprintf(output, "%s\t%s\t%llu\n", line,
										        of->second.c_str(), cdat.pos);
										fprintf(command, "skip\n");
										continue;
									} else if (toforget.find(cdat.file.c_str()) != toforget.end()) {
										Debug(DL_TRACE3, "Old file '%s' %llu that can not be used contain file '%s'",
										      cdat.file.c_str(), cdat.pos, name.c_str());
										notuse = true;
									}
								}

								if (testmode) {
									std::pair<fPtr, bool> res = tmp.insert(std::make_pair(cdat.file, TmpFile()));
									File & dat = res.first->second;
									if (res.second) {
										GzFile t;
										t.open(cdat.file, "rb");
										dat = t;
									}
									dat.seek(cdat.pos);
									test = dat;
									throw Dummy();
								}
								if (!notuse && cdat.file.compare(0, dataFolder.size(), dataFolder) == 0) {
									Debug(DL_TRACE2, "Skip file '%s' that exists in prev backup '%s' %llu"
									      " (backup contain unused files)",
									      name.c_str(), cdat.file.c_str(), cdat.pos);
									fprintf(output, "%s\t%s\t%llu\n", line, cdat.file.c_str(), cdat.pos);
								} else {
									Debug(DL_TRACE1, "File '%s' exists in '%s' %llu, repack it",
									      name.c_str(), cdat.file.c_str(), cdat.pos);
									std::pair<fPtr, bool> res = tmp.insert(std::make_pair(cdat.file, TmpFile()));
									File & dat = res.first->second;
									if (res.second) {
										GzFile t;
										t.open(cdat.file, "rb");
										dat = t;
									}
									dat.seek(cdat.pos);
									dataopen(repack, dataName);
									long long offs = repack.offset();
									while (fs > 0) {
										char buf[8096];
										int size = dat.read(buf, (int)sizeof(buf) < fs ? sizeof(buf) : fs, true);
										fs -= size;
										repack.write(buf, size);
									}
									fprintf(output, "%s\t%s\t%llu\n", line, repack.fname().c_str(), offs);
								}
								fprintf(command, "skip\n");
								continue;
							}
							Debug(DL_TRACE2, "Cache file mismatch '%s'", name.c_str());
						} catch (FileError & e) {
							if (e.file == &data)
								throw e;
							Debug(DL_WARNING, "Failed to read file from prev backup '%s'", name.c_str());
						} catch (Dummy & e) {
							Debug(DL_TRACE1, "Testing '%s'. It is found in '%s':%llu",
							      name.c_str(), test.fname().c_str(), test.offset());
						} catch (Error & e) {
							Debug(DL_WARNING, "Failed to use old file: %s", e.Message.c_str());
						}
					}
					Debug(DL_DEBUG, "End '%s'", end);
					if (fs > 0) {
						Debug(DL_DEBUG, "Wait data");
						fprintf(command, "data\n");
						Debug(DL_DEBUG, "Read data");

						ssPtr ptr;
						if (fs > MINSIZE) {
							ptr = arch.begin();
							for (; ptr != arch.end(); ptr++)
								if (name.size() > ptr->size() &&
								        name.compare(name.size() - ptr->size(), ptr->size(), *ptr) == 0)
									break;
						} else
							ptr = arch.end();

						File outfile;
						string offs;
						if (ptr != arch.end()) {
							RegFile fd;
							Debug(DL_TRACE3, "Archive file '%s' leave it unpacked", name.c_str());
							fd.open(nextname(dataName), O_WRONLY | O_CREAT | O_TRUNC, 0644);
							outfile = fd;
							offs = "a";

						} else if (fs >= MAXOFFSET) {
							GzFile fd;
							Debug(DL_TRACE3, "Long file '%s' pack it in single file", name.c_str());
							fd.open(nextname(dataName), "wb9");
							outfile = fd;
							offs = "0";

						} else {
							dataopen(data, dataName);
							outfile = data;
							offs = str(data.offset());
						}
						long long rcvd = 0;
						try {
							while (1) {
								char buf[8096*10];
								int exp;
								if (fread(&exp, sizeof(exp), 1, input) != 1)
									throw Error("Failed to get block size for '%s'", name.c_str());
								if (exp == -1) throw Dummy();
								if (exp == 0) break;
								while (exp) {
									int size = fread(buf, 1, (int)sizeof(buf) > exp ? exp : sizeof(buf), input);
									if (size <= 0)
										throw Error("Failed to get block for '%s'", name.c_str());
									if (test.good()) {
										char tbuf[8096*10];
										int got = test.read(tbuf, size, false);
										if (got != size)
											Debug(DL_ERROR, "Test filed for '%s' file. Read error '%s':%llu",
											      name.c_str(), test.fname().c_str(), test.offset());
										else if (memcmp(tbuf, buf, size))
											Debug(DL_ERROR, "Test filed for '%s' file '%s':%llu mismatch",
											      name.c_str(), test.fname().c_str(), test.offset());
									}
									outfile.write(buf, size);
									exp -= size;
									rcvd += size;
								}
							}
							if (fs != rcvd) {
								Debug(DL_WARNING, "Recieved bytes count for '%s' less then expected %lld",
								      name.c_str(), fs - rcvd);
								fprintf(output, "%d\t%s\t%s\t%llu%s\t%s\t%s\n", type, name.c_str(),
								        key.c_str(), rcvd, end, outfile.fname().c_str(), offs.c_str());
							} else
								fprintf(output, "%s\t%s\t%s\n", line, outfile.fname().c_str(), offs.c_str());
						} catch (Dummy & e) {
							Debug(DL_WARNING, "File '%s' was removed", name.c_str());
						}

					} else
						fprintf(output, "%s e\n", line);
				} else
					fprintf(output, "%s", line);
			}
			if (line[0] != '.')
				throw Error("Incomplete data from client. Last line '%s'", line);
			Debug(DL_TRACE, "No data left");

		} else if (!removearch) {
			if (!argc)
				throw Usage("At least one parameter expected");

			FILE * in = openfile(argv[0], "r", stdin);
			string folder;
			if (basedir)
				folder = basedir;
			else {
				folder = argv[0];
				string::size_type pos = folder.rfind('/');
				if (pos == string::npos)
					folder.clear();
				else
					folder.resize(pos + 1);
			}
			Debug(DL_INFO, "Backup base data folder '%s'", folder.c_str());

			argv++;
			argc--;

			StringSet valid;
			for (; argc; argc--, argv++) {
				string dat = *argv;
				while (dat.size() && dat[dat.size()-1] == '/') dat.resize(dat.size() - 1);
				valid.insert(dat);
				Debug(DL_INFO, "Adding file to list: '%s'", dat.c_str());
			}

			FileMap data;
			File tar;
			if (tarfile) {
				if (compress) {
					GzFile t;
					t.open(fileno(tarfile), "wb9");
					tar = t;
				} else {
					RegFile t;
					t.open(fileno(tarfile));
					tar = t;
				}
			}
			string tmpname = "../backup." + str(getpid());
			Debug(DL_TRACE, "Parse backup listing");
			while ((size = tryget(&line, &linesize, in)) > 0) {
				line[size-1] = 0;
				char * end;
				int type = strtol(line, &end, 10);
				end++;
				string name = unescape(getword(&end));

				Debug(DL_TRACE3, "Got '%s'", name.c_str());
				if (!valid.empty()) {
					ssPtr i = valid.begin();
					for (; i != valid.end(); i++)
						if (name.compare(0, i->size(), *i) == 0)
							break;
					if (i == valid.end()) {
						Debug(DL_DEBUG, "Exclude file '%s'", name.c_str());
						continue;
					}
				}
				Debug(DL_TRACE1, "Extract '%s' type %d", name.c_str(), type);

				try {
					if (!tarfile) {
						string::size_type fpos = name.rfind('/');
						if (fpos != string::npos) {
							string folder = name.substr(0, fpos);
							createfolder(folder);
						}
						struct stat sb;
						if (lstat(name.c_str(), &sb) == 0) {
							if (unlinkfirst == 'K')
								continue;
							if ((!S_ISDIR(sb.st_mode) || type != FTS_D)) {
								switch (unlinkfirst) {
								case 'q':
									if (ask("Do you want to overwrite file \"" + name +
									        "\"? (type \"yes\" to continue)") != "yes")
										continue;
								case 'U':
									removefile(name);
									break;
								case 'Q':
									if (!S_ISREG(sb.st_mode) || type != FTS_F)
										removefile(name);
								}
							}
						}
					}

					if (type == FTS_F) {
						struct timeval tvp[2];
						tvp[1].tv_sec = strtoll(end, &end, 16);
						tvp[1].tv_usec = 0;
						end++;
						long long size = strtoll(end, &end, 10);
						end++;
						int mode = strtol(end, &end, 8);
						end++;
						int uid = strtol(end, &end, 10);
						end++;
						int gid = strtol(end, &end, 10);
						end++;
						tvp[0].tv_sec = strtoll(end, &end, 16);
						tvp[0].tv_usec = 0;
						end++;
						RegFile out;
						if (tarfile) {
							tarhead(tar, name, mode, uid, gid, size, tvp[1].tv_sec, 0);
							out.set(tar);
						} else {

							try {
								out.open(name, O_WRONLY | O_CREAT | O_TRUNC, mode);
							} catch (FileError & err) {
								//
								//try unlink and reopen
								Debug(DL_TRACE1, "Try to unlink and reopen '%s' type %d", name.c_str(), type);
								unlink ( name.c_str() );
								out.open(name, O_WRONLY | O_CREAT | O_TRUNC, mode);

							}
						}
						if (size) {
							string fname = getword(&end);
							if (*end == 'a') {
								copyfile(folder + fname, out, size);
							} else {
								std::pair<fPtr, bool> res = data.insert(std::make_pair(folder + fname, TmpFile()));
								File & dat = res.first->second;
								if (res.second) {
									if (folder.find(':') != string::npos) {
										RegFile tmp;
										tmp.open(tmpname, O_WRONLY | O_CREAT | O_TRUNC, 0600);
										struct flock fl;
										fl.l_start = 0;
										fl.l_len = 0;
										fl.l_type = F_WRLCK;
										fl.l_whence = SEEK_SET;
										fcntl(tmp.fd(), F_SETLKW, &fl);
										copyfile(folder + fname, tmp);
										tmp.close();
										GzFile t;
										t.open(tmpname, "rb");
										unlink(tmpname.c_str());
										dat = t;
									} else {
										GzFile t;
										t.open(folder + fname, "rb");
										dat = t;
									}
								}
								char buf[8096];
								long long sz = size, offs = strtoll(end, &end, 10);
								dat.seek(offs);
								while (sz) {
									int got = dat.read(buf, sz > (int)sizeof(buf) ? sizeof(buf) : sz, true);
									out.write(buf, got);
									Debug(DL_DEBUG, "Wrote %d bytes", got);
									sz -= got;
								}
							}
						}
						if (tarfile) {
							int left = size & 0x1FF;
							if (left) {
								char buf[512];
								bzero(buf, sizeof(buf));
								tar.write(buf, 512 - left);
							}
						} else {
							if (futimes(out.fd(), tvp) != 0) {
								throw Error("Failed to set utimes to file '%s'", name.c_str());
							}
							if (fchown(out.fd(), uid, gid) != 0) {
								throw Error("Failed to set owner of file '%s'", name.c_str());
							}
							if (fchmod(out.fd(), mode) != 0) {
								throw Error("Failed to set access mode to file '%s'", name.c_str());
							}
						}
					} else if (type == FTS_SL) {
						struct timeval tvp[2];
						int mode = strtol(end, &end, 8);
						end++;
						int uid = strtol(end, &end, 10);
						end++;
						int gid = strtol(end, &end, 10);
						end++;
						tvp[0].tv_sec = strtoll(end, &end, 16);
						tvp[0].tv_usec = 0;
						end++;
						tvp[1].tv_sec = strtoll(end, &end, 16);
						tvp[1].tv_usec = 0;
						end++;
						string rname = unescape(end);
						if (tarfile) {
							tarhead(tar, name, mode, uid, gid, 0, tvp[1].tv_sec, 2, rname.c_str());
						} else {

							
							Debug( DL_INFO , "link: %s", name.c_str());
							struct stat sb;
							if (lstat( name.c_str(), &sb ) == 0) { //File exists
								Debug( DL_INFO , "file exist. to prevent error need 'unlink'");
								unlink( name.c_str() );
							}
							
							if (symlink(rname.c_str(), name.c_str()) != 0) {
								char buf[MAXPATHLEN*2];
								int size = readlink(name.c_str(), buf, sizeof(buf));
								if (size > 0)
									buf[size] = 0;

								if (size < 0 || rname != buf)
									throw Error("Failed to create symlink '%s'", name.c_str());
							}
							if (lchown(name.c_str(), uid, gid) != 0)
								throw Error("Failed to change owner of symlink '%s'", name.c_str());
#ifdef FreeBSD
							if (lchmod(name.c_str(), mode) != 0)
								throw Error("Failed to change access mode of symlink '%s'", name.c_str());
#endif
						}
					} else if (type == FTS_D) {
						int mode = strtol(end, &end, 8);
						end++;
						int uid = strtol(end, &end, 10);
						end++;
						int gid = strtol(end, &end, 10);
#ifdef FreeBSD
						end++;
						int flags = strtol(end, &end, 16);
#endif
						if (tarfile) {
							tarhead(tar, name, mode, uid, gid, 0, time(NULL), 5);
						} else {
#ifdef FreeBSD
							if (skipcommon && (flags&UF_OPAQUE) == UF_OPAQUE) {
								Debug(DL_TRACE, "Remove folder '%s'. User have own private folder", name.c_str());
								removefile(name);
							}
#endif
							if (mkdir(name.c_str(), mode) != 0) {
								if (lstat(name.c_str(), &sb) != 0 || !S_ISDIR(sb.st_mode))
									throw Error("Failed to create folder '%s'", name.c_str());
							}
							if (chmod(name.c_str(), mode) != 0)
								throw Error("Failed to change mod of '%s'", name.c_str());

							if (chown(name.c_str(), uid, gid) != 0)
								throw Error("Failed to change owner of '%s'", name.c_str());
#ifdef FreeBSD
							if (chflags(name.c_str(), flags))
								Debug(DL_WARNING, "Failed to set flags %x for '%s'", flags, name.c_str());
#endif
						}
					} else if (type == FTS_NS) {
						if (tarfile) {
							tarhead(tar, name, 0, 0, 0, 0, 0, 1, end);
						} else {
							if (stat(end, &sb) == -1) {
								Debug(DL_DEBUG, "Hard link '%s' to unexistent file '%s' - skip", name.c_str(), end);
								continue;
							}
							if (link(end, name.c_str()) != 0)
								throw Error("Failed to create hardlink '%s'", name.c_str());
						}
					} else if (type == FTS_DEFAULT) {
						int mode = strtol(end, &end, 8);
						end++;
						int uid = strtol(end, &end, 10);
						end++;
						int gid = strtol(end, &end, 10);
						end++;
						int dev_major = strtol(end, &end, 10);
						end ++;
						int dev_minor = strtol(end, &end, 10);
						end ++;

						if (tarfile) {
							if (S_ISFIFO(mode)) {
								tarhead(tar, name, mode&07777, uid, gid, 0, time(NULL), 6);
							} else {
								Debug(DL_DEBUG, "Unsipported '%s' mode = 0%o", name.c_str(), mode);
							}
						} else {
							if (S_ISFIFO(mode)) {
								if (mkfifo(name.c_str(), mode&07777) != 0) {
									struct stat sb;
									if (lstat(name.c_str(), &sb) != 0 || !S_ISFIFO(sb.st_mode) ||
									        chmod(name.c_str(), mode&0&7777))
										throw Error("Failed to create fifo '%s'", name.c_str());
								}
								if (chown(name.c_str(), uid, gid) != 0)
									throw Error("Failed to change owner of fifo '%s'", name.c_str());
							} else if (S_ISCHR(mode) || S_ISBLK(mode) ) {

								Debug(DL_DEBUG, "Character device '%s' mode = 0%o, dev=%lu", name.c_str(), mode, makedev(dev_major, dev_minor));
								if (mknod(name.c_str(), mode, makedev(dev_major, dev_minor) != 0)) {
									Debug (DL_DEBUG, "Cannot creat. try unlink first");
									unlink (name.c_str());
									if (mknod(name.c_str(), mode, makedev(dev_major, dev_minor)) != 0) {
										throw Error("Failed to create character or block device '%s', device: %lu", name.c_str(), makedev(dev_major, dev_minor));
									} else {
										Debug (DL_DEBUG, "file %s created successfully", name.c_str());
									}
								}

							} else {
								Debug(DL_DEBUG, "Unsipported '%s' mode = 0%o", name.c_str(), mode);
							}
						}
#ifdef FreeBSD
					} else if (type == FTS_W) {
						Debug(DL_TRACE, "Whiteout '%s'", name.c_str());
#endif
					}
				} catch (TarError & e) {
					Debug(DL_WARNING, "Tar error: %s. File was skiped", e.Message.c_str());

				} catch (Error & e) {
					if (tarfile) {
						Debug(DL_ERROR, "Error occured. Tar file currupted");
						throw e;
					}
					if (!forse)
						throw e;
					Debug(DL_WARNING, "Error '%s'", e.Message.c_str());
				}
			}
			fclose(in);
			if (tarfile) {
				char buf[512];
				bzero(buf, sizeof(buf));
				tar.write(buf, sizeof(buf));
				tar.write(buf, sizeof(buf));
			}
		}

		if (removecount) {
			Debug(DL_INFO, "Remove old archives from folder '%s'. Leave latest %d", removearchf, removecount);
			Dir dir(removearchf);
			StringMMap files;
			while (dir.read()) {
				Debug(DL_TRACE, "File found '%s'", dir.ent->d_name);
				files.insert(std::make_pair(dir.sb.st_mtime, dir.ent->d_name));
			}

			rsmmPtr ptr = files.rbegin();
			Debug(DL_INFO, "Total files %d", files.size());
			for (int i = removecount; i > 0 && ptr != files.rend(); i--, ptr++);
			for (; ptr != files.rend(); ptr++) {
				Debug(DL_INFO, "Remove list with data '%s'", ptr->second.c_str());
				removefile((string(removearchf) + "/" + ptr->second).c_str());
			}
		} else if (removearch)
			removefile(removearch);

	} catch (Usage & e) {
		Debug(DL_WARNING, "Fatl error: %s", e.Message.c_str());
		printf("%s\n", e.Message.c_str());
		printf(USAGE, PROG);
		return 1;
	} catch (Error & e) {
		Debug(DL_ERROR, "Fatl error: %s", e.Message.c_str());
		return 1;
	}
	if (line)
		free(line);
	return 0;
}

