#ifndef _ISP_CLASSES_H_
#define _ISP_CLASSES_H_

#include <string>
#include <vector>
#include <list>
#include <map>
#include <set>

#define UPDATE_CONTROL "(c) ISPsystem.com"

#ifdef Windows
#include <ispmgr\windows\windefs.h>
#include <time.h>
#define DSEP "\\"
#else
#include <unistd.h>
#define DSEP "/"
#endif

#define	ForEachI(LIST, VAR)	for(typeof((LIST).begin()) VAR = (LIST).begin(); VAR!=(LIST).end(); ++VAR)
#define	ForEachR(LIST, VAR)	for(typeof((LIST).rbegin()) VAR = (LIST).rbegin(); VAR!=(LIST).rend(); ++VAR)
#define	ElemExists(LIST, VAR)	(LIST.find(VAR)!=LIST.end())
#define FOREACH(a, b)		 for (a = b.begin(); a != b.end(); a++)

#ifndef Windows
#define	Swap(a, b)			{typeof(a) tmp = a; a = b; b =  tmp;}
#endif

#define	EXCEPTION	:public InternalError
#define	EXCEPT(m)	: InternalError(m)
using std::string;

typedef std::vector<string> StringVector;
typedef std::vector<string>::iterator vsPtr;
typedef std::vector<string>::const_iterator cvsPtr;

typedef std::list<string> StringList;
typedef std::list<string>::iterator lsPtr;
typedef std::list<string>::const_iterator clsPtr;
typedef std::list<string>::reverse_iterator rlsPtr;
typedef std::list<string>::const_reverse_iterator crlsPtr;

typedef std::list<int> IntList;
typedef std::list<int>::iterator liPtr;
typedef std::list<int>::const_iterator cliPtr;

typedef std::map<string, string> StringMap;
typedef std::map<string, string>::iterator msPtr;
typedef std::map<string, string>::reverse_iterator rmsPtr;
typedef StringMap::const_iterator cmsPtr;

typedef std::multimap<string, string> StringMultiMap;
typedef std::multimap<string, string>::iterator mmsPtr;
typedef std::multimap<string, string>::const_iterator cmmsPtr;

typedef std::map<string, int> IntMap;
typedef std::map<string, int>::iterator miPtr;
typedef std::map<string, int>::const_iterator cmiPtr;

typedef std::map<string, long long> LongIntMap;

typedef std::map<string, double> DoubleMap;
typedef std::map<string, double>::iterator mdPtr;

typedef std::map<int, string> IntCache;
typedef std::map<int, string>::iterator icPtr;

typedef std::map<int, int> IntInt;
typedef std::map<int, int>::iterator iiPtr;
typedef std::map<int, int>::const_iterator ciiPtr;

typedef std::set<string> StringSet;
typedef StringSet::iterator ssPtr;
typedef StringSet::const_iterator cssPtr;

typedef std::set<int> IntSet;
typedef IntSet::iterator isPtr;
typedef IntSet::const_iterator cisPtr;

typedef std::pair<string, string> StringPair;

struct StrWithTime {
	time_t Time;
	string Str;
	StrWithTime() {
	}
	StrWithTime(const string& _str, const time_t _time) {
		Str = _str;
		Time = _time;
	}
	StrWithTime(const string& _str) {
		Str = _str;
		time(&Time);
	}
};

typedef std::map<string, StrWithTime> StrTimeMap;
typedef std::map<string, StrWithTime>::const_iterator cswtPtr;
typedef std::map<string, StrWithTime>::iterator swtPtr;

class Object {};

class ObjectEvent {
public:
	typedef void (Object::*function_of_object)();

	struct ObjEvent {
		Object *obj;
		function_of_object func;
	};

	std::list<ObjEvent> EventList;
	typedef std::list<ObjEvent>::const_iterator EventListPtr;

	class Dummy {
	public:
		typedef void (*Function)();
		Dummy(Function f): func(f) {}
		void Func() {
			func();
		}
	private:
		Function func;
	};

public:
	void Add(Dummy::Function f) {
		ObjEvent e = {(Object *)new Dummy(f), (function_of_object)&Dummy::Func};
		EventList.push_back(e);
	}

	template <class Func>
	void Add(Func *o, void (Func::*f)()) {
		ObjEvent e = {(Object *)o, (function_of_object)f};
		EventList.push_back(e);
	}
	void Execute() const {
		for (EventListPtr e = EventList.begin(); e != EventList.end(); e++)
			((e->obj)->*(e->func))();
	}
	inline bool empty() const { return EventList.empty(); }
};

template <class Param>
class eOneParamEvent {
private:
	typedef void (Object::*function_of_object)(Param& param1);
	typedef std::pair<Object *, function_of_object> Event;
	typedef std::list<Event> EventListType;
	typedef typename EventListType::const_iterator EventListPtr;
	typedef void (*Function)(Param& param1);

	class Dummy {
	public:
		Dummy(Function f): func(f) {}
		void Func(Param &p) {
			func(p);
		}
	private:
		Function func;
	};

	EventListType EventList;

public:
	void Add(Function f) {
		EventList.push_back(std::make_pair((Object *)new Dummy(f), (function_of_object)&Dummy::Func));
	}

	template <class Func>
	void Add(Func *o, void (Func::*f)(Param & param1)) {
		EventList.push_back(std::make_pair((Object *)o, (function_of_object)f));
	}
	void Execute(Param & param1) const {
		for (EventListPtr e = EventList.begin(); e != EventList.end(); e++)
			((e->first)->*(e->second))(param1);
	}
	inline bool empty() const { return EventList.empty(); }
};

template <class Param1, class Param2>
class eTwoParamEvent {
private:
	typedef void (Object::*function_of_object)(Param1 & param1, Param2 & param2);
	typedef std::pair<Object *, function_of_object> Event;
	typedef std::list<Event> EventListType;
	typedef typename EventListType::const_iterator EventListPtr;
	typedef void (*Function)(Param1 & param1, Param2 & param2);

	class Dummy {
	public:
		Dummy(Function f): func(f) {}
		void Func(Param1 &p1, Param2 &p2) {
			func(p1, p2);
		}
	private:
		Function func;
	};

	EventListType EventList;

public:
	void Add(Function f) {
		EventList.push_back(std::make_pair((Object *)new Dummy(f), (function_of_object)&Dummy::Func));
	}

	template <class Func>
	void Add(Func *o, void (Func::*f)(Param1 & param1, Param2 & param2)) {
		EventList.push_back(std::make_pair((Object *)o, (function_of_object)f));
	}

	void Execute(Param1 & param1, Param2 & param2) const {
		for (EventListPtr e = EventList.begin(); e != EventList.end(); e++)
			((e->first)->*(e->second))(param1, param2);
	}

	inline bool empty() const { return EventList.empty(); }
};


#if defined(Linux) || defined(SunOS) || defined(Darwin)
#define LOFBSD(A,B)	A
#endif
#ifdef FreeBSD
#define LOFBSD(A,B)	B
#endif

extern "C" {
	bool ModInit();
}

#endif
