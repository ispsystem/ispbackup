project(ispbackup CXX)

set(${PROJECT_NAME}_SRC ispbackup.cpp)
add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SRC})
target_link_libraries(${PROJECT_NAME} z curl)
