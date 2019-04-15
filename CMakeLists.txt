cmake_minimum_required(VERSION 3.00)

project(PyDolphinDB)

add_definitions(-DLINUX -DLOGGING_LEVEL_1 -D_GLIBCXX_USE_CXX11_ABI=0)

set(PYTHON_EXECUTABLE /usr/bin/python3)
set(CMAKE_INSTALL_RPATH "$ORIGIN")
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE) 

include_directories(api-cplusplus/include)
link_directories(api-cplusplus/bin/linux_x64)
link_libraries(libDolphinDBAPI.so)

add_subdirectory(pybind11)

pybind11_add_module(PyDolphinDB 
	src/module.cpp
	src/Utils.hpp
	src/Utils.cpp
	src/Session.hpp
	src/Session.cpp)

target_link_libraries(PyDolphinDB
	PUBLIC
	DolphinDBAPI
	ssl
	uuid
	crypto
	pthread)
