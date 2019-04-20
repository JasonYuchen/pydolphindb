cmake_minimum_required(VERSION 3.00)

project(pydolphindb)

add_definitions(-DLINUX 
                -DLOGGING_LEVEL_1 
				-D_GLIBCXX_USE_CXX11_ABI=0 
				-DVERSION_INFO="1.0.0")

set(CMAKE_CXX_STANDARD 14)
set(PYBIND11_CPP_STANDARD -std=c++14)
set(PYTHON_EXECUTABLE /usr/bin/python3)
set(CMAKE_INSTALL_RPATH "$ORIGIN")
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE) 

include_directories(api-cplusplus/include
                    ${PROJECT_SOURCE_DIR})

link_directories(api-cplusplus/bin/linux_x64 
                 ${OPENSSL_LIB_PATH})

link_libraries(libDolphinDBAPI.so 
               ssl)

add_subdirectory(pybind11)

pybind11_add_module(pydolphindb 
	src/module.cpp
	src/Utils.h
	src/Utils.cpp
	src/Session.h
	src/Session.cpp)

target_link_libraries(pydolphindb
	PUBLIC
	DolphinDBAPI
	ssl
	uuid
	crypto
	pthread)
