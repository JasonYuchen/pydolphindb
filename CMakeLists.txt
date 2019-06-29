cmake_minimum_required(VERSION 3.00)

project(pydolphindbimpl)

if(WIN32)
    add_definitions(-DWINDOWS)
elseif(UNIX)
    add_definitions(-DLINUX -D_GLIBCXX_USE_CXX11_ABI=0)
endif()

add_definitions(-DLOGGING_LEVEL_1)

set(CMAKE_CXX_STANDARD 11)
set(PYBIND11_CPP_STANDARD -std=c++11)
set(CMAKE_INSTALL_RPATH "$ORIGIN")
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

include_directories(${PROJECT_SOURCE_DIR}/api-cplusplus/include
    ${PROJECT_SOURCE_DIR}/src)

if(WIN32)
    link_directories(${PROJECT_SOURCE_DIR}/api-cplusplus/bin/mingw_x64 
        ${OPENSSL_LIB_PATH})
elseif(UNIX)
    link_directories(${PROJECT_SOURCE_DIR}/api-cplusplus/bin/linux_x64 
        ${OPENSSL_LIB_PATH})
endif()


link_libraries(DolphinDBAPI
    ssl)

add_subdirectory(pybind11)

aux_source_directory(${PROJECT_SOURCE_DIR}/src PYDOLPHINDB_SOURCE)

pybind11_add_module(pydolphindbimpl 
    ${PYDOLPHINDB_SOURCE})

target_link_libraries(pydolphindbimpl
    PUBLIC
    DolphinDBAPI
    ssl
    uuid
    crypto
    pthread)

install(TARGETS pydolphindbimpl
    DESTINATION ${PROJECT_SOURCE_DIR}/pydolphindb)

if(WIN32)
    install(FILES ${PROJECT_SOURCE_DIR}/api-cplusplus/bin/mingw_x64/libDolphinDBAPI.dll
        DESTINATION ${PROJECT_SOURCE_DIR}/pydolphindb)
elseif(UNIX)
    install(FILES ${PROJECT_SOURCE_DIR}/api-cplusplus/bin/linux_x64/libDolphinDBAPI.so
        DESTINATION ${PROJECT_SOURCE_DIR}/pydolphindb)
endif()
