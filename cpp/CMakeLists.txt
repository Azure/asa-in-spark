cmake_minimum_required (VERSION 3.8)
project (cpp)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 64bit compile
SET(CMAKE_CXX_FLAGS "-m64")
SET(CMAKE_C_FLAGS "-m64")
SET(CMAKE_EXE_LINKER_FLAGS "-m64")
SET(CMAKE_MODULE_LINKER_FLAGS "-m64")
SET(CMAKE_SHARED_LINKER_FLAGS "-m64")

set(CMAKE_POSITION_INDEPENDENT_CODE ON) # fPIC

find_package(Java REQUIRED)
find_package(JNI REQUIRED)
include(UseJava)

message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")

create_javah(TARGET javaheaders
    CLASSES com.microsoft.ASANative
    CLASSPATH ../../java/target/classes)

add_library(asa SHARED ASANative.cpp utils.hpp)
target_link_libraries(asa stdc++fs)
target_include_directories(asa PUBLIC ${JNI_INCLUDE_DIRS})
