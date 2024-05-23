# ---------------------------------------------------------------------------
# IBSurgeon Full Text Search UDR Library.
#
# The original code was created by Simonov Denis
# for open source project "IBSurgeon Full Text Search UDR".
#
# Copyright (c) 2022 Simonov Denis <sim-mail@list.ru>
# and all contributors signed below.
#
# All Rights Reserved.
# Contributor(s): ______________________________________.
# ----------------------------------------------------------------------------

cmake_minimum_required (VERSION 3.6)

project (luceneudr VERSION 1.3.1)

set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/build)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

###############################################################################
# CMake standard options
############

option(BUILD_SHARED_LIBS "Build shared library" ON)


###############################################################################
# Require and enable C++ 0x/11/14/17
############
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
message(STATUS "luceneudr compile: C++${CMAKE_CXX_STANDARD}")

add_compile_definitions(HAVE_CONFIG_H)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wnarrowing -Werror")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -s")
    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag("-stdlib=libc++" CXX_SUPPORTS_STDLIB)
    if(CXX_SUPPORTS_STDLIB)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -stdlib=libc++")
        set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -stdlib=libc++")
    endif()	
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Intel")
    string(REGEX REPLACE "[/-]W[0-4]" "/W4" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Qstd=c++17")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /QaxCORE-AVX2")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /fp:precise")
    set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG}   /Od")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O3")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /Qipo")
elseif(MSVC)
    string(REGEX REPLACE "/W[0-4]" "/W4" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    if(MSVC_VERSION LESS 1900)
        message(FATAL_ERROR "luceneudr build: Build requires C++17-compliant compiler")
    endif()
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
    message(STATUS "luceneudr build: compiler flags - ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG}")
elseif(CMAKE_BUILD_TYPE MATCHES Release)
    message(STATUS "luceneudr build: compiler flags - ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE}")
endif()

####################################
# src
####################################
file(GLOB_RECURSE luceneudr_sources "src/*")

set(FIREBIRD_DIR /opt/firebird)
set(FIREBIRD_LIB_DIR ${FIREBIRD_DIR}/lib)
set(FIREBIRD_INCLUDE_DIR ${FIREBIRD_DIR}/include)
set(FIREBIRD_UDR_DIR ${FIREBIRD_DIR}/plugins/udr)

####################################
# library target
####################################
add_library(luceneudr SHARED ${luceneudr_sources})

find_package(liblucene++ REQUIRED)
find_package(liblucene++-contrib REQUIRED)

include_directories(${liblucene++_INCLUDE_DIRS})
include_directories(${liblucene++-contrib_INCLUDE_DIRS})

link_directories(${liblucene++_LIBRARY_DIRS})
link_directories(${liblucene++-contrib_LIBRARY_DIRS})
link_directories(${FIREBIRD_LIB_DIR})

####################################
# include directories
####################################
target_include_directories(luceneudr
  PRIVATE
     "include"
)

target_link_libraries(luceneudr fbclient)
target_link_libraries(luceneudr -lstdc++fs)

target_link_libraries(luceneudr ${liblucene++_LIBRARIES})
target_link_libraries(luceneudr ${liblucene++-contrib_LIBRARIES})

install(TARGETS luceneudr DESTINATION ${FIREBIRD_UDR_DIR})
install(FILES "sql/fts$install.sql"
              "sql/fts$install_1.sql"
              "sql/fts$uninstall.sql"
              DESTINATION ${FIREBIRD_UDR_DIR})

