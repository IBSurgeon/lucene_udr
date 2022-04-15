cmake_minimum_required (VERSION 3.0)

project (luceneudr VERSION 0.9.5)

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
	if (NOT (CMAKE_VERSION VERSION_LESS 3.6.0)) # Compiler features for Intel in CMake 3.6+
		set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Qstd=c++17")
	endif()
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

####################################
# library target
####################################
add_library(luceneudr SHARED ${luceneudr_sources})

find_package(ICU 50.2 COMPONENTS uc i18n REQUIRED)
find_package(Boost 1.53.0 COMPONENTS date_time filesystem iostreams regex system thread REQUIRED)
find_package(ZLIB REQUIRED)

include_directories(${Boost_INCLUDE_DIR})
link_directories(${Boost_LIBRARY_DIR})
include_directories(${ICU_INCLUDE_DIRS})
link_directories(${ICU_LIBRARY_DIRS})
include_directories(${ZLIB_INCLUDE_DIRS})
link_directories(${ZLIB_LIBRARY_DIRS})

####################################
# include directories
####################################
target_include_directories(luceneudr
  PRIVATE
     "include"
)

target_link_libraries(luceneudr ICU::uc ICU::i18n)
