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

cmake_minimum_required (VERSION 3.13)

project (luceneudr 
    VERSION 1.4.0
    LANGUAGES CXX
)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(FIREBIRD_DIR /opt/firebird CACHE PATH "Firebird installation directory")
set(LUCENE_DIR "" CACHE PATH "Lucene++ installation prefix (if empty, find_package() is used)")
if(MSVC)
    set(BOOST_LIBRARYDIR "" CACHE PATH "Directory with Boost libraries for MSVC auto-linking")
endif()

set(FIREBIRD_INCLUDE_DIR ${FIREBIRD_DIR}/include)
set(FIREBIRD_UDR_DIR ${FIREBIRD_DIR}/plugins/udr)

####################################
# library target
####################################
add_library(luceneudr SHARED ${luceneudr_sources})
target_sources(luceneudr PRIVATE
    "src/Analyzers.cpp"
    "src/EnglishAnalyzer.cpp"
    "src/FBFieldInfo.cpp"
    "src/FBUtils.cpp"
    "src/FTS.cpp"
    "src/FTS_HIGHLIGHTER.cpp"
    "src/FTS_MANAGEMENT.cpp"
    "src/FTS_STATISTICS.cpp"
    "src/FTS_TRIGGER_HELPER.cpp"
    "src/FTSHelper.cpp"
    "src/FTSIndex.cpp"
    "src/FTSTrigger.cpp"
    "src/FTSUtils.cpp"
    "src/LuceneAnalyzerFactory.cpp"
    "src/LuceneFiles.cpp"
    "src/LuceneUdr.cpp"
    "src/Relations.cpp"
)

# require C++17 standard
target_compile_features(luceneudr PRIVATE cxx_std_17)
target_compile_definitions(luceneudr PRIVATE HAVE_CONFIG_H)
if(MSVC)
    target_compile_definitions(luceneudr PRIVATE WIN32_LEAN_AND_MEAN)
    target_compile_options(luceneudr PRIVATE /MP)
else()
    target_compile_options(luceneudr PRIVATE
    #    -Werror

        -Wall
        -Wextra
        -Wpedantic

        -Wno-unused-parameter
    )
endif()

if(LUCENE_DIR)
    set(liblucene++_INCLUDE_DIRS ${LUCENE_DIR}/include/lucene++)
    set(liblucene++_LIBRARY_DIRS ${LUCENE_DIR}/lib)
    set(liblucene++_LIBRARIES lucene++)
    set(liblucene++-contrib_INCLUDE_DIRS ${LUCENE_DIR}/include/lucene++)
    set(liblucene++-contrib_LIBRARY_DIRS ${LUCENE_DIR}/lib)
    set(liblucene++-contrib_LIBRARIES lucene++-contrib)
else()
    find_package(liblucene++ QUIET)
    find_package(liblucene++-contrib QUIET)
    if(NOT liblucene++_FOUND OR NOT liblucene++-contrib_FOUND)
        message(FATAL_ERROR "Lucene++ not found. Set LUCENE_DIR, e.g. -DLUCENE_DIR=\"C:/Program Files/lucene++\"")
    endif()
endif()

if(MSVC)
    # Lucene++ headers depend on Boost
    find_package(Boost REQUIRED)
endif()

####################################
# include directories
####################################
target_include_directories(luceneudr PRIVATE
    ${liblucene++_INCLUDE_DIRS}
    ${liblucene++-contrib_INCLUDE_DIRS}
    ${FIREBIRD_INCLUDE_DIR}
    "include"
)

####################################
# link directories
####################################
target_link_directories(luceneudr PRIVATE
    ${liblucene++_LIBRARY_DIRS}
    ${liblucene++-contrib_LIBRARY_DIRS}
)

target_link_libraries(luceneudr
    ${liblucene++_LIBRARIES}
    ${liblucene++-contrib_LIBRARIES}
)

if(MSVC)
    target_link_libraries(luceneudr Boost::boost)
    if(BOOST_LIBRARYDIR)
        target_link_directories(luceneudr PRIVATE ${BOOST_LIBRARYDIR})
    endif()
else()
    target_link_libraries(luceneudr -lstdc++fs)
endif()

install(TARGETS luceneudr DESTINATION ${FIREBIRD_UDR_DIR})
install(FILES "sql/fts$install.sql"
              "sql/fts$install_1.sql"
              "sql/fts$uninstall.sql"
              DESTINATION ${FIREBIRD_UDR_DIR}
)

