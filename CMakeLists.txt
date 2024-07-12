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
    VERSION 1.3.3
    LANGUAGES CXX
)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(FIREBIRD_DIR /opt/firebird)
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
target_compile_options(luceneudr PRIVATE
#    -Werror

    -Wall
    -Wextra
    -Wpedantic

    -Wno-unused-parameter
)

find_package(liblucene++ REQUIRED)
find_package(liblucene++-contrib REQUIRED)

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
    -lstdc++fs
    ${liblucene++_LIBRARIES}
    ${liblucene++-contrib_LIBRARIES}
)

install(TARGETS luceneudr DESTINATION ${FIREBIRD_UDR_DIR})
install(FILES "sql/fts$install.sql"
              "sql/fts$install_1.sql"
              "sql/fts$uninstall.sql"
              DESTINATION ${FIREBIRD_UDR_DIR}
)

