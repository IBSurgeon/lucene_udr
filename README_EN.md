# UDR full-text search based on Lucene++

There is no built-in full-text search subsystem in Firebird. The Lucene UDR library implements 
full-text search procedures and functions using the freely distributed Lucene library. The original 
Lucene search engine is written in Java. Unfortunately the FB Java plugin for writing external
stored procedures and functions are still in Beta stage. Therefore, Lucene UDP uses the Lucene port to 
the C++ language [Lucene++](https://github.com/luceneplusplus/LucenePlusPlus). Lucene++ is slightly 
faster than the original Lucene engine, but has slightly less features.

## Installing Lucene UDR

To install Lucene UDR, you need:

1. Unpack the zip archive with dynamic libraries into the `plugins/udr` directory
2. Execute the script [fts$install.sql](https://github.com/sim1984/lucene_udr/blob/main/sql/fts%24install.sql)  
to register procedures and functions in an indexed database.
For databases of the 1st SQL dialect, use the script [fts$install_1.sql](https://github.com/sim1984/lucene_udr/blob/main/sql/fts%24install_1.sql)

You can download ready-made builds for Windows OS using the links:
* [LuceneUdr_Win_x64.zip](https://github.com/sim1984/lucene_udr/releases/download/1.0/LuceneUdr_Win_x64.zip)
* [LuceneUdr_Win_x86.zip](https://github.com/sim1984/lucene_udr/releases/download/1.0/LuceneUdr_Win_x86.zip)

Under Linux, you can compile the library yourself.

Download the demo database, for which the examples are prepared, using the following links:
* [fts_demo_3.0.zip](https://github.com/sim1984/lucene_udr/releases/download/1.0/fts_demo_3.0.zip) - database for Firebird 3.0;
* [fts_demo_4.0.zip](https://github.com/sim1984/lucene_udr/releases/download/1.0/fts_demo_4.0.zip) - database for Firebird 4.0.

## Building and installing the library under Linux

Lucene UDR is based on [Lucene++](https://github.com/luceneplusplus/LucenePlusPlus). 
In some Linux distributions, you can install `lucene++` and `lucene++-contrib` from their repositories. 
If there are no libraries in the repositories, then you will need to download and build them from the source.

```
$ git clone https://github.com/luceneplusplus/LucenePlusPlus.git
$ cd LucenePlusPlus
$ mkdir build; cd build
$ cmake ..
$ make
$ sudo make install
```

In order for the `lucene++` library to be installed in `/usr/lib` and not in `/usr/local/lib`, run `cmake -DCMAKE_INSTALL_PREFIX=/usr..` instead of `cmake ..`

The building of the lucene++ library is described in more detail in [BUILDING.md](https://github.com/luceneplusplus/LucenePlusPlus/blob/master/doc/BUILDING.md).

Now you can start building UDR Lucene.

```
$ git clone https://github.com/sim1984/lucene_udr.git
$ cd lucene_udr
$ mkdir build; cd build
$ cmake ..
$ make
$ sudo make install
```

In the process of executing `cmake ..` the following error may occur

```
CMake Error at /usr/lib64/cmake/liblucene++/liblucene++Config.cmake:41 (message):
  File or directory /usr/lib64/usr/include/lucene++/ referenced by variable
  liblucene++_INCLUDE_DIRS does not exist !
Call Stack (most recent call first):
  /usr/lib64/cmake/liblucene++/liblucene++Config.cmake:47 (set_and_check)
  CMakeLists.txt:78 (find_package)
```

To fix it, you need to fix the files `liblucene++Config.cmake` and `liblucene++-contrib Config.cmake`, where
to replace the line

```
get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../usr" ABSOLUTE)
```

with 

```
get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)
```

## Configuring Lucene UDR

Before using full-text search in your database, you need to make a preliminary configuration.
The Lucene UDR settings are in the file `$(root)\ftp.ini`. If this file does not exist, then create it yourself.

This file specifies the path to the directory where full-text indexes for the specified database will be created.

The full path to the database or alias must be set as the section name of the ini file 
(depending on the value of the `DatabaseAccess` parameter in `firebird.conf`). 
The path to the full-text index directory is specified in the `ftsDirectory` key.

```ini
[fts_demo]
ftsDirectory=f:\fbdata\3.0\fts\fts_demo

[f:\fbdata\3.0\fts_demo.fdb]
ftsDirectory=f:\fbdata\3.0\fts\fts_demo
```

Important: The user or group under which the Firebird service is running must have read and write permissions for the directory with full-text indexes.

You can get the directory location for full-text indexes using a query:

```sql
SELECT FTS$MANAGEMENT.FTS$GET_DIRECTORY() AS DIR_NAME
FROM RDB$DATABASE
```

## Creating full-text indexes

To create a full-text index, you need to perform three steps sequentially:
1. Creating a full-text index for a table using the procedure `FTS$MANAGEMENT.FTS$CREATE_INDEX`;
2. Adding indexed fields using the procedure `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD`;
3. Building an index using the procedure `FTS$MANAGEMENT.FTS$REBUILD_INDEX'.

### Creating a full-text index for a table

To create a full-text index for a table, call the procedure `FTS$MANAGEMENT.FTS$CREATE_INDEX'.

The first parameter specifies the name of the full-text index, the second - the name of the indexed table. The remaining parameters are optional.

The third parameter specifies the name of the analyzer. The analyzer specifies for which language the indexed fields will be analyzed. 
If the parameter is omitted, the STANDARD analyzer (for English) will be used. The list of available analyzers can be found 
using the procedure `FTS$MANAGEMENT.FTS$ANALYZERS'.

List of available analyzers:

* STANDARD - StandardAnalyzer (English);
* ARABIC - Arabic Analyzer;
* BRAZILIAN - BrazilianAnalyzer;
* CHINESE - ChineseAnalyzer;
* CJK - CJKAnalyzer (Chinese Letter);
* CZECH - CzechAnalyzer;
* DUTCH - DutchAnalyzer;
* ENGLISH - StandardAnalyzer (English);
* FRENCH - FrenchAnalyzer;
* GERMAN - GermanAnalyzer;
* GREEK - GreekAnalyzer;
* PERSIAN - PersianAnalyzer;
* RUSSIAN - RussianAnalyzer.

The fourth parameter specifies the name of the table field that will be returned as a search result. 
This is usually a primary or unique key field. Setting a special pseudo field `RDB$DB_KEY` is also supported. 
The value of only one field of one of the types can be returned:

* `SMALLINT`, `INTEGER`, `BIGINT` - fields of these types are often used as artificial primary key based on generators (sequences);

* `CHAR(16) CHARACTER SET OCTETS` or `BINARY(16)` - fields of these types are used as an artificial primary key based on GUID, that is, generated using `GEN_UUID()`;

* the `RDB$DB_KEY` field of type `CHAR(8) CHARACTER SET OCTETS`.

If this parameter is not set (NULL value), then an attempt will be made to find the field in the primary key for permanent tables and GTT. 
This attempt will be successful if the key is not composite and the field on which it is built has one of the data types described above. 
If the primary key does not exist, the pseudo field `RDB$DB_KEY` will be used.

The fifth parameter can be set to describe the field.

For examples, a table with the following structure is used:

```sql
CREATE TABLE PRODUCTS (
    PRODUCT_ID             BIGINT GENERATED BY DEFAULT AS IDENTITY,
    PRODUCT_UUID           CHAR(16) CHARACTER SET OCTETS NOT NULL,
    PRODUCT_NAME           VARCHAR(200) NOT NULL,
    UPC_EAN_CODE           VARCHAR(150),
    SELLING_PRICE          VARCHAR(400),
    MODEL_NUMBER           VARCHAR(45),
    ABOUT_PRODUCT          BLOB SUB_TYPE TEXT,
    PRODUCT_SPECIFICATION  BLOB SUB_TYPE TEXT,
    TECHNICAL_DETAILS      BLOB SUB_TYPE TEXT,
    SHIPPING_WEIGHT        VARCHAR(15),
    PRODUCT_DIMENSIONS     VARCHAR(50),
    VARIANTS               BLOB SUB_TYPE TEXT,
    PRODUCT_URL            VARCHAR(255) NOT NULL,
    IS_AMAZON_SELLER       BOOLEAN,
    CONSTRAINT PK_PRODUCT PRIMARY KEY (PRODUCT_ID),
    CONSTRAINT UNQ_PRODUCT_UUID UNIQUE (PRODUCT_UUID)
);
```

