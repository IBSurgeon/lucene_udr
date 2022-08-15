# IBSurgeon Full Text Search UDR

IBSurgeon FTS UDR library implements full text search procedures and functions for Firebird SQL, to run them in SQL queries,
using the powerful capabilities of Lucene search engine. 
In this UDR we use [Lucene++](https://github.com/luceneplusplus/LucenePlusPlus), it is the C++ implementation of Lucene search engine, 
to achieve the fastest search with true full text search capabilities.
This UDR is 100% free and open source, with LGPL license. 

Windows and Linux versions are available: for Windows we have ready-to-use binaries and for Linux it is necessary to build UDR to work 
on the specific distribution (we have easy building instruction).

The library is developed by the grant from IBSurgeon [www.ib-aid.com](https://www.ib-aid.com).

## Installing Lucene UDR

To install Lucene UDR, you need:

1. Unpack the zip archive with dynamic libraries into the `plugins/udr` directory
2. Execute the script [fts$install.sql](https://github.com/IBSurgeon/lucene_udr/blob/main/sql/fts%24install.sql)  
to register procedures and functions in an indexed database.
For databases of the 1st SQL dialect, use the script [fts$install_1.sql](https://github.com/IBSurgeon/lucene_udr/blob/main/sql/fts%24install_1.sql)

You can download ready-made builds for Windows OS using the links:
* [LuceneUdr_Win_x64.zip](https://github.com/IBSurgeon/lucene_udr/releases/download/1.2/LuceneUdr_Win_x64.zip)
* [LuceneUdr_Win_x86.zip](https://github.com/IBSurgeon/lucene_udr/releases/download/1.2/LuceneUdr_Win_x86.zip)

Under Linux, you can compile the library yourself.

Download the demo database, for which the examples are prepared, using the following links:
* [fts_demo_3.0.zip](https://github.com/IBSurgeon/lucene_udr/releases/download/1.2/fts_demo_3.0.zip) - database for Firebird 3.0;
* [fts_demo_4.0.zip](https://github.com/IBSurgeon/lucene_udr/releases/download/1.2/fts_demo_4.0.zip) - database for Firebird 4.0.

Documentation in English and Russian is available at the links:
* [lucene-udr.pdf](https://github.com/IBSurgeon/lucene_udr/releases/download/1.2/lucene-udr.pdf);
* [lucene-udr-rus.pdf](https://github.com/IBSurgeon/lucene_udr/releases/download/1.2/lucene-udr-rus.pdf).

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
$ git clone https://github.com/IBSurgeon/lucene_udr.git
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
The Lucene UDR settings are in the file `$(root)\fts.ini`. If this file does not exist, then create it yourself.
Where `$(root)` is the root directory of the Firebird installation.

This file specifies the path to the directory where full-text indexes for the specified database will be created.

The full path to the database or alias must be set as the section name of the ini file 
(depending on the value of the `DatabaseAccess` parameter in `firebird.conf`). 
The path to the full-text index directory is specified in the `ftsDirectory` key.

```ini
[fts_demo]
ftsDirectory=f:\fbdata\3.0\fts\fts_demo
```

or

```ini
[f:\fbdata\3.0\fts_demo.fdb]
ftsDirectory=f:\fbdata\3.0\fts\fts_demo
```

On Linux, the section name is case-sensitive. It must exactly match the value of the query result:

```sql
select mon$attachment_name
from mon$attachments
where mon$attachment_id = current_connection;
```

If your connection can occur both through an alias and with the path to the database, you can write both sections to the ini file at once.

```ini
[f:\fbdata\3.0\fts_demo.fdb]
ftsDirectory=f:\fbdata\3.0\fts\fts_demo

[fts_demo]
ftsDirectory=f:\fbdata\3.0\fts\fts_demo
```

Important: The user or group under which the Firebird service is running must have read and write permissions for the directory with full-text indexes.

You can get the directory location for full-text indexes using a query:

```sql
SELECT FTS$MANAGEMENT.FTS$GET_DIRECTORY() AS DIR_NAME
FROM RDB$DATABASE
```

## Analyzers

Analysis is the transformation of known text into smaller, more precise units for easier retrieval.

The text goes through various operations to extract keywords, remove common words and punctuation marks, convert words to lower case, and so on.

The list of available analyzers can be obtained using the `FTS$MANAGEMENT.FTS$ANALYZERS` procedure.

The following analyzers are available by default:

* ARABIC - Arabic Analyzer;
* BRAZILIAN - BrazilianAnalyzer;
* CHINESE - ChineseAnalyzer;
* CJK - CJKAnalyzer (Chinese Letter);
* CZECH - CzechAnalyzer;
* DUTCH - DutchAnalyzer;
* ENGLISH - EnglishAnalyzer;
* FRENCH - FrenchAnalyzer;
* GERMAN - GermanAnalyzer;
* GREEK - GreekAnalyzer;
* KEYWORD - KeywordAnalyzer;
* PERSIAN - PersianAnalyzer;
* RUSSIAN - RussianAnalyzer;
* SIMPLE - SimpleAnalyzer;
* STANDARD - StandardAnalyzer (English);
* STOP - StopAnalyzer;
* WHITESPACE - WhitespaceAnalyzer;
* SNOWBALL(DANISH) - SnowballAnalyzer('danish');
* SNOWBALL(DUTCH) - SnowballAnalyzer('dutch', DutchStopWords);
* SNOWBALL(ENGLISH) - SnowballAnalyzer('english', EnglishStopWords);
* SNOWBALL(FINNISH) - SnowballAnalyzer('finnish');
* SNOWBALL(FRENCH) - SnowballAnalyzer('french', FrenchStopWords);
* SNOWBALL(GERMAN) - SnowballAnalyzer('german', GermanStopWords);
* SNOWBALL(HUNGARIAN) - SnowballAnalyzer('hungarian');
* SNOWBALL(ITALIAN) - SnowballAnalyzer('italian');
* SNOWBALL(NORWEGIAN) - SnowballAnalyzer('norwegian');
* SNOWBALL(PORTER) - SnowballAnalyzer('porter', EnglishStopWords);
* SNOWBALL(PORTUGUESE) - SnowballAnalyzer('portuguese');
* SNOWBALL(ROMANIAN) - SnowballAnalyzer('romanian');
* SNOWBALL(RUSSIAN) - SnowballAnalyzer('russian', RussianStopWords);
* SNOWBALL(SPANISH) - SnowballAnalyzer('spanish');
* SNOWBALL(SWEDISH) - SnowballAnalyzer('swedish');
* SNOWBALL(TURKISH) - SnowballAnalyzer('turkish').

### Analyzer types

Let's consider the most commonly used analyzers.

#### STANDARD - StandardAnalyzer

The standard analyzer splits text into words, numbers, URLs and emails. It converts the text to lowercase, 
after which the stop-word filter for English is applied to the resulting terms.

#### STOP - StopAnalyzer

StopAnalyzer splits text into non-letter characters. It converts the text to lowercase, after which the stop-word filter for English is applied to the resulting terms.

Unlike StandardAnalyzer, StopAnalyzer is not capable of recognizing URLs and e-mails.

#### SIMPLE - SimpleAnalyzer

SimpleAnalyzer separates text into non-letter characters. It converts text to lowercase.
SimpleAnalyzer does not apply a stop-word filter, and it is not capable of recognizing URLs and e-mails.

#### WHITESPACE - WhitespaceAnalyzer

WhitespaceAnalyzer - splits text by whitespace characters.

#### KEYWORD - KeywordAnalyzer

KeywordAnalyzer - Represents text as one single term.
KeywordAnalyzer is useful for fields like id and zip codes.

#### Language analyzers

There are also special analyzers for different languages, such as EnglishAnalyzer, FrenchAnalyzer, RussianAnalyzer and others.

Such analyzers split text into words, numbers, URLs and EMAILs. It converts the text to lower case, after which the stop word filter 
for the specified language is applied to the resulting terms.

After filtering, the stemming algorithm is applied. Stemming is the process of reducing inflected (or sometimes derived) words to their word stem, 
base or root form—generally a written word form. The stem need not be identical to the morphological root of the word; it is usually sufficient 
that related words map to the same stem, even if this stem is not in itself a valid root.  Stemming is necessary so that the search 
can be carried out not only by the word itself, but also by its forms.

#### Snowball analyzers

Analyzers that use stemming algorithms from the "Snowball" project.

### Creating custom analyzers

The IBSurgeon FTS UDR library allows you to create custom analyzers. Through the SQL language it is not possible to change the algorithms 
for splitting text into terms and stemming algorithms, however, you can specify a list of your own stop words.

You can create custom analyzer based on one of the built-in analyzers. In order to be able to create custom analyzer based on the base analyzer, 
the base analyzer must support the stop-word filter. A new analyzer is created with an empty list of stop words.

To create custom analyzer, call the `FTS$MANAGEMENT.FTS$CREATE_ANALYZER` procedure. The first parameter specifies the name of the new analyzer, 
the second - the name of the base analyzer, the third, optional parameter, you can specify the description of the analyzer.

After creating the analyzer, you can add the necessary stop words using the `FTS$MANAGEMENT.FTS$ADD_STOP_WORD` procedure.

An example of creating your own analyzer:

```sql
execute procedure FTS$MANAGEMENT.FTS$CREATE_ANALYZER('FARMNAME_EN', 'ENGLISH');

commit;

execute procedure FTS$MANAGEMENT.FTS$ADD_STOP_WORD('FARMNAME_EN', 'farm');
execute procedure FTS$MANAGEMENT.FTS$ADD_STOP_WORD('FARMNAME_EN', 'owner');

commit;
```

Note:

If you add or remove stop words from the analyzer, on the basis of which there are already built indexes, 
then these indexes change their status to 'U' - Updated metadata, that is, they require rebuilding.

## Creating full-text indexes

To create a full-text index, you need to perform three steps sequentially:
1. Creating a full-text index for a table using the procedure `FTS$MANAGEMENT.FTS$CREATE_INDEX`;
2. Adding indexed fields using the procedure `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD`;
3. Building an index using the procedure `FTS$MANAGEMENT.FTS$REBUILD_INDEX`.

### Creating a full-text index for a table

To create a full-text index for a table, call the procedure `FTS$MANAGEMENT.FTS$CREATE_INDEX`.

The first parameter specifies the name of the full-text index, the second - the name of the indexed table. The remaining parameters are optional.

The third parameter specifies the name of the analyzer. The analyzer specifies for which language the indexed fields will be analyzed. 
If the parameter is omitted, the STANDARD analyzer (for English) will be used. 

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

The example below creates an index `IDX_PRODUCT_NAME` for the `PRODUCTS` table using the `STANDARD` analyzer.
The `PRODUCT_ID` field is returned. Its name was automatically extracted from the primary key of the `PRODUCTS` table.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_NAME', 'PRODUCTS');

COMMIT;
```

The following example will create an index `IDX_PRODUCT_NAME_EN` using the analyzer `ENGLISH`.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_NAME_EN', 'PRODUCTS', 'ENGLISH');

COMMIT;
```

You can specify the name of the field that will be returned as a search result.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_ID_2_EN', 'PRODUCTS', 'ENGLISH', 'PRODUCT_ID');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_UUID_EN', 'PRODUCTS', 'ENGLISH', 'PRODUCT_UUID');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_DBKEY_EN', 'PRODUCTS', 'ENGLISH', 'RDB$DB_KEY');

COMMIT;
```

### Adding fields for indexing

After creating the index, you need to add fields that will be searched using the procedure `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD`. 
The first parameter specifies the index name, the second the name of the field to be added.
The third optional parameter can specify the significance multiplier for the field. 
By default, the significance of all index fields is the same and equal to 1.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_NAME_EN', 'PRODUCT_NAME');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_DBKEY_EN', 'PRODUCT_NAME');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_UUID_EN', 'PRODUCT_NAME');


EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_ID_2_EN', 'PRODUCT_NAME');
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_ID_2_EN', 'ABOUT_PRODUCT');

COMMIT;
```

In the indexes `IDX_PRODUCT_NAME_EN`, `IDX_PRODUCT_DBKEY_EN` and `IDX_PRODUCT_UUID_EN` one field `PRODUCT_NAME` is processed, 
and in the index `IDX_PRODUCT_ID_2_EN` two fields `PRODUCT_NAME` and `ABOUT_PRODUCT` are processed.

The following example shows the creation of an index with two fields `PRODUCT_NAME` and `ABOUT_PRODUCT`. 
The significance of the `PRODUCT_NAME` field is 4 times higher than the significance of the `ABOUT_PRODUCT` field.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_ID_2X_EN', 'PRODUCTS', 'ENGLISH', 'PRODUCT_ID');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_ID_2X_EN', 'PRODUCT_NAME', 4);
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_ID_2X_EN', 'ABOUT_PRODUCT');

COMMIT;
```

### Building an index

To build the index, the procedure `FTS$MANAGEMENT.FTS$REBUILD_INDEX` is used. The name of the full-text index must be specified as an input parameter.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_NAME_EN');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_DBKEY_EN');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_UUID_EN');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_ID_2_EN');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_ID_2X_EN');

COMMIT;
```

At the stage of building for the index, a corresponding folder of the same name is created in the directory for full-text indexes.
These folders contain the Lucene index files. This part of the process happens outside of transaction control, so ROLLBACK will not remove the index files.

In addition, in case of a successful build, the status of the index changes to 'C' (Complete). Status changes occur in the current transaction.

## Index statuses

The description of the indexes is stored in the service table `FTS$INDEXES`.

The `FTS$INDEX_STATUS` field stores the index status. The index can have 4 statuses:

* *N* - New index. It is set when creating an index in which there is not a single segment yet.
* *U* - Updated metadata. It is set every time the index metadata changes, for example, when
an index segment is added or deleted. If the index has such a status, then it needs to be rebuilt so that the search for it
works correctly.
* *I* - Inactive. Inactive index. Inactive indexes are not updated by the `FTS$UPDATE_INDEXES` procedure.
* *C* - Complete. Active index. Such indexes are updated by the procedure `FTS$UPDATE_INDEXES`.
The index enters this state only after a complete build or rebuild.

## Search using full-text indexes

The `FTS$SEARCH` procedure is used to search the full-text index.

The first parameter specifies the name of the index with which the search will be performed, and the second parameter specifies the search phrase.
The third optional parameter sets a limit on the number of records returned, by default 1000.
The fourth parameter allows you to enable the search results explanation mode, FALSE by default.

Search example:

```sql
SELECT
    FTS$RELATION_NAME
  , FTS$KEY_FIELD_NAME
  , FTS$DB_KEY
  , FTS$ID
  , FTS$UUID
  , FTS$SCORE
  , FTS$EXPLANATION
FROM FTS$SEARCH('IDX_PRODUCT_NAME_EN', 'Transformers Bumblebee')
```

Output parameters:

- FTS$RELATION_NAME - the name of the table in which the document was found;
- FTS$KEY_FIELD_NAME - the name of the key field in the table;
- FTS$DB_KEY - the value of the key field in the format `RDB$DB_KEY`;
- FTS$ID - value of a key field of type `BIGINT` or `INTEGER`;
- FTS$UUID - value of a key field of type `BINARY(16)`. This type is used to store the GUID;
- FTS$SCORE - the degree of compliance with the search query;
- FTS$EXPLANATION - explanation of search results.

The query result will be available in one of the fields `FTS$DB_KEY`, `FTS$ID`, `FTS$UUID`, depending on which resulting field was specified when creating the index.

To extract data from the target table, it is enough to simply make a join with it, the condition of which depends on how the index was created.

Here are examples of different join options:

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_ID,
  P.PRODUCT_NAME
FROM FTS$SEARCH('IDX_PRODUCT_NAME_EN', 'Transformers Bumblebee') FTS
JOIN PRODUCTS P ON P.PRODUCT_ID = FTS.FTS$ID;

SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_UUID,
  P.PRODUCT_NAME
FROM FTS$SEARCH('IDX_PRODUCT_UUID_EN', 'Transformers Bumblebee') FTS
JOIN PRODUCTS P ON P.PRODUCT_UUID = FTS.FTS$UUID;

SELECT
  FTS.FTS$SCORE,
  P.RDB$DB_KEY,
  P.PRODUCT_ID,
  P.PRODUCT_NAME
FROM FTS$SEARCH('IDX_PRODUCT_DBKEY_EN', 'Transformers Bumblebee') FTS
JOIN PRODUCTS P ON P.RDB$DB_KEY = FTS.FTS$DB_KEY;
```

To search for two fields at once, we use the index `IDX_PRODUCT_ID_2_EN`, in which the fields `PRODUCT_NAME` and `ABOUT_PRODUCT` were specified during creation.

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_ID,
  P.PRODUCT_NAME,
  P.ABOUT_PRODUCT
FROM FTS$SEARCH('IDX_PRODUCT_ID_2_EN', 'Transformers Bumblebee') FTS
JOIN PRODUCTS P ON P.PRODUCT_ID = FTS.FTS$ID;
```

To explain the search results, set the last parameter to TRUE.

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_ID,
  P.PRODUCT_NAME,
  P.ABOUT_PRODUCT,
  FTS.FTS$EXPLANATION
FROM FTS$SEARCH('IDX_PRODUCT_ID_2_EN', 'Transformers Bumblebee', 5, TRUE) FTS
JOIN PRODUCTS P ON P.PRODUCT_ID = FTS.FTS$ID;
```

The `FTS$EXPLANATION` field will contain an explanation of the result.

```
4.12074 = (MATCH) sum of:
  1.7817 = (MATCH) sum of:
    1.16911 = (MATCH) weight(PRODUCT_NAME:transformers in 3329), product of:
      0.455576 = queryWeight(PRODUCT_NAME:transformers), product of:
        6.84324 = idf(docFreq=28, maxDocs=10002)
        0.0665732 = queryNorm
      2.56622 = (MATCH) fieldWeight(PRODUCT_NAME:transformers in 3329), product of:
        1 = tf(termFreq(PRODUCT_NAME:transformers)=1)
        6.84324 = idf(docFreq=28, maxDocs=10002)
        0.375 = fieldNorm(field=PRODUCT_NAME, doc=3329)
    0.612596 = (MATCH) weight(ABOUT_PRODUCT:transformers in 3329), product of:
      0.480313 = queryWeight(ABOUT_PRODUCT:transformers), product of:
        7.21481 = idf(docFreq=19, maxDocs=10002)
        0.0665732 = queryNorm
      1.27541 = (MATCH) fieldWeight(ABOUT_PRODUCT:transformers in 3329), product of:
        1.41421 = tf(termFreq(ABOUT_PRODUCT:transformers)=2)
        7.21481 = idf(docFreq=19, maxDocs=10002)
        0.125 = fieldNorm(field=ABOUT_PRODUCT, doc=3329)
  2.33904 = (MATCH) sum of:
    1.60308 = (MATCH) weight(PRODUCT_NAME:bumblebee in 3329), product of:
      0.533472 = queryWeight(PRODUCT_NAME:bumblebee), product of:
        8.01332 = idf(docFreq=8, maxDocs=10002)
        0.0665732 = queryNorm
      3.00499 = (MATCH) fieldWeight(PRODUCT_NAME:bumblebee in 3329), product of:
        1 = tf(termFreq(PRODUCT_NAME:bumblebee)=1)
        8.01332 = idf(docFreq=8, maxDocs=10002)
        0.375 = fieldNorm(field=PRODUCT_NAME, doc=3329)
    0.735957 = (MATCH) weight(ABOUT_PRODUCT:bumblebee in 3329), product of:
      0.526458 = queryWeight(ABOUT_PRODUCT:bumblebee), product of:
        7.90796 = idf(docFreq=9, maxDocs=10002)
        0.0665732 = queryNorm
      1.39794 = (MATCH) fieldWeight(ABOUT_PRODUCT:bumblebee in 3329), product of:
        1.41421 = tf(termFreq(ABOUT_PRODUCT:bumblebee)=2)
        7.90796 = idf(docFreq=9, maxDocs=10002)
        0.125 = fieldNorm(field=ABOUT_PRODUCT, doc=3329)
```

For comparison, an explanation of the index search results with fields that have a different significance coefficient is shown.

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_ID,
  P.PRODUCT_NAME,
  P.ABOUT_PRODUCT,
  FTS.FTS$EXPLANATION
FROM FTS$SEARCH('IDX_PRODUCT_ID_2X_EN', 'Transformers Bumblebee', 5, TRUE) FTS
JOIN PRODUCTS P ON P.PRODUCT_ID = FTS.FTS$ID;
```

```
13.7448 = (MATCH) sum of:
  4.67643 = (MATCH) sum of:
    4.67643 = (MATCH) weight(PRODUCT_NAME:transformers in 166), product of:
      0.455576 = queryWeight(PRODUCT_NAME:transformers), product of:
        6.84324 = idf(docFreq=28, maxDocs=10002)
        0.0665732 = queryNorm
      10.2649 = (MATCH) fieldWeight(PRODUCT_NAME:transformers in 166), product of:
        1 = tf(termFreq(PRODUCT_NAME:transformers)=1)
        6.84324 = idf(docFreq=28, maxDocs=10002)
        1.5 = fieldNorm(field=PRODUCT_NAME, doc=166)
  9.06839 = (MATCH) sum of:
    9.06839 = (MATCH) weight(PRODUCT_NAME:bumblebee in 166), product of:
      0.533472 = queryWeight(PRODUCT_NAME:bumblebee), product of:
        8.01332 = idf(docFreq=8, maxDocs=10002)
        0.0665732 = queryNorm
      16.9988 = (MATCH) fieldWeight(PRODUCT_NAME:bumblebee in 166), product of:
        1.41421 = tf(termFreq(PRODUCT_NAME:bumblebee)=2)
        8.01332 = idf(docFreq=8, maxDocs=10002)
        1.5 = fieldNorm(field=PRODUCT_NAME, doc=166)
```

## Syntax of search queries

### Terms

Search queries (search phrases) consist of terms and operators. Lucene supports simple and complex terms.
Simple terms consist of one word, complex terms consist of several. The first of them are ordinary words,
for example, "Hello", "world". The second type of terms is a group of words, for example, "Hello world".
Several terms can be linked together using logical operators.

### Fields

Lucene supports multi-field search. By default, the search is performed in all fields of the full-text index,
the expression for each field is repeated and connected by the `OR` operator. For example, if you have an index containing
the fields `PRODUCT_NAME` and `ABOUT_PRODUCT`, then the query

```
Transformers Bumblebee
```

will be equivalent to the query

```
(PRODUCT_NAME: Transformers Bumblebee) OR (ABOUT_PRODUCT: Transformers Bumblebee)
```

You can specify which field you want to search by, to do this, specify the field name, the colon symbol ":" in the request,
and then the search phrase for this field.

Example of searching for the word "Polyester" in the `ABOUT_PRODUCT` field and the words "Transformers Bumblebee" in the `PRODUCT_NAME` field:

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_ID,
  P.PRODUCT_NAME,
  P.ABOUT_PRODUCT,
  FTS.FTS$EXPLANATION
FROM FTS$SEARCH('IDX_PRODUCT_ID_2_EN', '(PRODUCT_NAME: Transformers Bumblebee) AND (ABOUT_PRODUCT: Polyester)', 5, TRUE) FTS
JOIN PRODUCTS P ON P.PRODUCT_ID = FTS.FTS$ID;
```

Note: Lucene, like Firebird, supports delimited fields. It is strongly discouraged to use spaces and other special characters in field names, 
as this will make it much more difficult to write search queries. If your field contains a space or other special character, 
it must be escaped using the "\\" character.

For example, if you have an index for two fields "Product Name" and "Product Specification" and you want to find the word "Weight" in the specification, 
then the query should look like this:

```
Product\ Specification: Weight
```

### Wildcard Searches

Lucene supports single and multiple character wildcard searches within single terms (not within phrase queries).

To perform a single character wildcard search use the "?" symbol.

To perform a multiple character wildcard search use the "\*" symbol.

The single character wildcard search looks for terms that match that with the single character replaced. 
For example, to search for "text" or "test" you can use the search:

```
te?t
```

Multiple character wildcard searches looks for 0 or more characters. For example, to search for "test", "tests" or "tester", you can use the search:

```
test*
```

You can also use the wildcard searches in the middle of a term.

```
te*t
```

Note: You cannot start a search query with the characters "?" or "\*".

### Fuzzy Searches

Lucene supports fuzzy searches based on the Levenshtein Distance, or Edit Distance algorithm. 
To do a fuzzy search use the tilde, "~", symbol at the end of a Single word Term. For example to search for a term 
similar in spelling to "roam" use the fuzzy search:

```
roam~
```

This search will find terms like "foam" and "roams".

An additional (optional) parameter can specify the required similarity.
The value is between 0 and 1, with a value closer to 1 only terms with a higher similarity will be matched. 
For example:

```
roam~0.8
```

The default that is used if the parameter is not given is 0.5.

### Proximity Searches

Lucene supports finding words are a within a specific distance away. To do a proximity search use the tilde, "~", 
symbol at the end of a Phrase. For example to search for "apache" and "jakarta" within 10 words of each other 
in a document use the search:

```
"jakarta apache"~10
```

### Range Searches

Range Queries allow one to match documents whose field(s) values are between the lower and upper bound specified 
by the Range Query. Range Queries can be inclusive or exclusive of the upper and lower bounds. 
Sorting is done lexicographically.

```
BYDATE:[20020101 TO 20030101]
```

This will find documents whose BYDATE fields have values between 20020101 and 20030101, inclusive. 
Note that Range Queries are not reserved for date fields. 

You could also use range queries with non-date fields:

```
TITLE:{Aida TO Carmen}
```

This will find all documents whose titles are between "Aida" and "Carmen", but not including "Aida" and "Carmen".

Inclusive range queries are denoted by square brackets. Exclusive range queries are denoted by curly brackets.

### Boosting a Term

Lucene provides the relevance level of matching documents based on the terms found. To boost a term use the caret, "^", 
symbol with a boost factor (a number) at the end of the term you are searching. 
The higher the boost factor, the more relevant the term will be.

Boosting allows you to control the relevance of a document by boosting its term. For example, if you are searching for

```
jakarta apache
```

and you want the term "jakarta" to be more relevant boost it using the ^ symbol along with the boost factor next to the term. 
You would type:

```
jakarta^4 apache
```

This will make documents with the term jakarta appear more relevant. You can also boost Phrase Terms as in the example:

```
"jakarta apache"^4 "Apache Lucene"
```

By default, the boost factor is 1. Although the boost factor must be positive, it can be less than 1 (e.g. 0.2)

### Boolean Operators

Boolean operators allow you to use logical constructions when setting
search conditions, and allow you to combine several terms.
Lucene supports the following logical operators: `AND`, `+`, `OR`, `NOT`, `-`.

Boolean operators must be specified in capital letters.

#### OR operator

`OR` is the default logical operator, which means that if
no other logical operator is specified between the two terms of the search phrase, then the `OR` operator is substituted. In this case, the search system finds
the document if one of the terms specified in the search phrase is present in it.
An alternative notation for the `OR` operator is `||`.

```
"Hello world" "world"
```

Equivalent to:

```
"Hello world" OR "world"
```

#### AND operator

The `AND` operator indicates that all search terms combined by the operator must be present in the text.
An alternative notation of the operator is `&&`.

```
"Hello" AND "world"
```

#### Operator +

The `+` operator indicates that the word following it must necessarily be present in the text.
For example, to search for records that must contain the word "hello" and may
contain the word "world", the search phrase may look like:

```
+Hello world
```

#### NOT operator

The `NOT` operator allows you to exclude from the search results those in which the term following the operator occurs. Instead of the word `NOT`, 
the symbol "!" can be used. For example, to search for records that should contain the word "hello" and should not contain the word "world", 
the search phrase may look like:

```
"Hello" NOT "world"
```

Note: The `NOT` operator cannot be used with only one term. For example, a search with this condition will not return results:

```
NOT "world"
```

#### Operator –

This operator is analogous to the `NOT` operator. Usage example:

```
"Hello" -"world"
```

#### Grouping

Lucene supports using parentheses to group clauses to form sub queries. This can be very useful if you want to control the boolean logic for a query.

To search for either "jakarta" or "apache" and "website" use the query:

```
(jakarta OR apache) AND website
```

This eliminates any confusion and makes sure you that "website" must exist and either term "jakarta" or "apache" may exist.

### Field Grouping

Lucene supports using parentheses to group multiple clauses to a single field.

To search for a title that contains both the word "return" and the phrase "pink panther" use the query:

```
TITLE:(+return +"pink panther")
```

### Escaping Special Characters

Lucene supports escaping special characters that are part of the query syntax. 
The current list special characters are

```
+ - && || ! ( ) { } [ ] ^ " ~ * ? : \
```

To escape these character use the "\\"  before the character. 
For example to search for "(1 + 1) : 2" use the query:


```
\( 1 \+ 1 \) \: 2
```

To escape special characters, you can use the `FTS$ESCAPE_QUERY` function.

```sql
FTS$ESCAPE_QUERY('(1 + 1) : 2')
```

A more detailed English-language description of the syntax is available on the official website
Lucene: [https://lucene.apache.org/core/3_0_3/queryparsersyntax.html](https://lucene.apache.org/core/3_0_3/queryparsersyntax.html).

## Indexing views

You can index not only permanent tables, but also complex views.

In order to index a view, one requirement must be met:
there must be a field in the view by which you can uniquely identify the record.

Let's say you have a view `V_PRODUCT_CATEGORIES`, where `PRODUCT_UUID` is the unique identifier of the `PRODUCTS` table:

```sql
CREATE TABLE CATEGORIES (
    ID             BIGINT GENERATED BY DEFAULT AS IDENTITY,
    CATEGORY_NAME  VARCHAR(80) NOT NULL,
    CONSTRAINT PK_CATEGORY PRIMARY KEY (ID),
    CONSTRAINT UNQ_CATEGORY_NAME UNIQUE (CATEGORY_NAME)
);

CREATE TABLE PRODUCT_CATEGORIES (
    ID            BIGINT GENERATED BY DEFAULT AS IDENTITY,
    PRODUCT_UUID  CHAR(16) CHARACTER SET OCTETS NOT NULL,
    CATEGORY_ID   BIGINT NOT NULL,
    CONSTRAINT PK_PRODUCT_CATEGORIES PRIMARY KEY (ID),
    CONSTRAINT UNQ_PRODUCT_CATEGORIES UNIQUE (PRODUCT_UUID, CATEGORY_ID),
    CONSTRAINT FK_PRODUCT_CAT_REF_CATEGORY FOREIGN KEY (CATEGORY_ID) REFERENCES CATEGORIES (ID),
    CONSTRAINT FK_PRODUCT_CAT_REF_PRODUCT FOREIGN KEY (PRODUCT_UUID) REFERENCES PRODUCTS (PRODUCT_UUID)
);

CREATE OR ALTER VIEW V_PRODUCT_CATEGORIES(
    PRODUCT_UUID,
    CATEGORIES)
AS
SELECT
    PC.PRODUCT_UUID
  , LIST(C.CATEGORY_NAME, ' | ') AS CATEGORIES
FROM PRODUCT_CATEGORIES PC
JOIN CATEGORIES C
     ON C.ID = PC.CATEGORY_ID
GROUP BY 1
;
```

You want to search for products of a category, but the name of the category is in the reference table and one product can have several categories. 
In this case, you can create the following full-text index:

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_CATEGORIES', 'V_PRODUCT_CATEGORIES', 'ENGLISH', 'PRODUCT_UUID');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_CATEGORIES', 'CATEGORIES');

COMMIT;

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_CATEGORIES');

COMMIT;
```

The search for a product by its category looks like this:

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_UUID,
  P.PRODUCT_NAME,
  PC.CATEGORIES,
  FTS.FTS$EXPLANATION
FROM FTS$SEARCH('IDX_PRODUCT_CATEGORIES', '"Toys & Games"') FTS
JOIN V_PRODUCT_CATEGORIES PC ON PC.PRODUCT_UUID = FTS.FTS$UUID
JOIN PRODUCTS P ON P.PRODUCT_UUID = PC.PRODUCT_UUID;
```

## Text analysis

The result of the search depends on which analyzer was used to create the index.
The analyzer performs the following functions: splits the text into separate words, converts words to lowercase,
removal of stop words, other filters, stemming. As a result of text analysis for indexing, terms will be selected from the text, which fall into the index.

In order to find out which terms are included in the index, you can use the stored procedure `FTS$ANALYZE`.

```sql
PROCEDURE FTS$ANALYZE (
    FTS$TEXT     BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
    FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD')
RETURNS (
    FTS$TERM VARCHAR(8191) CHARACTER SET UTF8
)
```

The first parameter specifies the text to be analyzed, and the second parameter is the name of the analyzer.

Usage example:

```sql
SELECT FTS$TERM
FROM FTS$ANALYZE('IBSurgeon FTS UDR library implements full text search procedures and functions for Firebird SQL', 'STANDARD')
```

## Highlighting found terms in a text fragment

It is often necessary not only to find documents on request, but also to highlight what was found.

To highlight the found terms in a text fragment, the package `FTS$HIGHLIGHTER` is used. The package contains:

- function `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT` to highlight the found terms in a text fragment;
- procedure `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS` returns several fragments of text with the highlight of terms in the fragment.

### Highlighting found terms using the FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT function

The function `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT` returns the best text fragment in which the found terms are highlighted with tags.

The function is described as

```sql
  FUNCTION FTS$BEST_FRAGMENT (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>')
  RETURNS VARCHAR(8191) CHARACTER SET UTF8;
```

The `FTS$TEXT` parameter specifies the text in which fragments are searched and selected.

The `FTS$QUERY` parameter specifies the search phrase.

The third optional parameter `FTS$ANALYZER` specifies the name of the analyzer with which the terms are allocated.

The `FTS$FIELD_NAME` parameter specifies the name of the field being searched for. It must be specified if the search query explicitly contains several fields,
otherwise the parameter can be omitted or set as NULL.

The `FTS$FRAGMENT_SIZE` parameter specifies a limit on the length of the returned fragment.
Please note that the actual length of the returned text may be longer. The returned fragment usually does not break the words,
in addition, it does not take into account the length of the tags themselves for selection.

The `FTS$LEFT_TAG` parameter specifies the tag that is added to the found term on the left.

The `FTS$RIGHT_TAG` parameter specifies the tag that is added to the found fragment on the right.

The simplest example of use:

```sql
SELECT
  FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT(
    q'!!Go to your orders and start the return Select the ship method Ship it! |
    Go to your orders and start the return Select the ship method Ship it! |
    show up to 2 reviews by default A shiny Pewter key ring with a 3D element
    of a rotating golf ball made of a PVC material. This makes a great accessory
    for your sports bag. | 1.12 ounces (View shipping rates and policies)!!',
    'A shiny Pewter',
    'English',
    NULL
  ) AS TEXT_FRAGMENT
FROM RDB$DATABASE
```

Now let's combine the search itself and the selection of the found terms:

```sql
EXECUTE BLOCK (
  FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8 = :FTS_QUERY
)
RETURNS (
  FTS$SCORE DOUBLE PRECISION,
  PRODUCT_ID TYPE OF COLUMN PRODUCTS.PRODUCT_ID,
  PRODUCT_NAME TYPE OF COLUMN PRODUCTS.PRODUCT_NAME,
  ABOUT_PRODUCT TYPE OF COLUMN PRODUCTS.ABOUT_PRODUCT,
  HIGHTLIGHT_PRODUCT_NAME VARCHAR(8191) CHARACTER SET UTF8,
  HIGHTLIGHT_ABOUT_PRODUCT VARCHAR(8191) CHARACTER SET UTF8
)
AS
BEGIN
  FOR
    SELECT
      FTS.FTS$SCORE,
      PRODUCTS.PRODUCT_ID,
      PRODUCTS.PRODUCT_NAME,
      PRODUCTS.ABOUT_PRODUCT,
      FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT(PRODUCTS.PRODUCT_NAME, :FTS$QUERY, 'ENGLISH', 'PRODUCT_NAME') AS HIGHTLIGHT_PRODUCT_NAME,
      FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT(PRODUCTS.ABOUT_PRODUCT, :FTS$QUERY, 'ENGLISH', 'ABOUT_PRODUCT') AS HIGHTLIGHT_ABOUT_PRODUCT
    FROM FTS$SEARCH('IDX_PRODUCT_ID_2_EN', :FTS$QUERY, 25) FTS
    JOIN PRODUCTS ON PRODUCTS.PRODUCT_ID = FTS.FTS$ID
  INTO
    FTS$SCORE,
    PRODUCT_ID,
    PRODUCT_NAME,
    ABOUT_PRODUCT,
    HIGHTLIGHT_PRODUCT_NAME,
    HIGHTLIGHT_ABOUT_PRODUCT
  DO
    SUSPEND;
END
```

### Highlighting the found terms using the FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS procedure

The procedure `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS` returns several fragments of text in which the found terms are marked with tags.

The procedure is described as

```sql
  PROCEDURE FTS$BEST_FRAGMENTS (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
      FTS$MAX_NUM_FRAGMENTS INTEGER NOT NULL DEFAULT 10,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>')
  RETURNS (
      FTS$FRAGMENT VARCHAR(8191) CHARACTER SET UTF8);
```

The input parameters of the procedure `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS` are identical to the parameters of 
the function `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT`, but there is one additional parameter `FTS$MAX_NUM_FRAGMENTS`, 
which limits the number of fragments returned.

The text of the found fragments with selected occurrences of terms is returned to the output parameter `FTS$FRAGMENT`. 
This procedure should be applied in one document already found.

Usage example:

```sql
SELECT
    BOOKS.TITLE
  , BOOKS.CONTENT
  , F.FTS$FRAGMENT
FROM BOOKS
LEFT JOIN FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS(
  BOOKS.CONTENT,
  'friendly',
  'ENGLISH'
) F ON TRUE
WHERE BOOKS.ID = 8
```

## Keeping data up-to-date in full-text indexes

There are several ways to keep full-text indexes up-to- date:

1. Periodically call the procedure `FTS$MANAGEMENT.FTS$REBUILD_INDEX` for the specified index.
This method completely rebuilds the full-text index. In this case, all records of the table or view are read
for which the index was created.

2. You can maintain full-text indexes using triggers and calling one of the `FTS$LOG_BY_ID` procedures inside them,
`FTS$LOG_BY_UUID` or `FTS$LOG_BY_DBKEY`. Which of the procedures to call
depends on which type of field is selected as the key (integer, UUID (GIUD) or `RDB$DB_KEY`).
When calling these procedures, the change record is added to a special table `FTS$LOG` (change log).
Changes from the log are transferred to full-text indexes by calling the procedure `FTS$UPDATE_INDEXES`.
The call to this procedure must be done in a separate script, which can be placed in the task scheduler (Windows)
or cron (Linux) with some frequency, for example 5 minutes.

3. Delayed updating of full-text indexes, using FirebirdStreaming technology. In this case, a special
service reads the replication logs and extracts from them the information necessary to update the full-text indexes.
(under development).

### Triggers to keep full-text indexes up-to-date

To maintain the relevance of full-text indexes, it is necessary to create triggers that, when changing
any of the fields included in the full-text index, writes information about the record change to a special table
`FTS$LOG` (log).

Rules for writing triggers to support full-text indexes:

1. In the trigger, it is necessary to check all fields that participate in the full-text index.
The field validation conditions must be combined via `OR`.

2. For the `INSERT` operation, it is necessary to check all fields included in full-text indexes whose value is different
from `NULL`. If this condition is met, then one of the procedures must be performed
`FTS$LOG_BY_DBKEY('<table name>', NEW.RDB$DB_KEY, 'I');` or `FTS$LOG_BY_ID('<table name>', NEW.<key field>, 'I')`
or `FTS$LOG_BY_UUID('<table name>', NEW.<key field>, 'I')`.

3. For the `UPDATE` operation, it is necessary to check all fields included in full-text indexes whose value has changed.
If this condition is met, then the procedure `FTS$LOG_BY_DBKEY('<table name>', OLD.RDB$DB_KEY, 'U');`
or `FTS$LOG_BY_ID('<table name>', OLD.<key field>, 'U')`or `FTS$LOG_BY_UUID('<table name>', OLD.<key field>, 'U')`.

4. For the `DELETE` operation, it is necessary to check all fields included in full-text indexes whose value is different
from `NULL`. If this condition is met, then it is necessary to perform the procedure
`FTS$LOG_CHANGE('<table name>', OLD.RDB$DB_KEY, 'D');`.

To facilitate the task of writing such triggers, there is a special package `FTS$TRIGGER_HELPER`, which
contains procedures for generating trigger source texts. So for example, in order to generate triggers
to support full-text indexes created for the `PRODUCTS` table, you need to run the following query:

```sql
SELECT
    FTS$TRIGGER_SCRIPT
FROM FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS('PRODUCTS', TRUE)
```

This query will return the following trigger text for all created FTS indexes on the `PRODUCTS` table:

```sql
CREATE OR ALTER TRIGGER "FTS$PRODUCTS_AIUD" FOR "PRODUCTS"
ACTIVE AFTER INSERT OR UPDATE OR DELETE
POSITION 100
AS
BEGIN
  /* Block for key PRODUCT_ID */
  IF (INSERTING AND (NEW."ABOUT_PRODUCT" IS NOT NULL
      OR NEW."PRODUCT_NAME" IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_ID('PRODUCTS', NEW."PRODUCT_ID", 'I');
  IF (UPDATING AND (NEW."ABOUT_PRODUCT" IS DISTINCT FROM OLD."ABOUT_PRODUCT"
      OR NEW."PRODUCT_NAME" IS DISTINCT FROM OLD."PRODUCT_NAME")) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_ID('PRODUCTS', OLD."PRODUCT_ID", 'U');
  IF (DELETING AND (OLD."ABOUT_PRODUCT" IS NOT NULL
      OR OLD."PRODUCT_NAME" IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_ID('PRODUCTS', OLD."PRODUCT_ID", 'D');
  /* Block for key PRODUCT_UUID */
  IF (INSERTING AND (NEW."PRODUCT_NAME" IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_UUID('PRODUCTS', NEW."PRODUCT_UUID", 'I');
  IF (UPDATING AND (NEW."PRODUCT_NAME" IS DISTINCT FROM OLD."PRODUCT_NAME")) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_UUID('PRODUCTS', OLD."PRODUCT_UUID", 'U');
  IF (DELETING AND (OLD."PRODUCT_NAME" IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_UUID('PRODUCTS', OLD."PRODUCT_UUID", 'D');
  /* Block for key RDB$DB_KEY */
  IF (INSERTING AND (NEW."PRODUCT_NAME" IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_DBKEY('PRODUCTS', NEW.RDB$DB_KEY, 'I');
  IF (UPDATING AND (NEW."PRODUCT_NAME" IS DISTINCT FROM OLD."PRODUCT_NAME")) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_DBKEY('PRODUCTS', OLD.RDB$DB_KEY, 'U');
  IF (DELETING AND (OLD."PRODUCT_NAME" IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_DBKEY('PRODUCTS', OLD.RDB$DB_KEY, 'D');
END
```

Updating all full-text indexes, you need to create an SQL script `fts$update.sql`

```sql
EXECUTE PROCEDURE FTS$UPDATE_INDEXES;
```

Then a script to call the SQL script via ISQL, something like the following

```bash
isql -user SYSDBA -pas masterkey -i fts$update.sql inet://localhost/mydatabase
```

Pay attention! The package `FTS$TRIGGER_HELPER` helps to generate triggers to support full-text indexes
only for regular tables. If you want to maintain a full-text index on the view, then you need
to develop such triggers for the base tables of the view yourself.
Below is an example that supports a full-text index of triggers for a view `V_PRODUCT_CATEGORIES`.

```sql
SET TERM ^;

-- Field PRODUCT_UUID and CATEGORY_ID from table PRODUCT_CATEGORIES
CREATE OR ALTER TRIGGER FTS$PRODUCT_CATEGORIES_AIUD FOR PRODUCT_CATEGORIES
ACTIVE AFTER INSERT OR UPDATE OR DELETE
POSITION 100
AS
BEGIN
  IF (INSERTING) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_UUID('V_PRODUCT_CATEGORIES', NEW.PRODUCT_UUID, 'I');

  IF (UPDATING AND (NEW.PRODUCT_UUID <> OLD.PRODUCT_UUID
      OR NEW.CATEGORY_ID <> OLD.CATEGORY_ID)) THEN
  BEGIN
    EXECUTE PROCEDURE FTS$LOG_BY_UUID('V_PRODUCT_CATEGORIES', OLD.PRODUCT_UUID, 'D');
    EXECUTE PROCEDURE FTS$LOG_BY_UUID('V_PRODUCT_CATEGORIES', NEW.PRODUCT_UUID, 'I');
  END

  IF (DELETING) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_UUID('V_PRODUCT_CATEGORIES', OLD.PRODUCT_UUID, 'D');
END
^

-- Change from table CATEGORIES
CREATE OR ALTER TRIGGER FTS$CATEGORIES_AU FOR CATEGORIES
ACTIVE AFTER UPDATE
POSITION 100
AS
DECLARE PRODUCT_UUID TYPE OF COLUMN PRODUCT_CATEGORIES.PRODUCT_UUID;
BEGIN
  IF (NEW.CATEGORY_NAME <> OLD.CATEGORY_NAME) THEN
  BEGIN
    SELECT MAX(PRODUCT_CATEGORIES.PRODUCT_UUID)
    FROM PRODUCT_CATEGORIES
    JOIN CATEGORIES ON CATEGORIES.ID = PRODUCT_CATEGORIES.CATEGORY_ID
    WHERE CATEGORIES.CATEGORY_NAME = OLD.CATEGORY_NAME
    INTO PRODUCT_UUID;

    EXECUTE PROCEDURE FTS$LOG_BY_UUID('V_PRODUCT_CATEGORIES', :PRODUCT_UUID, 'U');
  END
END
END
^

SET TERM ;^
```

## Description of procedures and functions for working with full-text search

### FTS$MANAGEMENT package

The `FTS$MANAGEMENT` package contains procedures and functions for managing full-text indexes. This package is intended
for database administrators.

#### Function FTS$MANAGEMENT.FTS$GET_DIRECTORY

The function `FTS$MANAGEMENT.FTS$GET_DIRECTORY` returns the directory where the files and folders of full-text indexes for
the current database are located.

```sql
  FUNCTION FTS$GET_DIRECTORY ()
  RETURNS VARCHAR(255) CHARACTER SET UTF8
  DETERMINISTIC;
```

#### Procedure FTS$MANAGEMENT.FTS$ANALYZERS

The procedure `FTS$MANAGEMENT.FTS$ANALYZERS` returns a list of available analyzers.

```sql
  PROCEDURE FTS$ANALYZERS
  RETURNS (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
      FTS$BASE_ANALYZER VARCHAR(63) CHARACTER SET UTF8,
      FTS$STOP_WORDS_SUPPORTED BOOLEAN,
      FTS$SYSTEM_FLAG BOOLEAN);
```

Output parameters:

- FTS$ANALYZER - analyzer name;
- FTS$BASE_ANALYZER - name of base analyzer;
- FTS$STOP_WORDS_SUPPORTED - stop words supported;
- FTS$SYSTEM_FLAG - is system analyzer.

#### Procedure FTS$MANAGEMENT.FTS$CREATE_ANALYZER

The `FTS$MANAGEMENT.FTS$CREATE_ANALYZER` procedure creates a new analyzer based on the base one. One of the built-in analyzers can be used as a base one.

```sql
  PROCEDURE FTS$CREATE_ANALYZER (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$BASE_ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8 DEFAULT NULL);
```

Input parameters:

- FTS$ANALYZER - analyzer name;
- FTS$BASE_ANALYZER - base analyzer name;
- FTS$DESCRIPTION - analyzer description.

#### Procedure FTS$MANAGEMENT.FTS$DROP_ANALYZER

The procedure `FTS$MANAGEMENT.FTS$DROP_ANALYZER` drops a custom analyzer.

```sql
  PROCEDURE FTS$DROP_ANALYZER (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
```

Input parameters:

- FTS$ANALYZER - analyzer name.

#### Procedure FTS$MANAGEMENT.FTS$ANALYZER_STOP_WORDS

The procedure `FTS$MANAGEMENT.FTS$ANALYZER_STOP_WORDS` returns a list of stop words for the given analyzer.

```sql
  PROCEDURE FTS$ANALYZER_STOP_WORDS (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  RETURNS (
      FTS$WORD VARCHAR(63) CHARACTER SET UTF8);
```

Input parameters:

- FTS$ANALYZER - analyzer name.

Output parameters:

- FTS$WORD - stop word.

#### Procedure FTS$MANAGEMENT.FTS$ADD_STOP_WORD

The procedure `FTS$MANAGEMENT.FTS$ADD_STOP_WORD` adds a stop word to the custom analyzer.

```sql
  PROCEDURE FTS$ADD_STOP_WORD (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$WORD VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
```

Input parameters:

- FTS$ANALYZER - analyzer name;
- FTS$WORD - stop word.

#### Procedure FTS$MANAGEMENT.FTS$DROP_STOP_WORD

The procedure `FTS$MANAGEMENT.FTS$DROP_STOP_WORD` removes a stop word from the custom analyzer.

```sql
  PROCEDURE FTS$DROP_STOP_WORD (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$WORD VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
```

Входные параметры:

- FTS$ANALYZER - analyzer name;
- FTS$WORD - stop word.

#### Procedure FTS$MANAGEMENT.FTS$CREATE_INDEX

The procedure `FTS$MANAGEMENT.FTS$CREATE_INDEX` creates a new full-text index.

```sql
  PROCEDURE FTS$CREATE_INDEX (
      FTS$INDEX_NAME     VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$ANALYZER       VARCHAR(63) CHARACTER SET UTF8 DEFAULT 'STANDARD',
      FTS$KEY_FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8 DEFAULT NULL);
```

Input parameters:

- FTS$INDEX_NAME - index name. Must be unique among full-text index names;
- FTS$RELATION_NAME - name of the table to be indexed;
- FTS$ANALYZER - the name of the analyzer. If not specified, the STANDARD analyzer (StandardAnalyzer) is used;
- FTS$KEY_FIELD_NAME - the name of the field whose value will be returned by the search procedure `FTS$SEARCH`, usually this is the key field of the table;
- FTS$DESCRIPTION - description of the index.

#### Procedure FTS$MANAGEMENT.FTS$DROP_INDEX

The procedure `FTS$MANAGEMENT.FTS$DROP_INDEX` deletes the full-text index.

```sql
  PROCEDURE FTS$DROP_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
```

Input parameters:

- FTS$INDEX_NAME - index name.

#### Procedure FTS$MANAGEMENT.SET_INDEX_ACTIVE

The procedure `FTS$MANAGEMENT.SET_INDEX_ACTIVE` allows you to make the index active or inactive.

```sql
  PROCEDURE FTS$SET_INDEX_ACTIVE (
      FTS$INDEX_NAME   VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$INDEX_ACTIVE BOOLEAN NOT NULL);
```

Input parameters:

- FTS$INDEX_NAME - index name;
- FTS$INDEX_ACTIVE - activity flag.

#### Procedure FTS$MANAGEMENT.FTS$COMMENT_ON_INDEX

The procedure `FTS$MANAGEMENT.FTS$COMMENT_ON_INDEX` adds or deletes a user comment to the index.

```sql
  PROCEDURE FTS$COMMENT_ON_INDEX (
      FTS$INDEX_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8);
```

Input parameters:

- FTS$INDEX_NAME - index name;
- FTS$DESCRIPTION - user description of the index.

#### Procedure FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD

The procedure `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD` adds a new field to the full-text index.

```sql
  PROCEDURE FTS$ADD_INDEX_FIELD (
      FTS$INDEX_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$BOOST         DOUBLE PRECISION DEFAULT NULL);
```

Input parameters:

- FTS$INDEX_NAME - index name;
- FTS$FIELD_NAME - the name of the field to be indexed;
- FTS$BOOST - the coefficient of increasing the significance of the segment (by default 1.0).

#### Procedure FTS$MANAGEMENT.FTS$DROP_INDEX_FIELD

The procedure `FTS$MANAGEMENT.FTS$DROP_INDEX_FIELD` removes the field from the full-text index.

```sql
  PROCEDURE FTS$DROP_INDEX_FIELD (
      FTS$INDEX_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
```

Input parameters:

- FTS$INDEX_NAME - index name;
- FTS$FIELD_NAME - field name.

#### Procedure FTS$MANAGEMENT.FTS$SET_INDEX_FIELD_BOOST

The procedure `FTS$MANAGEMENT.FTS$SET_INDEX_FIELD_BOOST` sets the significance coefficient for the index field.

```sql
  PROCEDURE FTS$SET_INDEX_FIELD_BOOST (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$BOOST DOUBLE PRECISION);
```

Input parameters:

- FTS$INDEX_NAME - index name;
- FTS$FIELD_NAME - the name of the field to be indexed;
- FTS$BOOST - the coefficient of increasing the significance of the segment.

If you do not specify a significance factor when adding a field to the index, then by default it is 1.0.
Using the procedure `FTS$MANAGEMENT.FTS$SET_INDEX_FIELD_BOOST` it can be changed.
Note that after running this procedure, the index needs to be rebuilt.

#### Procedure FTS$MANAGEMENT.FTS$REBUILD_INDEX

The procedure `FTS$MANAGEMENT.FTS$REBUILD_INDEX` rebuilds the full-text index.

```sql
  PROCEDURE FTS$REBUILD_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
```

Input parameters:

- FTS$INDEX_NAME - index name.

#### Procedure FTS$MANAGEMENT.FTS$REINDEX_TABLE

The procedure `FTS$MANAGEMENT.FTS$REINDEX_TABLE` rebuilds all full-text indexes for the specified table.

```sql
  PROCEDURE FTS$REINDEX_TABLE (
      FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
```

Input parameters:

- FTS$RELATION_NAME - the name of the table.

#### Procedure FTS$MANAGEMENT.FTS$FULL_REINDEX

The procedure `FTS$MANAGEMENT.FTS$FULL_REINDEX` rebuilds all full-text indexes in the database.

#### Procedure FTS$MANAGEMENT.FTS$OPTIMIZE_INDEX

The procedure `FTS$MANAGEMENT.FTS$OPTIMIZE_INDEX` optimizes the specified index.

```sql
  PROCEDURE FTS$OPTIMIZE_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
```

Input parameters:

- FTS$INDEX_NAME - index name.

#### Procedure FTS$MANAGEMENT.FTS$OPTIMIZE_INDEXES

The procedure `FTS$MANAGEMENT.FTS$OPTIMIZE_INDEXES` optimizes all full-text indexes in the database.

### FTS$SEARCH procedure

The `FTS$SEARCH` procedure performs a full-text search by the specified index.

```sql
PROCEDURE FTS$SEARCH (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
    FTS$LIMIT INT NOT NULL DEFAULT 1000,
    FTS$EXPLAIN BOOLEAN DEFAULT FALSE
)
RETURNS (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
    FTS$KEY_FIELD_NAME VARCHAR(63) CHARACTER SET UTF8,
    FTS$DB_KEY CHAR(8) CHARACTER SET OCTETS,
    FTS$ID BIGINT,
    FTS$UUID CHAR(16) CHARACTER SET OCTETS,
    FTS$SCORE DOUBLE PRECISION,
    FTS$EXPLANATION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
```

Input parameters:

- FTS$INDEX_NAME - the name of the full-text index in which the search is performed;
- FTS$QUERY - expression for full-text search;
- FTS$LIMIT - limit on the number of records (search result). By default, 1000;
- FTS$EXPLAIN - whether to explain the search result. By default, FALSE.

Output parameters:

- FTS$RELATION_NAME - the name of the table in which the document was found;
- FTS$KEY_FIELD_NAME - the name of the key field in the table;
- FTS$DB_KEY - the value of the key field in the format `RDB$DB_KEY`;
- FTS$ID - value of a key field of type `BIGINT` or `INTEGER`;
- FTS$UUID - value of a key field of type `BINARY(16)`. This type is used to store the GUID;
- FTS$SCORE - the degree of compliance with the search query;
- FTS$EXPLANATION - explanation of search results.

### Function FTS$ESCAPE_QUERY

The 'FTS$ESCAPE_QUERY` function escapes special characters in the search query.

```sql
FUNCTION FTS$ESCAPE_QUERY (
    FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8
)
RETURNS VARCHAR(8191) CHARACTER SET UTF8;
```

Input parameters:

- FTS$QUERY - a search query or part of it in which special characters need to be escaped.

### Procedure FTS$ANALYZE

The `FTS$ANALYZE` procedure analyzes the text according to the given analyzer and returns a list of terms.

```sql
PROCEDURE FTS$ANALYZE (
    FTS$TEXT     BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
    FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD')
RETURNS (
    FTS$TERM VARCHAR(8191) CHARACTER SET UTF8
)
```

Input parameters:

- FTS$TEXT - text for analysis;
- FTS$ANALYZER - analyzer.

Output parameters:

- FTS$TERM - term.

### Procedure FTS$LOG_BY_ID

The procedure `FTS$LOG_BY_ID` adds a record of a change in one of the fields included in the full-text indexes
built on the table to the change log `FTS$LOG`, on the basis of which the full-text indexes will be updated.
This procedure should be used if an integer field is used as the primary key. Such keys
are often generated using generators/sequences.

```sql
PROCEDURE FTS$LOG_BY_ID (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$ID            BIGINT NOT NULL,
    FTS$CHANGE_TYPE   FTS$D_CHANGE_TYPE NOT NULL
)
```

Input parameters:

- FTS$RELATION_NAME - the name of the table for which the change record is added;
- FTS$ID - value of the key field;
- FTS$CHANGE_TYPE - type of change (I - INSERT, U - UPDATE, D - DELETE).

### Procedure FTS$LOG_BY_UUID

The procedure `FTS$LOG_BY_UUID` adds a record of a change in one of the fields included in the full-text indexes
built on the table to the change log `FTS$LOG`, on the basis of which the full-text indexes will be updated.
This procedure should be used if a UUID (GUID) is used as the primary key. Such keys
are often generated using the `GEN_UUID` function.

```sql
PROCEDURE FTS$LOG_BY_UUID (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$UUID          CHAR(16) CHARACTER SET OCTETS NOT NULL,
    FTS$CHANGE_TYPE   FTS$D_CHANGE_TYPE NOT NULL
)
```

Input parameters:

- FTS$RELATION_NAME - the name of the table for which the change record is added;
- FTS$UUID - value of the key field;
- FTS$CHANGE_TYPE - type of change (I - INSERT, U - UPDATE, D - DELETE).

### Procedure FTS$LOG_BY_DBKEY

The procedure `FTS$LOG_BY_DBKEY` adds a record of a change in one of the fields included in the full-text indexes
built on the table to the change log `FTS$LOG`, on the basis of which the full-text indexes will be updated.
This procedure should be used if the pseudo field `RDB$DB_KEY` is used as the primary key.

```sql
PROCEDURE FTS$LOG_BY_DBKEY (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$DBKEY         CHAR(8) CHARACTER SET OCTETS NOT NULL,
    FTS$CHANGE_TYPE   FTS$D_CHANGE_TYPE NOT NULL
)
```

Input parameters:

- FTS$RELATION_NAME - the name of the table for which the change record is added;
- FTS$DBKEY - value of the pseudo field `RDB$DB_KEY`;
- FTS$CHANGE_TYPE - type of change (I - INSERT, U - UPDATE, D - DELETE).

### Procedure FTS$CLEAR_LOG

The procedure `FTS$CLEAR_LOG` clears the change log `FTS$LOG`, based on which the full-text indexes are updated.

### Procedure FTS$UPDATE_INDEXES

The procedure `FTS$UPDATE_INDEXES` updates full-text indexes on entries in the change log `FTS$LOG`.
This procedure is usually run on a schedule (cron) in a separate session with some interval, for example 5 seconds.

### FTS$HIGHLIGHTER package

The `FTS$HIGHLIGHTER` package contains procedures and functions that return fragments of the text in which the original phrase was found,
and highlights the terms found.

#### Function FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT

The `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT` function returns the best text fragment that matches the full-text search expression
and highlights the terms found in it.

```sql
  FUNCTION FTS$BEST_FRAGMENT (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>')
  RETURNS VARCHAR(8191) CHARACTER SET UTF8;
```

Input parameters:

- FTS$TEXT - the text in which the search is done;
- FTS$QUERY - full-text search expression;
- FTS$ANALYZER - analyzer;
- FTS$FIELD_NAME — the name of the field in which the search is performed;
- FTS$FRAGMENT_SIZE - the length of the returned fragment. No less than is required to return whole words;
- FTS$LEFT_TAG - left tag for highlighting;
- FTS$RIGHT_TAG - right tag for highlighting.

#### Procedure FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS

The procedure `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS` returns the best text fragments that match the full-text search expression
and highlights the terms found in them.

```sql
  PROCEDURE FTS$BEST_FRAGMENTS (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
      FTS$MAX_NUM_FRAGMENTS INTEGER NOT NULL DEFAULT 10,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>')
  RETURNS (
      FTS$FRAGMENT VARCHAR(8191) CHARACTER SET UTF8);
```

Input parameters:

- FTS$TEXT - the text in which the search is done;
- FTS$QUERY - full-text search expression;
- FTS$ANALYZER - analyzer;
- FTS$FIELD_NAME — the name of the field in which the search is performed;
- FTS$FRAGMENT_SIZE - the length of the returned fragment. No less than is required to return whole words;
- FTS$MAX_NUM_FRAGMENTS - maximum number of fragments;
- FTS$LEFT_TAG - left tag for highlighting;
- FTS$RIGHT_TAG - right tag for highlighting.

Output parameters:

- FTS$FRAGMENT - a text fragment corresponding to the search query.

### FTS$TRIGGER_HELPER package

The package `FTS$TRIGGER_HELPER` contains procedures and functions that help to create triggers to maintain the relevance
of full-text indexes.

#### Procedure FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS

The procedure `FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS` generates trigger source codes for a given table
to keep full-text indexes up to date.

```sql
  PROCEDURE FTS$MAKE_TRIGGERS (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$MULTI_ACTION BOOLEAN NOT NULL DEFAULT TRUE,
    FTS$POSITION SMALLINT NOT NULL DEFAULT 100
  )
  RETURNS (
    FTS$TRIGGER_NAME VARCHAR(63) CHARACTER SET UTF8,
    FTS$TRIGGER_RELATION VARCHAR(63) CHARACTER SET UTF8,
    FTS$TRIGGER_EVENTS VARCHAR(26) CHARACTER SET UTF8,
    FTS$TRIGGER_POSITION SMALLINT,
    FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
    FTS$TRIGGER_SCRIPT BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  );
```

Input parameters:

- FTS$RELATION_NAME - name of the table for which triggers are created;
- FTS$MULTI_ACTION - universal trigger flag. If set to TRUE,
a trigger script for multiple actions will be generated, otherwise a separate trigger script will be generated for each action;
- FTS$POSITION - position of triggers.

Output parameters:

- FTS$TRIGGER_NAME - the name of the trigger;
- FTS$TRIGGER_RELATION - the table for which the trigger is created;
- FTS$TRIGGER_EVENTS - trigger events;
- FTS$TRIGGER_POSITION - trigger position;
- FTS$TRIGGER_SOURCE - the source code of the trigger body;
- FTS$TRIGGER_SCRIPT - trigger creation script.

#### Procedure FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS_BY_INDEX

The procedure `FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS_BY_INDEX` generates trigger source codes for a given index
to keep the full-text index up to date.

```sql
  PROCEDURE FTS$MAKE_TRIGGERS_BY_INDEX (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$MULTI_ACTION BOOLEAN NOT NULL DEFAULT TRUE,
    FTS$POSITION SMALLINT NOT NULL DEFAULT 100
  )
  RETURNS (
    FTS$TRIGGER_NAME VARCHAR(63) CHARACTER SET UTF8,
    FTS$TRIGGER_RELATION VARCHAR(63) CHARACTER SET UTF8,
    FTS$TRIGGER_EVENTS VARCHAR(26) CHARACTER SET UTF8,
    FTS$TRIGGER_POSITION SMALLINT,
    FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
    FTS$TRIGGER_SCRIPT BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  );
```

Input parameters:

- FTS$INDEX_NAME - the name of the index for which triggers are created;
- FTS$MULTI_ACTION - universal trigger flag. If set to TRUE,
a trigger script for multiple actions will be generated, otherwise a separate trigger script will be generated for each action;
- FTS$POSITION - position of triggers.

Output parameters:

- FTS$TRIGGER_NAME - the name of the trigger;
- FTS$TRIGGER_RELATION - the table for which the trigger is created;
- FTS$TRIGGER_EVENTS - trigger events;
- FTS$TRIGGER_POSITION - trigger position;
- FTS$TRIGGER_SOURCE - the source code of the trigger body;
- FTS$TRIGGER_SCRIPT - trigger creation script.

#### Procedure FTS$TRIGGER_HELPER.FTS$MAKE_ALL_TRIGGERS

The procedure `FTS$TRIGGER_HELPER.FTS$MAKE_ALL_TRIGGERS` generates trigger source codes to keep all full-text indexes up to date.

```sql
  PROCEDURE FTS$MAKE_ALL_TRIGGERS (
    FTS$MULTI_ACTION BOOLEAN NOT NULL DEFAULT TRUE,
    FTS$POSITION SMALLINT NOT NULL DEFAULT 100
  )
  RETURNS (
    FTS$TRIGGER_NAME VARCHAR(63) CHARACTER SET UTF8,
    FTS$TRIGGER_RELATION VARCHAR(63) CHARACTER SET UTF8,
    FTS$TRIGGER_EVENTS VARCHAR(26) CHARACTER SET UTF8,
    FTS$TRIGGER_POSITION SMALLINT,
    FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
    FTS$TRIGGER_SCRIPT BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  );
```

Input parameters:

- FTS$MULTI_ACTION - universal trigger flag. If set to TRUE,
a trigger script for multiple actions will be generated, otherwise a separate trigger script will be generated for each action;
- FTS$POSITION - position of triggers.

Output parameters:

- FTS$TRIGGER_NAME - the name of the trigger;
- FTS$TRIGGER_RELATION - the table for which the trigger is created;
- FTS$TRIGGER_EVENTS - trigger events;
- FTS$TRIGGER_POSITION - trigger position;
- FTS$TRIGGER_SOURCE - the source code of the trigger body;
- FTS$TRIGGER_SCRIPT - trigger creation script.

### FTS$STATISTICS package

The `FTS$STATISTICS` package contains procedures and functions for obtaining information about full-text indexes and their statistics.
This package is intended primarily for database administrators.

#### Function FTS$STATISTICS.FTS$LUCENE_VERSION

The function `FTS$STATISTICS.FTS$LUCENE_VERSION` returns the version of the lucene++ library based on which the full-text search is built.

```sql
  FUNCTION FTS$LUCENE_VERSION ()
  RETURNS VARCHAR(20) CHARACTER SET UTF8 
  DETERMINISTIC;
```

#### Function FTS$STATISTICS.FTS$GET_DIRECTORY

The function `FTS$STATISTICS.FTS$GET_DIRECTORY` returns the directory where the files and folders of full-text indexes for
the current database are located.

```sql
  FUNCTION FTS$GET_DIRECTORY ()
  RETURNS VARCHAR(255) CHARACTER SET UTF8 
  DETERMINISTIC;
```

#### Procedure FTS$STATISTICS.FTS$INDEX_STATISTICS

The procedure `FTS$STATISTICS.FTS$INDEX_STATISTICS` returns low-level information and statistics for the specified index.

```sql
  PROCEDURE FTS$INDEX_STATISTICS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  RETURNS (
      FTS$ANALYZER         VARCHAR(63) CHARACTER SET UTF8,
      FTS$INDEX_STATUS     TYPE OF FTS$D_INDEX_STATUS,
      FTS$INDEX_DIRECTORY  VARCHAR(255) CHARACTER SET UTF8,
      FTS$INDEX_EXISTS     BOOLEAN,
      FTS$INDEX_OPTIMIZED  BOOLEAN,
      FTS$HAS_DELETIONS    BOOLEAN,
      FTS$NUM_DOCS         INTEGER,
      FTS$NUM_DELETED_DOCS INTEGER,
      FTS$NUM_FIELDS       SMALLINT,
      FTS$INDEX_SIZE       INTEGER);
```

Input parameters:

- FTS$INDEX_NAME - index name.

Output parameters:

- FTS$ANALYZER - analyzer name;
- FTS$INDEX_STATUS - index status:
    - I - inactive;
    - N - new index (rebuild required);
    - C - completed and active;
    - U - metadata updated (rebuild required);
- FTS$INDEX_DIRECTORY - index location directory;
- FTS$INDEX_EXISTS - does the index physically exist;
- FTS$HAS_DELETIONS - were there any deletions of documents from the index;
- FTS$NUM_DOCS - number of indexed documents;
- FTS$NUM_DELETED_DOCS - number of deleted documents (before optimization);
- FTS$NUM_FIELDS - number of internal index fields;
- FTS$INDEX_SIZE - the size of the index in bytes.

#### Procedure FTS$STATISTICS.FTS$INDICES_STATISTICS

The procedure `FTS$STATISTICS.FTS$INDICES_STATISTICS` returns low-level information and statistics for all full-text indexes.

```sql
  PROCEDURE FTS$INDICES_STATISTICS
  RETURNS (
      FTS$INDEX_NAME       VARCHAR(63) CHARACTER SET UTF8,
      FTS$ANALYZER         VARCHAR(63) CHARACTER SET UTF8,
      FTS$INDEX_STATUS     TYPE OF FTS$D_INDEX_STATUS,
      FTS$INDEX_DIRECTORY  VARCHAR(255) CHARACTER SET UTF8,
      FTS$INDEX_EXISTS     BOOLEAN,
      FTS$INDEX_OPTIMIZED  BOOLEAN,
      FTS$HAS_DELETIONS    BOOLEAN,
      FTS$NUM_DOCS         INTEGER,
      FTS$NUM_DELETED_DOCS INTEGER,
      FTS$NUM_FIELDS       SMALLINT,
      FTS$INDEX_SIZE       INTEGER);
```

Output parameters:

- FTS$INDEX_NAME - index name;
- FTS$ANALYZER - analyzer name;
- FTS$INDEX_STATUS - index status:
    - I - inactive;
    - N - new index (rebuild required);
    - C - completed and active;
    - U - metadata updated (rebuild required);
- FTS$INDEX_DIRECTORY - index location directory;
- FTS$INDEX_EXISTS - does the index physically exist;
- FTS$HAS_DELETIONS - were there any deletions of documents from the index;
- FTS$NUM_DOCS - number of indexed documents;
- FTS$NUM_DELETED_DOCS - number of deleted documents (before optimization);
- FTS$NUM_FIELDS - number of internal index fields;
- FTS$INDEX_SIZE - the size of the index in bytes.

#### Procedure FTS$STATISTICS.FTS$INDEX_SEGMENT_INFOS

The procedure `FTS$STATISTICS.FTS$INDEX_SEGMENT_INFOS` returns information about index segments.
Here the segment is defined from the Lucene perspective.

```sql
  PROCEDURE FTS$INDEX_SEGMENT_INFOS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  RETURNS (
      FTS$SEGMENT_NAME      VARCHAR(63) CHARACTER SET UTF8,
      FTS$DOC_COUNT         INTEGER,
      FTS$SEGMENT_SIZE      INTEGER,
      FTS$USE_COMPOUND_FILE BOOLEAN,
      FTS$HAS_DELETIONS     BOOLEAN,
      FTS$DEL_COUNT         INTEGER,
      FTS$DEL_FILENAME      VARCHAR(255) CHARACTER SET UTF8);
```

Input parameters:

- FTS$INDEX_NAME - index name.

Output parameters:

- FTS$SEGMENT_NAME - segment name;
- FTS$DOC_COUNT - number of documents in the segment;
- FTS$SEGMENT_SIZE - segment size in bytes;
- FTS$USE_COMPOUND_FILE - the segment uses a composite file;
- FTS$HAS_DELETIONS - there were deletions of documents from the segment;
- FTS$DEL_COUNT - number of deleted documents (before optimization);
- FTS$DEL_FILENAME - file with deleted documents.

#### Procedure FTS$STATISTICS.FTS$INDEX_FIELDS

The procedure `FTS$STATISTICS.FTS$INDEX_FIELDS` returns the names of the internal fields of the index.

```sql
  PROCEDURE FTS$INDEX_FIELDS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  RETURNS (
      FTS$FIELD_NAME VARCHAR(127) CHARACTER SET UTF8);
```
   
Input parameters:

- FTS$INDEX_NAME - index name.

Output parameters:

- FTS$FIELD_NAME - field name.

#### Procedure FTS$STATISTICS.FTS$INDEX_FILES

The procedure `FTS$STATISTICS.FTS$INDEX_FILES` returns information about index files.

```sql
  PROCEDURE FTS$INDEX_FILES (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  RETURNS (
      FTS$FILE_NAME VARCHAR(127) CHARACTER SET UTF8,
      FTS$FILE_TYPE VARCHAR(63) CHARACTER SET UTF8,
      FTS$FILE_SIZE INTEGER);
```
   
Input parameters:

- FTS$INDEX_NAME - index name.

Output parameters:

- FTS$FILE_NAME - file name;
- FTS$FILE_TYPE - file type;
- FTS$FILE_SIZE - file size in bytes.

#### Procedure FTS$STATISTICS.FTS$INDEX_FIELD_INFOS

The procedure `FTS$STATISTICS.FTS$INDEX_FIELD_INFOS` returns information about the index fields.

```sql
  PROCEDURE FTS$INDEX_FIELD_INFOS (
      FTS$INDEX_NAME   VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$SEGMENT_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL)
  RETURNS (
      FTS$FIELD_NAME                      VARCHAR(127) CHARACTER SET UTF8,
      FTS$FIELD_NUMBER                    SMALLINT,
      FTS$IS_INDEXED                      BOOLEAN,
      FTS$STORE_TERM_VECTOR               BOOLEAN,
      FTS$STORE_OFFSET_TERM_VECTOR        BOOLEAN,
      FTS$STORE_POSITION_TERM_VECTOR      BOOLEAN,
      FTS$OMIT_NORMS                      BOOLEAN,
      FTS$OMIT_TERM_FREQ_AND_POS          BOOLEAN,
      FTS$STORE_PAYLOADS                  BOOLEAN);
```
   
Input parameters:

- FTS$INDEX_NAME - index name;
- FTS$SEGMENT_NAME - index segment name,
if not specified, the active segment is taken.

Output parameters:

- FTS$FIELD_NAME - field name;
- FTS$FIELD_NUMBER - field number;
- FTS$IS_INDEXED - the field is indexed;
- FTS$STORE_TERM_VECTOR - reserved;
- FTS$STORE_OFFSET_TERM_VECTOR - reserved;
- FTS$STORE_POSITION_TERM_VECTOR - reserved;
- FTS$OMIT_NORMS - reserved;
- FTS$OMIT_TERM_FREQ_AND_POS - reserved;
- FTS$STORE_PAYLOADS - reserved.

