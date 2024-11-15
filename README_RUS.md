# IBSurgeon Full Text Search UDR

Библиотека IBSurgeon FTS UDR реализует процедуры и функции полнотекстового поиска для Firebird SQL, чтобы запускать их в SQL-запросах,
используя мощные возможности поисковой системы Lucene.
В этой UDR используется [Lucene++](https://github.com/luceneplusplus/LucenePlusPlus), это реализация поисковой системы Lucene на C++
для достижения максимально быстрого поиска с истинными возможностями полнотекстового поиска.
Эта UDR является 100% бесплатной и с открытым исходным кодом, с лицензией LGPL.

Доступны версии для Windows и Linux: для Windows у нас есть готовые к использованию двоичные файлы, а для Linux необходимо собрать UDR из исходных кодов
в зависимости от конкретного дистрибутива (у нас есть простая инструкция по сборке).

Библиотека разработана за счет гранта IBSurgeon [www.ib-aid.com](https://www.ib-aid.com).

## Установка Lucene UDR

Для установки Lucene UDR необходимо:

1. Распаковать zip архив с динамическими библиотеками в каталог `plugins\udr`
2. Выполнить скрипт [fts$install.sql](https://github.com/IBSurgeon/lucene_udr/blob/main/sql/fts%24install.sql) 
для регистрации процедур и функций в индексируемой БД. 
Для баз данных 1-ого SQL диалекта используйте скрипт [fts$install_1.sql](https://github.com/IBSurgeon/lucene_udr/blob/main/sql/fts%24install_1.sql) 

Скачать готовые сборки под ОС Windows можно по ссылкам:
* [LuceneUdr_Win_x64.zip](https://github.com/IBSurgeon/lucene_udr/releases/download/1.3/LuceneUdr_Win_x64.zip)
* [LuceneUdr_Win_x86.zip](https://github.com/IBSurgeon/lucene_udr/releases/download/1.3/LuceneUdr_Win_x86.zip)

Под ОС Linux вы можете скомпилировать библиотеку самостоятельно.

Скачать демонстрационную базу данных, для которой подготовлены примеры можно по следующим ссылкам:
* [fts_demo_3.0.zip](https://github.com/IBSurgeon/lucene_udr/releases/download/1.3/fts_demo_3.0.zip) - база данных для Firebird 3.0;
* [fts_demo_4.0.zip](https://github.com/IBSurgeon/lucene_udr/releases/download/1.3/fts_demo_4.0.zip) - база данных для Firebird 4.0.

Документация на английском и русском языках доступна по ссылкам:
* [lucene-udr.pdf](https://github.com/IBSurgeon/lucene_udr/releases/download/1.3/lucene-udr.pdf);
* [lucene-udr-rus.pdf](https://github.com/IBSurgeon/lucene_udr/releases/download/1.3/lucene-udr-rus.pdf).

## Сборка и установка библиотеки под Linux

Lucene UDR построена на основе [Lucene++](https://github.com/luceneplusplus/LucenePlusPlus). 
В некоторых дистрибутивах Linux вы можете установить `lucene++` и `lucene++-contrib` из 
их репозиториев. Если же библиотеки в репозиториях отсутствуют, то вам потребуется скачать и собрать их из исходников.

```
git clone https://github.com/luceneplusplus/LucenePlusPlus.git
cd LucenePlusPlus
mkdir build; cd build
cmake ..
make
sudo cmake --install . --prefix /usr
```

Более подробно сборка библиотеки lucene++ описана в [BUILDING.md](https://github.com/luceneplusplus/LucenePlusPlus/blob/master/doc/BUILDING.md).

Теперь можно приступать к сборке UDR Lucene.

```
git clone https://github.com/IBSurgeon/lucene_udr.git
cd lucene_udr
mkdir build
cmake -S . -B ./build
cmake --build ./build -- -j12
sudo cmake --install ./build
```

В процессе выполнения `cmake -S . -B ./build` может возникнуть следующая ошибка

```
CMake Error at /usr/lib64/cmake/liblucene++/liblucene++Config.cmake:41 (message):
  File or directory /usr/lib64/usr/include/lucene++/ referenced by variable
  liblucene++_INCLUDE_DIRS does not exist !
Call Stack (most recent call first):
  /usr/lib64/cmake/liblucene++/liblucene++Config.cmake:47 (set_and_check)
  CMakeLists.txt:78 (find_package)
```

Для её исправления необходимо исправить файлы `liblucene++Config.cmake` и `liblucene++-contribConfig.cmake`, где 
заменить строчку

```
get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../usr" ABSOLUTE)
```

на

```
get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)
```


## Настройка Lucene UDR

Перед использованием полнотекстового поиска в вашей базе данных необходимо произвести предварительную настройку.
Настройки Lucene UDR находятся в файле `$(root)\fts.ini`. Если этого файла нет, то создайте его самостоятельно.
Здесь `$(root)` - корневая директория установки Firebird.

В этом файле задаётся путь к директории, в которой будут создаваться полнотекстовые индексы для указанной базы данных.

В качестве имени секции ini файла должен быть задан полный путь к базе данных или алиас (в зависимости от значения 
параметра `DatabaseAccess` в `firebird.conf`). Путь к директории полнотекстовых индексов указывается в ключе `ftsDirectory`. 

```ini
[fts_demo]
ftsDirectory=f:\fbdata\3.0\fts\fts_demo
```

или

```ini
[f:\fbdata\3.0\fts_demo.fdb]
ftsDirectory=f:\fbdata\3.0\fts\fts_demo
```

В Linux имя секции зависит от регистра символов. Оно должно полностью совпадать со значением результата запроса:

```sql
select mon$attachment_name
from mon$attachments
where mon$attachment_id = current_connection;
```

Если ваше подключение может происходить как через алиас, так и с указанием пути к базе данных вы можете прописать в ini файл сразу обе секции.

```ini
[f:\fbdata\3.0\fts_demo.fdb]
ftsDirectory=f:\fbdata\3.0\fts\fts_demo

[fts_demo]
ftsDirectory=f:\fbdata\3.0\fts\fts_demo
```

Важно: пользователь или группа, под которым выполняется служба Firebird, должен иметь права на чтение и запись для 
директории с полнотекстовыми индексами.

Получить расположение директории для полнотекстовых индексов можно с помощью запроса:

```sql
SELECT FTS$MANAGEMENT.FTS$GET_DIRECTORY() AS DIR_NAME
FROM RDB$DATABASE
```

## Анализаторы

Анализ - это преобразование заданного текста в более мелкие и точные единицы для облегчения поиска.

Текст проходит через различные операции по извлечению ключевых слов, удалению общих слов и знаков препинания, преобразования слов в нижний регистр и так далее.

Список доступных анализаторов можно получить с помощью процедуры `FTS$MANAGEMENT.FTS$ALL_ANALYZERS`.

По умолчанию доступны следующие анализаторы:

* ARABIC - ArabicAnalyzer (Арабский язык);
* BRAZILIAN - BrazilianAnalyzer (Бразильский язык);
* CHINESE - ChineseAnalyzer (Китайский язык);
* CJK - CJKAnalyzer (Китайское письмо);
* CZECH - CzechAnalyzer (Чешский язык);
* DUTCH - DutchAnalyzer (Голландский язык);
* ENGLISH - EnglishAnalyzer (Английский язык);
* FRENCH - FrenchAnalyzer (Французский язык);
* GERMAN - GermanAnalyzer (Немецкий язык);
* GREEK - GreekAnalyzer (Греческий язык);
* KEYWORD - KeywordAnalyzer;
* PERSIAN - PersianAnalyzer (Персидский язык);
* RUSSIAN - RussianAnalyzer (Русский язык);
* STANDARD - StandardAnalyzer (Английский язык);
* SIMPLE - SimpleAnalyzer;
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

### Типы анализаторов

Рассмотрим наиболее часто используемые анализаторы.

#### STANDARD - StandardAnalyzer

Стандартный анализатор разделяет текст на слова, числа, URL адреса и EMAIL. Он преобразует текст в нижний регистр, после чего к 
полученным термам применяется фильтр стоп слов для английского языка.

#### STOP - StopAnalyzer

StopAnalyzer разделяет текст на небуквенные символы. Он преобразует текст в нижний регистр, после чего к 
полученным термам применяется фильтр стоп слов для английского языка.

В отличие от StandardAnalyzer, StopAnalyzer не способен распознавать URL-адреса и e-mail.

#### SIMPLE - SimpleAnalyzer

SimpleAnalyzer разделяет текст на небуквенные символы. Он преобразует текст в нижний регистр. 
SimpleAnalyzer не применяет фильтр стоп слов, кроме того он не способен распознавать URL-адреса и e-mail.

#### WHITESPACE - WhitespaceAnalyzer

WhitespaceAnalyzer - разбивает текст по пробельным символам.

#### KEYWORD - KeywordAnalyzer

KeywordAnalyzer - представляет текст как один единый терм. 
KeywordAnalyzer полезен для таких полей, как идентификаторы и почтовые индексы.

#### Языковые анализаторы

Существуют также специальные анализаторы для разных языков, такие как EnglishAnalyzer, FrenchAnalyzer, RussianAnalyzer и другие.

Такие анализаторы разделяют текст на слова, числа, URL адреса и EMAIL. Они преобразуют текст в нижний регистр, после чего к 
полученным термам применяется фильтр стоп слов для заданного языка. 

После фильтрации применяется алгоритм стемминга. Стемминг (англ. stemming) — это процесс нахождения основы слова для заданного исходного слова. 
Основа слова необязательно совпадает с морфологическим корнем слова. Стемминг необходим для того чтобы поиск мог осуществляться не только по самому слову, но и его формам.

#### Snowball анализаторы

Анализаторы в которых используются алгоритмы стемминга из проекта "Snowball".

### Создание собственных анализаторов

Библиотека IBSurgeon FTS UDR позволяет создавать свои собственные анализаторы. Через язык SQL невозможно изменить алгоритмы разделения текста на термы и алгоритмы стемминга, 
однако вы можете задать список своих стоп слов.

Создать свой анализатор можно на основе одного из встроенных анализаторов. Для того чтобы было возможно создать собственный анализатор на основе базового, 
базовый анализатор должен поддерживать фильтр стоп слов. Новый анализатор создаётся с пустым списком стоп слов.

Для создания собственного анализатора вызовете процедуру `FTS$MANAGEMENT.FTS$CREATE_ANALYZER`. Первым параметром указывается имя нового анализатора, вторым - имя базового анализатора,
третьим, необязательным параметром, можно задать описание анализатора.

После создания анализатора добавить необходимые стоп слова можно с помощью процедуры `FTS$MANAGEMENT.FTS$ADD_STOP_WORD`.

Пример создания собственного анализатора:

```sql
execute procedure FTS$MANAGEMENT.FTS$CREATE_ANALYZER('FARMNAME_EN', 'ENGLISH');

commit;

execute procedure FTS$MANAGEMENT.FTS$ADD_STOP_WORD('FARMNAME_EN', 'farm');
execute procedure FTS$MANAGEMENT.FTS$ADD_STOP_WORD('FARMNAME_EN', 'owner');

commit;
```

Замечание:

Если вы добавляете или удаляете стоп слова из анализатора, на основе которого уже есть построенные индексы, то эти индексы меняют статус на 'U' - Updated metadata, 
то есть требуют перестроения.

## Создание полнотекстовых индексов

Для создания полнотекстового индекса необходимо выполнить последовательно три шага:
1. Создание полнотекстового индекса для таблицы с помощью процедуры `FTS$MANAGEMENT.FTS$CREATE_INDEX`;
2. Добавление индексируемых полей с помощью процедуры `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD`;
3. Построение индекса с помощью процедуры `FTS$MANAGEMENT.FTS$REBUILD_INDEX`.


### Создание полнотекстового индекса для таблицы

Для создания полнотекстового индекса для таблицы необходимо вызвать процедуру `FTS$MANAGEMENT.FTS$CREATE_INDEX`.

Первым параметром задаёт имя полнотекстового индекса, вторым - имя индексируемой таблицы. Остальные параметры являются 
необязательными.

Третьим параметром задаётся имя анализатора. Анализатор задаёт для какого языка будет сделан анализ индексируемых полей.
Если параметр не задан, то будет использован анализатор STANDARD (для английского языка). 

Четвёртым параметром задаётся имя поля таблицы, которое будет возвращено в качестве результата поиска. Обычно это
поле первичного или уникального ключа. Также поддерживается задание специального псевдо поля `RDB$DB_KEY`.
Может быть возвращено значение только одного поля одного из типов:

* `SMALLINT`, `INTEGER`, `BIGINT` - поля этих типов часто используются в качестве искусственного первичного 
ключа на основе генераторов (последовательностей);

* `CHAR(16) CHARACTER SET OCTETS` или `BINARY(16)` - поля этих типов используются в качестве искусственного первичного
ключа на основе GUID, то есть сгенерированных с помощью `GEN_UUID()`;

* поле `RDB$DB_KEY` типа `CHAR(8) CHARACTER SET OCTETS`.

Если этот параметр не задан (значение NULL), то для постоянных таблиц и GTT будет произведена попытка найти поле в первичном ключе.
Эта попытка будет удачной, если ключ не является составным и поле, на котором он построен имеет один из типов данных описанных выше.
Если первичного ключа не существует, то будет использовано псевдо поле `RDB$DB_KEY`.

Пятым параметром можно задать описание поля.

Для примеров используется таблица со следующей структурой:

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

Пример ниже создаёт индекс `IDX_PRODUCT_NAME` для таблицы `PRODUCTS` с использованием анализатора `STANDARD`. 
Возвращается поле `PRODUCT_ID`. Его имя было автоматически извлечено из первичного ключа таблицы `PRODUCTS`.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_NAME', 'PRODUCTS');

COMMIT;
```

Следующий пример создаст индекс `IDX_PRODUCT_NAME_EN` с использованием анализатора `ENGLISH`.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_NAME_EN', 'PRODUCTS', 'ENGLISH');

COMMIT;
```

Можно указать конкретное имя поля которое будет возвращено в качестве результата поиска.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_ID_2_EN', 'PRODUCTS', 'ENGLISH', 'PRODUCT_ID');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_DBKEY_EN', 'PRODUCTS', 'ENGLISH', 'RDB$DB_KEY');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_UUID_EN', 'PRODUCTS', 'ENGLISH', 'PRODUCT_UUID');

COMMIT;
```

### Добавление полей для индексирования

После создания индекса, необходимо добавить поля по которым будет производиться поиск с помощью
процедуры `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD`. Первым параметром указывается имя индекса, вторым имя добавляемого поля.
Третьим необязательным параметром можно указать множитель значимости для поля. По умолчанию значимость всех полей индекса одинакова и равна 1.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_NAME_EN', 'PRODUCT_NAME');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_DBKEY_EN', 'PRODUCT_NAME');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_UUID_EN', 'PRODUCT_NAME');


EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_ID_2_EN', 'PRODUCT_NAME');
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_ID_2_EN', 'ABOUT_PRODUCT');

COMMIT;
```

В индексах `IDX_PRODUCT_NAME_EN`, `IDX_PRODUCT_DBKEY_EN` и `IDX_PRODUCT_UUID_EN` обрабатывается одно поле `PRODUCT_NAME`, 
а в индексе `IDX_PRODUCT_ID_2_EN` - два поля `PRODUCT_NAME` и `ABOUT_PRODUCT`.

В следующем примере показано создание индекса с двумя полями `PRODUCT_NAME` и `ABOUT_PRODUCT`. Значимость поля `PRODUCT_NAME` в 4 раз выше значимости поля `ABOUT_PRODUCT`.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_ID_2X_EN', 'PRODUCTS', 'ENGLISH', 'PRODUCT_ID');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_ID_2X_EN', 'PRODUCT_NAME', 4);
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_ID_2X_EN', 'ABOUT_PRODUCT');

COMMIT;
```

### Построение индекса

Для построения индекса используется процедура `FTS$MANAGEMENT.FTS$REBUILD_INDEX`. В качестве 
входного параметра необходимо указать имя полнотекстового индекса.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_NAME_EN');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_DBKEY_EN');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_UUID_EN');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_ID_2_EN');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_ID_2X_EN');

COMMIT;
```

На этапе построения для индекса создаётся соответсвующая одноимённая папка в директории для полнотекстовых индексов.
В этих папках располагаются файлы индекса Lucene. Эта часть процесса происходит вне контроля транзакций, поэтому ROLLBACK не удалит файлы индекса.

Кроме того, в случае успешного построения у индекса меняется статус на 'C' (Complete). Изменение статуса происходят в текущей транзакции.

## Статусы индекса

Описание индексов хранится в служебной таблице `FTS$INDICES`.

Поле `FTS$INDEX_STATUS` хранит статус индекса. Индекс может иметь 4 статуса:

* *N* - New index. Новый индекс. Устанавливается при создании индекса, в котором ещё нет ни одного сегмента.
* *U* - Updated metadata. Устанавливается каждый раз, когда изменяются метаданные индекса, например при добавлении 
или удалении сегмента индекса. Если индекс имеет такой статус, то он требует перестроения, чтобы поиск по нему 
работал корректно.
* *I* - Inactive. Неактивный индекс. Неактивные индексы не обновляются процедурой `FTS$UPDATE_INDEXES`.
* *C* - Complete. Активный индекс. Такие индексы обновляются процедурой `FTS$UPDATE_INDEXES`. 
Индекс переходит в это состояние только после полного построения или перестроения.

## Поиск с использованием полнотекстовых индексов

Для поиска по полнотекстовому индексу используется процедура `FTS$SEARCH`.

Первым параметром задаётся имя индекса, с помощью которого будет осуществлён поиск, а вторым - поисковая фраза.
Третий необязательный параметр задаёт ограничение на количество возвращаемых записей, по умолчанию 1000.
Четвёртый параметр позволяет включить режим объяснения результатов поиска, по умолчанию FALSE.

Пример поиска:

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

Выходные параметры:

- FTS$RELATION_NAME - имя таблицы в которой найден документ;
- FTS$KEY_FIELD_NAME - имя ключевого поля в таблице;
- FTS$DB_KEY - значение ключевого поля в формате `RDB$DB_KEY`;
- FTS$ID - значение ключевого поля типа `BIGINT` или `INTEGER`;
- FTS$UUID - значение ключевого поля типа `BINARY(16)`. Такой тип используется для хранения GUID;
- FTS$SCORE - степень соответствия поисковому запросу;
- FTS$EXPLANATION - объяснение результатов поиска.

Результат запроса будет доступен в одном из полей `FTS$DB_KEY`, `FTS$ID`, `FTS$UUID` в зависимости от того какое результирующие поле было указано при создании индекса.

Для извлечения данных из целевой таблицы достаточно просто выполнить с ней соединение условие которого зависит от того как создавался индекс.

Вот примеры различных вариантов соединения:

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_ID,
  P.PRODUCT_NAME
FROM 
  FTS$SEARCH('IDX_PRODUCT_NAME_EN', 'Transformers Bumblebee') FTS
  JOIN PRODUCTS P ON P.PRODUCT_ID = FTS.FTS$ID;

SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_UUID,
  P.PRODUCT_NAME
FROM 
  FTS$SEARCH('IDX_PRODUCT_UUID_EN', 'Transformers Bumblebee') FTS
  JOIN PRODUCTS P ON P.PRODUCT_UUID = FTS.FTS$UUID;

SELECT
  FTS.FTS$SCORE,
  P.RDB$DB_KEY,
  P.PRODUCT_ID,
  P.PRODUCT_NAME
FROM 
  FTS$SEARCH('IDX_PRODUCT_DBKEY_EN', 'Transformers Bumblebee') FTS
  JOIN PRODUCTS P ON P.RDB$DB_KEY = FTS.FTS$DB_KEY;
```

Для поиска сразу по двум полям используем индекс `IDX_PRODUCT_ID_2_EN`, в котором при создании были заданы поля `PRODUCT_NAME` и `ABOUT_PRODUCT`.

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_ID,
  P.PRODUCT_NAME,
  P.ABOUT_PRODUCT
FROM 
  FTS$SEARCH('IDX_PRODUCT_ID_2_EN', 'Transformers Bumblebee') FTS
  JOIN PRODUCTS P ON P.PRODUCT_ID = FTS.FTS$ID;
```

Для объяснения результатов поиска, установите последний параметр в TRUE.

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_ID,
  P.PRODUCT_NAME,
  P.ABOUT_PRODUCT,
  FTS.FTS$EXPLANATION
FROM 
  FTS$SEARCH('IDX_PRODUCT_ID_2_EN', 'Transformers Bumblebee', 5, TRUE) FTS
  JOIN PRODUCTS P ON P.PRODUCT_ID = FTS.FTS$ID;
```

Поле `FTS$EXPLANATION` будет содержать объяснение результата.

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

Для сравнения показано объяснение результатов поиска по индексу с полями у которых указан разный коэффициент значимости.

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_ID,
  P.PRODUCT_NAME,
  P.ABOUT_PRODUCT,
  FTS.FTS$EXPLANATION
FROM 
  FTS$SEARCH('IDX_PRODUCT_ID_2X_EN', 'Transformers Bumblebee', 5, TRUE) FTS
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

## Синтаксис поисковых запросов

### Термы

Поисковые запросы (фразы поиска) состоят из термов и операторов. Lucene поддерживает простые и сложные термы. 
Простые термы состоят из одного слова, сложные из нескольких. Первые из них, это обычные слова, 
например, "Hello", "world". Второй же тип термов это группа слов, например, "Hello world". 
Несколько термов можно связывать вместе при помощи логических операторов.

### Поля

Lucene поддерживает поиск по нескольким полям. По умолчанию поиск осуществляется во всех полях полнотекстового индекса, 
выражение по каждому полю повторяется и соединяется оператором `OR`. Например, если у вас индекс содержащий 
поля `PRODUCT_NAME` и `ABOUT_PRODUCT`, то запрос

```
Transformers Bumblebee
```

будет эквивалентен запросу

```
(PRODUCT_NAME: Transformers Bumblebee) OR (ABOUT_PRODUCT: Transformers Bumblebee)
```

Вы можете указать по какому полю вы хотите произвести поиск, для этого в запросе необходимо указать имя поля, символ двоеточия ":", 
после чего поисковую фразу для этого поля.

Пример поиска слова "Polyester" в поле `ABOUT_PRODUCT` и слов "Transformers Bumblebee" в поле `PRODUCT_NAME`:

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_ID,
  P.PRODUCT_NAME,
  P.ABOUT_PRODUCT,
  FTS.FTS$EXPLANATION
FROM 
  FTS$SEARCH('IDX_PRODUCT_ID_2_EN', '(PRODUCT_NAME: Transformers Bumblebee) AND (ABOUT_PRODUCT: Polyester)', 5, TRUE) FTS
  JOIN PRODUCTS P ON P.PRODUCT_ID = FTS.FTS$ID;
```

Замечание: Lucene, как и Firebird, поддерживает поля с разделителями. Настоятельно не рекомендуется использовать пробелы и другие специальные символы в именах полей,
поскольку это значительно затруднит написание поисковых запросов. Если же ваше поле содержит пробел или другой специальный символ, его необходимо экранировать с помощью
символа "\\".

Например, если у вас индекс по двум полям "Product Name" и "Product Specification" и вы хотите найти в спецификации слово "Weight", то запрос должен выглядеть следующим образом:

```
Product\ Specification: Weight
```

### Поиск с подстановочными знаками

Lucene поддерживает поиск с односимвольными и многосимвольными подстановочными знаками в рамках отдельных термов 
(но не во фразовых запросах).

Символ "?" заменяет один любой символ, а "\*" - любое количество символов.

Например, для поиска "text" или "test" вы можете использовать запрос:

```
te?t
```

Для поиска "test", "tests" или "tester" можно использовать запрос:

```
test*
```

Подстановочный знак можно использовать внутри терма:

```
te*t
```

Замечание: Поисковый запрос нельзя начинать с символов "?" или "\*".

### Нечёткий поиск

Lucene поддерживает нечеткий поиск на основе алгоритма расстояния Левенштейна (дистанция редактирования).

Чтобы выполнить нечеткий поиск, используйте тильду "~" в конце терма с одним словом. Например, для поиска терма
похожее по написанию на "roam", используйте запрос нечеткого поиска:

```
roam~
```

В результате этого запроса будут также найдены слова "foam" и "roams".

Дополнительный (необязательный) параметр может указать необходимое сходство.
Значение находится в диапазоне от 0 до 1. 
Чем значение ближе к 1, с более высоким сходством будут сопоставляться только термы. Например:

```
roam~0.8
```

Значение по умолчанию, которое используется, если параметр не указан, равно 0.5.


### Поиск близости

Lucene поддерживает поиск слов, находящихся на определенном расстоянии. 
Для поиска близости используйте тильду "~" в конце фразы. 
Например, чтобы найти "apache" и "jakarta" в пределах 10 слов друг от друга в документе, используйте поиск:

```
"jakarta apache"~10
```

### Поиск по диапазону

Запросы диапазона позволяют сопоставлять документы, значения полей которых находятся между нижней 
и верхней границей, указанной в запросе диапазона. Запросы диапазона могут включать или исключать верхнюю 
и нижнюю границы. Сортировка производится лексикографически.

```
BYDATE:[20020101 TO 20030101]
```

Этот запрос найдет документы, поля BYDATE которых имеют значения от 20020101 до 20030101 включительно. 
Обратите внимание, что запросы диапазона не зарезервированы для полей типа даты.

Вы также можете использовать запросы диапазона с полями без дат:

```
TITLE:{Aida TO Carmen}
```

Этот запрос позволит найти все документы, заголовки которых находятся между "Aida" и "Carmen", 
но не включая "Aida" и "Carmen".

Запросы включающего диапазона обозначаются квадратными скобками. 
Запросы исключающего диапазона обозначаются фигурными скобками.

### Усиление термов

Lucene рассчитывает уровень релевантности сопоставления документов на основе найденных терминов.
Чтобы усилить терм, используйте символ вставки "^" с коэффициентом усиления (число) в конце искомого термина. 
Чем выше коэффициент усиления, тем более релевантным будет терм.


Усиление позволяет контролировать релевантность документа, повышая релевантность его терма. Например, если вы ищете

```
jakarta apache
```

и хотите, чтобы терм "jakarta" был более значимым, увеличьте его значимость, используя символ "^" вместе с коэффициентом 
усиления рядом с термом.
Вы должны ввести:

```
jakarta^4 apache
```

Это сделает документы с термом "jakarta" более актуальными. Вы также можете повысить фразовые термы, как в примере:

```
"jakarta apache"^4 "Apache Lucene"
```

По умолчанию коэффициент значимости равен 1. 
Хотя коэффициент усиления значимости должен быть положительным, он может быть меньше 1 (например, 0.2).

### Логические операторы

Логические операторы позволяют использовать логические конструкции при задании условий
поиска, и позволяют комбинировать несколько термов. 
Lucene поддерживает следующие логические операторы: `AND`, `+`, `OR`, `NOT`, `-`.

Логические операторы должны указываться заглавными буквами.

#### Оператор OR

`OR` является логическим оператором по умолчанию, это означает, что если между двумя термами
фразы поиска не указан другой логический оператор, то подставляется оператор `OR`. При этом система поиска находит 
документ, если одна из указанных во фразе поиска терм в нем присутствует.
Альтернативным обозначением оператора `OR` является `||`.

```
"Hello world" "world"
```

Эквивалентно:

```
"Hello world" OR "world"
```

#### Оператор AND

Оператор `AND` указывает на то, что в тексте должны присутствовать все, объединенные оператором термы поиска. 
Альтернативным обозначением оператора является `&&`.

```
"Hello" AND "world"
```

#### Оператор +

Оператор `+` указывает на то, что следующее за ним слово должно обязательно присутствовать в тексте. 
Например, для поиска записей, которые должны содержать слово "hello" и могут
содержать слово "world", фраза поиска может иметь вид:

```
+Hello world
```

#### Оператор NOT

Оператор `NOT` позволяет исключить из результатов поиска те, в которых встречается терм,
следующий за оператором. Вместо слова `NOT` может использоваться символ "!". Например, для
поиска записей, которые должны содержать слово "hello", и не должны содержать слово "world",
фраза поиска может иметь вид:

```
"Hello" NOT "world"
```

Замечание: Оператор `NOT` не может использоваться только с одним термом. Например, поиск с таким
условием не вернет результатов:

```
NOT "world"
```

#### Оператор –

Этот оператор является аналогичным оператору `NOT`. Пример использования:

```
"Hello" -"world"
```

#### Группировка

Lucene поддерживает использование скобок для группировки предложений для формирования подзапросов. 
Это может быть очень полезно, если вы хотите контролировать логику запроса.

Для поиска "jakarta" или "apache" и "website" используйте запрос:

```
(jakarta OR apache) AND website
```

Это устраняет любую путаницу и гарантирует, что "website" должен существовать, и должен существовать один из термов "jakarta" или "apache".

### Группировка полей

Lucene поддерживает использование круглых скобок для группировки нескольких предложений в одном поле.

Для поиска заголовка, содержащего как слово "return", так и фразу "pink panther", используйте запрос:

```
TITLE:(+return +"pink panther")
```

### Экранирование специальных символов

Lucene поддерживает экранирование специальных символов, которые являются частью синтаксиса запроса.
Текущий список специальных символов:

```
+ - && || ! ( ) { } [ ] ^ " ~ * ? : \
```

Для экранирования этих символов используйте символ "\\" перед специальным символом

Например, фраза поиска для выражения "(1 + 1) : 2" будет иметь вид:

```
\( 1 \+ 1 \) \: 2
```

Для экранирования специальных символов вы можете воспользоваться функцией `FTS$ESCAPE_QUERY`.

```sql
  FTS$ESCAPE_QUERY('(1 + 1) : 2')
```

Более подробное англоязычное описание синтаксиса расположено на официальном сайте
Lucene: [https://lucene.apache.org/core/3_0_3/queryparsersyntax.html](https://lucene.apache.org/core/3_0_3/queryparsersyntax.html).

## Индексация представлений

Вы можете индексировать не только постоянные таблицы, но и сложные представления.

Для того чтобы индексировать представление должно быть соблюдено одно требование: 
в представлении должно быть поле, по которому вы можете однозначно идентифицировать запись.

Допустим у вас есть представление `V_PRODUCT_CATEGORIES`, где `PRODUCT_UUID` - уникальный идентификатор таблицы `PRODUCTS`:

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
FROM 
  PRODUCT_CATEGORIES PC
  JOIN CATEGORIES C
    ON C.ID = PC.CATEGORY_ID
GROUP BY 1
;
```

Вы хотите производить поиск товаров категории, но наименование категории находится в справочной таблицы и у одного товара может быть несколько категорий.
В этом случае можно создать следующий полнотекстовый индекс:

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_PRODUCT_CATEGORIES', 'V_PRODUCT_CATEGORIES', 'ENGLISH', 'PRODUCT_UUID');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_PRODUCT_CATEGORIES', 'CATEGORIES');

COMMIT;

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_PRODUCT_CATEGORIES');

COMMIT;
```

Поиск товара по его категории выглядит так:

```sql
SELECT
  FTS.FTS$SCORE,
  P.PRODUCT_UUID,
  P.PRODUCT_NAME,
  PC.CATEGORIES,
  FTS.FTS$EXPLANATION
FROM 
  FTS$SEARCH('IDX_PRODUCT_CATEGORIES', '"Toys & Games"') FTS
  JOIN V_PRODUCT_CATEGORIES PC ON PC.PRODUCT_UUID = FTS.FTS$UUID
  JOIN PRODUCTS P ON P.PRODUCT_UUID = PC.PRODUCT_UUID;
```

## Анализ текста

Результат поиска зависит от того какой анализатор был использован при создании индекса.
Анализатор выполняет следующие функции: разбивает текст на отдельные слова, приводит слова к нижнему регистру, 
удаляет стоп слов, применяет другие фильтры, применяет алгоритм стемминга. 
В результате анализа текста для индексации из текста будут выделены термы, которые и попадают в индекс.

Для того чтобы узнать, какие именно термы попадают в индекс, вы можете воспользоваться хранимой процедурой `FTS$ANALYZE`.

```sql
PROCEDURE FTS$ANALYZE (
    FTS$TEXT     BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
    FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD')
RETURNS (
    FTS$TERM VARCHAR(8191) CHARACTER SET UTF8
)
```

Первым параметром задаётся текст для анализа, а вторым имя анализатора.

Пример использования:

```sql
SELECT FTS$TERM
FROM FTS$ANALYZE('IBSurgeon FTS UDR library implements full text search procedures and functions for Firebird SQL', 'STANDARD')
```

## Выделение найденных термов во фрагменте текста

Часто необходимо не просто найти документы по запросу, но и выделить, то что было найдено.

Для выделения найденных термов во фрагменте текста используется пакет `FTS$HIGHLIGHTER`. В пакете присутствуют:

- функция `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT` для выделения найденной термов во фрагменте текста;
- процедура `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS` возвращающая несколько фрагментов текста с выделением термов во фрагменте.

### Выделение найденных термов с помощью функции FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT

Функция `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT` возвращает лучший фрагмент текста в котором найденные термы выделены тегами.

Функция описана как 

```sql
  FUNCTION FTS$BEST_FRAGMENT (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>'
  )
  RETURNS VARCHAR(8191) CHARACTER SET UTF8;
```

В параметре `FTS$TEXT` указывается текст в котором производится поиск и выделение фрагментов.

В параметре `FTS$QUERY` указывается поисковая фраза.

В третьем необязательном параметре `FTS$ANALYZER` указывается имя анализатора с помощью которого происходит выделение термов.

В параметре `FTS$FIELD_NAME` указывается имя поля по которому производится поиск. Его необходимо указывать необходимо если поисковый запрос явно содержит несколько полей,
в противном случае параметр можно не указывать или установить в качестве значения NULL.

В параметре `FTS$FRAGMENT_SIZE` указывается ограничение на длину возвращаемого фрагмента. 
Обратите внимание, реальная длина возвращаемого текста может быть больше. Возвращаемый фрагмент, обычно не разрывает слова, 
кроме того в нём не учитывается длина самих тегов для выделения.

В параметре `FTS$LEFT_TAG` указывается тег, который добавляется к найденному терму слева.

В параметре `FTS$RIGHT_TAG` указывается тег, который добавляется к найденному фрагменту справа.

Простейший пример использования:

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

Теперь объединим сам поиск и выделение найденных термов:

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

### Выделение найденных термов с помощью процедуры FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS

Процедура `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS` возвращает несколько фрагментов текста в котором найденные термы выделены тегами.

Процедура описана как 

```sql
  PROCEDURE FTS$BEST_FRAGMENTS (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
      FTS$MAX_NUM_FRAGMENTS INTEGER NOT NULL DEFAULT 10,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>'
  )
  RETURNS (
      FTS$FRAGMENT VARCHAR(8191) CHARACTER SET UTF8
  );
```

Входные параметры процедуры `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS` идентичны параметрам функции `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT`, но есть
один дополнительный параметр `FTS$MAX_NUM_FRAGMENTS`, который ограничивает количество возвращаемых фрагментов. 

Текст найденных фрагментов с выделенными вхождениями термов возвращается в выходном параметре `FTS$FRAGMENT`. Эту процедуру следует применять в уже найденном
одном документе.

Пример использования:

```sql
SELECT
    BOOKS.TITLE
  , BOOKS.CONTENT
  , F.FTS$FRAGMENT
FROM 
  BOOKS
  LEFT JOIN FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS(
    BOOKS.CONTENT,
    'friendly',
    'ENGLISH'
  ) F ON TRUE
WHERE BOOKS.ID = 8
```


## Поддержание актуальности данных в полнотекстовых индексах

Для поддержки актуальности полнотекстовых индексов существует несколько способов:

1. Периодически вызывать процедуру `FTS$MANAGEMENT.FTS$REBUILD_INDEX` для заданного индекса. 
Этот способ полностью перестраивает полнотекстовый индекс. В этом случае читаются все записи таблицы или представления 
для которой создан индекс.

2. Поддерживать полнотекстовые индексы можно с помощью триггеров. При срабатывании этих триггеров запись об изменении 
добавляется в специальную таблицу `FTS$LOG` (журнал изменений).
Изменения из журнала переносятся в полнотекстовые индексы с помощью вызова процедуры `FTS$UPDATE_INDEXES`.
Вызов этой процедуры необходимо делать в отдельном скрипте, который можно поставить в планировщике заданий (Windows) 
или cron (Linux) с некоторой периодичностью, например 5 минут.

3. Отложенное обновление полнотекстовых индексов, с помощью технологии FirebirdStreaming. В этом случае специальная 
служба читает логи репликации и извлекает из них информацию необходимую для обновления полнотекстовых индексов 
(в процессе разработки).


### Триггеры для поддержки актуальности полнотекстовых индексов

Для поддержки актуальности полнотекстовых индексов необходимо создать триггеры, которые при изменении
любого из полей, входящих в полнотекстовый индекс, записывает информацию об изменении записи в специальную таблицу 
`FTS$LOG` (журнал).

Правила написания триггеров для поддержки полнотекстовых индексов:

1. В триггере необходимо проверять всем поля, которые участвуют в полнотекстовом индексе.
Условия проверки полей должны быть объединены через `OR`.

2. Для операции `INSERT` необходимо проверять все поля, входящие в полнотекстовые индексы значение которых отличается 
от `NULL`. Если это условие соблюдается, то необходимо выполнить один из запросов в зависимости от типа ключевого поля

```sql
-- для ключей типа INTEGER или BIGINT
INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_ID, FTS$CHANGE_TYPE) VALUES('<table_name>', NEW.<key_field_name>, 'I');
-- для ключей типа RDB$DB_KEY
INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$DB_KEY, FTS$CHANGE_TYPE) VALUES('<table_name>', NEW.<key_field_name>, 'I');
-- для ключей типа UUID
INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('<table_name>', NEW.<key_field_name>, 'I');
```

3. Для операции `UPDATE` необходимо проверять все поля, входящие в полнотекстовые индексы значение которых изменилось.
Если это условие соблюдается, то необходимо выполнить один из запросов в зависимости от типа ключевого поля

```sql
-- для ключей типа INTEGER или BIGINT
INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_ID, FTS$CHANGE_TYPE) VALUES('<table_name>', OLD.<key_field_name>, 'U');
-- для ключей типа RDB$DB_KEY
INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$DB_KEY, FTS$CHANGE_TYPE) VALUES('<table_name>', OLD.<key_field_name>, 'U');
-- для ключей типа UUID
INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('<table_name>', OLD.<key_field_name>, 'U');
```

4. Для операции `DELETE` необходимо проверять все поля, входящие в полнотекстовые индексы значение которых отличается 
от `NULL`. Если это условие соблюдается, то необходимо выполнить один из запросов в зависимости от типа ключевого поля

```sql
-- для ключей типа INTEGER или BIGINT
INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_ID, FTS$CHANGE_TYPE) VALUES('<table_name>', OLD.<key_field_name>, 'D');
-- для ключей типа RDB$DB_KEY
INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$DB_KEY, FTS$CHANGE_TYPE) VALUES('<table_name>', OLD.<key_field_name>, 'D');
-- для ключей типа UUID
INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('<table_name>', OLD.<key_field_name>, 'D');
```

Для облегчения задачи написания таких триггеров существует специальный пакет `FTS$TRIGGER_HELPER`, в котором 
расположены процедуры генерирования исходных текстов триггеров. Так например, для того чтобы сгенерировать триггеры 
для поддержки полнотекстовых индексов созданных для таблицы `PRODUCTS`, необходимо выполнить следующий запрос:

```sql
SELECT
    FTS$TRIGGER_SCRIPT
FROM FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS('PRODUCTS', TRUE)
```

Этот запрос вернёт следующий текст триггера для всех созданных FTS индексов на таблице `PRODUCTS`:

```sql
CREATE OR ALTER TRIGGER "FTS$PRODUCTS_AIUD" FOR "PRODUCTS"
ACTIVE AFTER INSERT OR UPDATE OR DELETE
POSITION 100
AS
BEGIN
  /* Block for key PRODUCT_ID */
  IF (INSERTING AND (NEW."ABOUT_PRODUCT" IS NOT NULL
      OR NEW."PRODUCT_NAME" IS NOT NULL)) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_ID, FTS$CHANGE_TYPE) VALUES('PRODUCTS', NEW."PRODUCT_ID", 'I');
  IF (UPDATING AND (NEW."ABOUT_PRODUCT" IS DISTINCT FROM OLD."ABOUT_PRODUCT"
      OR NEW."PRODUCT_NAME" IS DISTINCT FROM OLD."PRODUCT_NAME")) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_ID, FTS$CHANGE_TYPE) VALUES('PRODUCTS', OLD."PRODUCT_ID", 'U');
  IF (DELETING AND (OLD."ABOUT_PRODUCT" IS NOT NULL
      OR OLD."PRODUCT_NAME" IS NOT NULL)) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_ID, FTS$CHANGE_TYPE) VALUES('PRODUCTS', OLD."PRODUCT_ID", 'D');
  /* Block for key PRODUCT_UUID */
  IF (INSERTING AND (NEW."PRODUCT_NAME" IS NOT NULL)) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('PRODUCTS', NEW."PRODUCT_UUID", 'I');
  IF (UPDATING AND (NEW."PRODUCT_NAME" IS DISTINCT FROM OLD."PRODUCT_NAME")) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('PRODUCTS', OLD."PRODUCT_UUID", 'U');
  IF (DELETING AND (OLD."PRODUCT_NAME" IS NOT NULL)) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('PRODUCTS', OLD."PRODUCT_UUID", 'D');
  /* Block for key RDB$DB_KEY */
  IF (INSERTING AND (NEW."PRODUCT_NAME" IS NOT NULL)) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$DB_KEY, FTS$CHANGE_TYPE) VALUES('PRODUCTS', NEW.RDB$DB_KEY, 'I');
  IF (UPDATING AND (NEW."PRODUCT_NAME" IS DISTINCT FROM OLD."PRODUCT_NAME")) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$DB_KEY, FTS$CHANGE_TYPE) VALUES('PRODUCTS', OLD.RDB$DB_KEY, 'U');
  IF (DELETING AND (OLD."PRODUCT_NAME" IS NOT NULL)) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$DB_KEY, FTS$CHANGE_TYPE) VALUES('PRODUCTS', OLD.RDB$DB_KEY, 'D');
END
```

Обновление всех полнотекстовых индексов необходимо создать SQL скрипт `fts$update.sql`

```sql
EXECUTE PROCEDURE FTS$UPDATE_INDEXES;
```

Затем скрипт для вызова SQL скрипта через ISQL, примерно следующего содержания

```bash
isql -user SYSDBA -pas masterkey -i fts$update.sql inet://localhost/mydatabase
```

Обратите внимание! Пакет `FTS$TRIGGER_HELPER` помогает генерировать триггеры поддержки полнотекстовых индексов
только для обычных таблиц. Если вы хотите поддерживать полнотекстовый индекс на представлении, то необходимо 
самостоятельно разработать такие триггеры для базовых таблиц представления. 
Ниже приведён пример, поддерживающих полнотекстовый индекс триггеров для представления
`V_PRODUCT_CATEGORIES`.

```sql
SET TERM ^;

-- Field PRODUCT_UUID and CATEGORY_ID from table PRODUCT_CATEGORIES
CREATE OR ALTER TRIGGER FTS$PRODUCT_CATEGORIES_AIUD FOR PRODUCT_CATEGORIES
ACTIVE AFTER INSERT OR UPDATE OR DELETE
POSITION 100
AS
BEGIN
  IF (INSERTING) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('V_PRODUCT_CATEGORIES', NEW.PRODUCT_UUID, 'I');

  IF (UPDATING AND (NEW.PRODUCT_UUID <> OLD.PRODUCT_UUID
      OR NEW.CATEGORY_ID <> OLD.CATEGORY_ID)) THEN
  BEGIN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('V_PRODUCT_CATEGORIES', OLD.PRODUCT_UUID, 'D');
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('V_PRODUCT_CATEGORIES', NEW.PRODUCT_UUID, 'I');
  END

  IF (DELETING) THEN
    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('V_PRODUCT_CATEGORIES', OLD.PRODUCT_UUID, 'D');
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

    INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) VALUES('V_PRODUCT_CATEGORIES', :PRODUCT_UUID, 'U');
  END
END
END
^

SET TERM ;^
```

## Описание процедур и функций для работы с полнотекстовым поиском

### Пакет FTS$MANAGEMENT

Пакет `FTS$MANAGEMENT` содержит процедуры и функции для управления полнотекстовыми индексами. Этот пакет предназначен
для администраторов базы данных.


#### Функция FTS$MANAGEMENT.FTS$GET_DIRECTORY

Функция `FTS$MANAGEMENT.FTS$GET_DIRECTORY` возвращает директорию в которой расположены файлы и папки полнотекстовых индексов для текущей базы данных.

```sql
  FUNCTION FTS$GET_DIRECTORY ()
  RETURNS VARCHAR(255) CHARACTER SET UTF8
  DETERMINISTIC;
```

#### Процедура FTS$MANAGEMENT.FTS$ALL_ANALYZERS

Процедура `FTS$MANAGEMENT.FTS$ALL_ANALYZERS` возвращает список доступных анализаторов.

```sql
  PROCEDURE FTS$ALL_ANALYZERS
  RETURNS (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
      FTS$BASE_ANALYZER VARCHAR(63) CHARACTER SET UTF8,
      FTS$STOP_WORDS_SUPPORTED BOOLEAN,
      FTS$SYSTEM_FLAG BOOLEAN
  );
```

Выходные параметры:

- FTS$ANALYZER - имя анализатора;
- FTS$BASE_ANALYZER - имя базового анализатора;
- FTS$STOP_WORDS_SUPPORTED - поддерживает ли стоп слова;
- FTS$SYSTEM_FLAG - является ли системным.

#### Процедура FTS$MANAGEMENT.FTS$CREATE_ANALYZER

Процедура `FTS$MANAGEMENT.FTS$CREATE_ANALYZER` создаёт новый анализатор на основе базового. 
В качестве базового может быть использован один из встроенных анализаторов.

```sql
  PROCEDURE FTS$CREATE_ANALYZER (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$BASE_ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8 DEFAULT NULL
  );
```

Входные параметры:

- FTS$ANALYZER - имя анализатора;
- FTS$BASE_ANALYZER - имя базового анализатора;
- FTS$DESCRIPTION - описание анализатора.

#### Процедура FTS$MANAGEMENT.FTS$DROP_ANALYZER

Процедура `FTS$MANAGEMENT.FTS$DROP_ANALYZER` удаляет пользовательский анализатор. 

```sql
  PROCEDURE FTS$DROP_ANALYZER (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
```

Входные параметры:

- FTS$ANALYZER - имя анализатора.

#### Процедура FTS$MANAGEMENT.FTS$ANALYZER_STOP_WORDS

Процедура `FTS$MANAGEMENT.FTS$ANALYZER_STOP_WORDS` возвращает список стоп слов для заданного анализатора.

```sql
  PROCEDURE FTS$ANALYZER_STOP_WORDS (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  RETURNS (
      FTS$WORD VARCHAR(63) CHARACTER SET UTF8
  );
```

Входные параметры:

- FTS$ANALYZER - имя анализатора.

Выходные параметры:

- FTS$WORD - стоп слово.

#### Процедура FTS$MANAGEMENT.FTS$ADD_STOP_WORD

Процедура `FTS$MANAGEMENT.FTS$ADD_STOP_WORD` добавляет стоп слово в пользовательский анализатор.

```sql
  PROCEDURE FTS$ADD_STOP_WORD (
      FTS$ANALYZER_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$WORD VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
```

Входные параметры:

- FTS$ANALYZER_NAME - имя анализатора;
- FTS$WORD - стоп слово.

#### Процедура FTS$MANAGEMENT.FTS$DROP_STOP_WORD

Процедура `FTS$MANAGEMENT.FTS$DROP_STOP_WORD` удаляет стоп слово из пользовательского анализатора.

```sql
  PROCEDURE FTS$DROP_STOP_WORD (
      FTS$ANALYZER_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$WORD VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
```

Входные параметры:

- FTS$ANALYZER_NAME - имя анализатора;
- FTS$WORD - стоп слово.

#### Процедура FTS$MANAGEMENT.FTS$CREATE_INDEX

Процедура `FTS$MANAGEMENT.FTS$CREATE_INDEX` создаёт новый полнотекстовый индекс. 

```sql
  PROCEDURE FTS$CREATE_INDEX (
      FTS$INDEX_NAME     VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$ANALYZER       VARCHAR(63) CHARACTER SET UTF8 DEFAULT 'STANDARD',
      FTS$KEY_FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8 DEFAULT NULL
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса. Должно быть уникальным среди имён полнотекстовых индексов;
- FTS$RELATION_NAME - имя таблицы, которая должна быть проиндексирована;
- FTS$ANALYZER - имя анализатора. Если не задано используется анализатор STANDARD (StandardAnalyzer);
- FTS$KEY_FIELD_NAME - имя поля значение которого будет возращено процедурой поиска `FTS$SEARCH`, обычно это ключевое поле таблицы;
- FTS$DESCRIPTION - описание индекса.

#### Процедура FTS$MANAGEMENT.FTS$DROP_INDEX

Процедура `FTS$MANAGEMENT.FTS$DROP_INDEX` удаляет полнотекстовый индекс.

```sql
  PROCEDURE FTS$DROP_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса.

#### Процедура FTS$MANAGEMENT.SET_INDEX_ACTIVE

Процедура `FTS$MANAGEMENT.SET_INDEX_ACTIVE` позволяет сделать индекс активным или неактивным. 

```sql
  PROCEDURE FTS$SET_INDEX_ACTIVE (
      FTS$INDEX_NAME   VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$INDEX_ACTIVE BOOLEAN NOT NULL
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$INDEX_ACTIVE - флаг активности.

#### Процедура FTS$MANAGEMENT.FTS$COMMENT_ON_INDEX

Процедура `FTS$MANAGEMENT.FTS$COMMENT_ON_INDEX` добавляет или удаляет пользовательский комментарий к индексу.

```sql
  PROCEDURE FTS$COMMENT_ON_INDEX (
      FTS$INDEX_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$DESCRIPTION - пользовательское описание индекса.

#### Процедура FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD

Процедура `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD` добавляет новый поле в полнотекстовый индекс. 

```sql
  PROCEDURE FTS$ADD_INDEX_FIELD (
      FTS$INDEX_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$BOOST         DOUBLE PRECISION DEFAULT NULL
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$FIELD_NAME - имя поля, которое должно быть проиндексировано;
- FTS$BOOST - коэффициент увеличения значимости сегмента (по умолчанию 1.0).

#### Процедура FTS$MANAGEMENT.FTS$DROP_INDEX_FIELD

Процедура `FTS$MANAGEMENT.FTS$DROP_INDEX_FIELD` удаляет поле из полнотекстового индекса. 

```sql
  PROCEDURE FTS$DROP_INDEX_FIELD (
      FTS$INDEX_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$FIELD_NAME - имя поля.

#### Процедура FTS$MANAGEMENT.FTS$SET_INDEX_FIELD_BOOST

Процедура `FTS$MANAGEMENT.FTS$SET_INDEX_FIELD_BOOST` устанавливает коэффициент значимости для поля индекса. 

```sql
  PROCEDURE FTS$SET_INDEX_FIELD_BOOST (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$BOOST DOUBLE PRECISION
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$FIELD_NAME - имя поля, которое должно быть проиндексировано;
- FTS$BOOST - коэффициент увеличения значимости сегмента.

Если при добавлении поля в индекс не указать коэффициент значимости, то по умолчанию он равен 1.0.
С помощью процедуры `FTS$MANAGEMENT.FTS$SET_INDEX_FIELD_BOOST` его можно изменить.
Обратите внимание, что после запуска этой процедуры индекс необходимо перестроить.

#### Процедура FTS$MANAGEMENT.FTS$REBUILD_INDEX

Процедура `FTS$MANAGEMENT.FTS$REBUILD_INDEX` перестраивает полнотекстовый индекс. 

```sql
  PROCEDURE FTS$REBUILD_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса.

#### Процедура FTS$MANAGEMENT.FTS$REINDEX_TABLE

Процедура `FTS$MANAGEMENT.FTS$REINDEX_TABLE` перестраивает все полнотекстовые индексы для указанной таблицы.

```sql
  PROCEDURE FTS$REINDEX_TABLE (
      FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
```

Входные параметры:

- FTS$RELATION_NAME - имя таблицы.

#### Процедура FTS$MANAGEMENT.FTS$FULL_REINDEX

Процедура `FTS$MANAGEMENT.FTS$FULL_REINDEX` перестраивает все полнотекстовые индексы в базе данных.

#### Процедура FTS$MANAGEMENT.FTS$OPTIMIZE_INDEX

Процедура `FTS$MANAGEMENT.FTS$OPTIMIZE_INDEX` оптимизирует указанный индекс.

```sql
  PROCEDURE FTS$OPTIMIZE_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса.

#### Процедура FTS$MANAGEMENT.FTS$OPTIMIZE_INDEXES

Процедура `FTS$MANAGEMENT.FTS$OPTIMIZE_INDEXES` оптимизирует все полнотекстовые индексы в базе данных.


### Процедура FTS$SEARCH

Процедура `FTS$SEARCH` осуществляет полнотекстовый поиск по заданному индексу.

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

Входные параметры:

- FTS$INDEX_NAME - имя полнотекстового индекса, в котором осуществляется поиск;
- FTS$QUERY - выражение для полнотекстового поиска;
- FTS$LIMIT - ограничение на количество записей (результата поиска). По умолчанию 1000;
- FTS$EXPLAIN - объяснять ли результат поиска. По умолчанию FALSE.

Выходные параметры:

- FTS$RELATION_NAME - имя таблицы в которой найден документ;
- FTS$KEY_FIELD_NAME - имя ключевого поля в таблице;
- FTS$DB_KEY - значение ключевого поля в формате `RDB$DB_KEY`;
- FTS$ID - значение ключевого поля типа `BIGINT` или `INTEGER`;
- FTS$UUID - значение ключевого поля типа `BINARY(16)`. Такой тип используется для хранения GUID;
- FTS$SCORE - степень соответствия поисковому запросу;
- FTS$EXPLANATION - объяснение результатов поиска.

### Функция FTS$ESCAPE_QUERY

Функция `FTS$ESCAPE_QUERY` экранирует специальные символы в поисковом запросе.

```sql
FUNCTION FTS$ESCAPE_QUERY (
    FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8
)
RETURNS VARCHAR(8191) CHARACTER SET UTF8;
```

Входные параметры:

- FTS$QUERY - поисковый запрос или его часть, в котором необходимо экранировать специальные символы.

### Процедура FTS$ANALYZE

Процедура `FTS$ANALYZE` производит анализ текста, согласно заданному анализатору, и возвращает список термов.

```sql
PROCEDURE FTS$ANALYZE (
    FTS$TEXT     BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
    FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD')
RETURNS (
    FTS$TERM VARCHAR(8191) CHARACTER SET UTF8
)
```

Входные параметры:

- FTS$TEXT - текст для анализа;
- FTS$ANALYZER - анализатор.

Выходные параметры:

- FTS$TERM - терм.

### Процедура FTS$UPDATE_INDEXES

Процедура `FTS$UPDATE_INDEXES` обновляет полнотекстовые индексы по записям в журнале изменений `FTS$LOG`. 
Эта процедура обычно запускается по расписанию (cron) в отдельной сессии с некоторым интервалом, например 5 секунд.

### Пакет FTS$HIGHLIGHTER

Пакет `FTS$HIGHLIGHTER` содержит процедуры и функции возвращающие фрагменты текста, в котором найдена исходная фраза, 
и выделяет найденные термы.

#### Функция FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT

Функция `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT` возвращает лучший фрагмент текста, который соответствует выражению полнотекстового поиска,
и выделяет в нем найденные термы.

```sql
  FUNCTION FTS$BEST_FRAGMENT (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>'
  )
  RETURNS VARCHAR(8191) CHARACTER SET UTF8;
```

Входные параметры:

- FTS$TEXT - текст, в котором делается поиск;
- FTS$QUERY - выражение полнотекстового поиска;
- FTS$ANALYZER - анализатор;
- FTS$FIELD_NAME — имя поля, в котором выполняется поиск;
- FTS$FRAGMENT_SIZE - длина возвращаемого фрагмента. Не меньше, чем требуется для возврата целых слов;
- FTS$LEFT_TAG - левый тег для выделения;
- FTS$RIGHT_TAG - правый тег для выделения. 

#### Процедура FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS

Процедура `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS` возвращает лучшие фрагменты текста, которые соответствуют выражению полнотекстового поиска,
и выделяет в них найденные термы.

```sql
  PROCEDURE FTS$BEST_FRAGMENTS (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
      FTS$MAX_NUM_FRAGMENTS INTEGER NOT NULL DEFAULT 10,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>'
  )
  RETURNS (
      FTS$FRAGMENT VARCHAR(8191) CHARACTER SET UTF8
  );
```

Входные параметры:

- FTS$TEXT - текст, в котором делается поиск;
- FTS$QUERY - выражение полнотекстового поиска;
- FTS$ANALYZER - анализатор;
- FTS$FIELD_NAME — имя поля, в котором выполняется поиск;
- FTS$FRAGMENT_SIZE - длина возвращаемого фрагмента. Не меньше, чем требуется для возврата целых слов;
- FTS$MAX_NUM_FRAGMENTS - максимальное количество фрагментов;
- FTS$LEFT_TAG - левый тег для выделения;
- FTS$RIGHT_TAG - правый тег для выделения. 

Выходные параметры:

- FTS$FRAGMENT - фрагмент текста, соответствующий поисковому запросу.


### Пакет FTS$TRIGGER_HELPER

Пакет `FTS$TRIGGER_HELPER` содержит процедуры и функции помогающие создавать триггеры для поддержки актуальности 
полнотекстовых индексов.

#### Процедура FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS

Процедура `FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS` генерирует исходные коды триггеров для заданной таблицы, 
чтобы поддерживать полнотекстовые индексы в актуальном состоянии.

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

Входные параметры:

- FTS$RELATION_NAME - имя таблицы, для которой создаются триггеры;
- FTS$MULTI_ACTION - универсальный флаг триггера. Если установлено значение TRUE, 
то будет сгенерирован скрипт триггера для нескольких действий, в противном случае для каждого действия будет сгенерирован скрипт отдельного триггера;
- FTS$POSITION - позиция триггеров. 

Выходные параметры:

- FTS$TRIGGER_NAME - имя триггера;
- FTS$TRIGGER_RELATION - таблица для которой создаётся триггер;
- FTS$TRIGGER_EVENTS - события триггера;
- FTS$TRIGGER_POSITION - позиция триггера;
- FTS$TRIGGER_SOURCE - исходный кода тела триггера;
- FTS$TRIGGER_SCRIPT - скрипт создания триггера. 

#### Процедура FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS_BY_INDEX

Процедура `FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS_BY_INDEX` генерирует исходные коды триггеров для заданного индекса, 
чтобы поддерживать полнотекстовый индекс в актуальном состоянии. 

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

Входные параметры:

- FTS$INDEX_NAME - имя индекса, для которого создаются триггеры; 
- FTS$MULTI_ACTION - универсальный флаг триггера. Если установлено значение TRUE, 
то будет сгенерирован скрипт триггера для нескольких действий, в противном случае для каждого действия будет сгенерирован скрипт отдельного триггера;
- FTS$POSITION - позиция триггеров. 

Выходные параметры:

- FTS$TRIGGER_NAME - имя триггера;
- FTS$TRIGGER_RELATION - таблица для которой создаётся триггер;
- FTS$TRIGGER_EVENTS - события триггера;
- FTS$TRIGGER_POSITION - позиция триггера;
- FTS$TRIGGER_SOURCE - исходный кода тела триггера;
- FTS$TRIGGER_SCRIPT - скрипт создания. 

#### Процедура FTS$TRIGGER_HELPER.FTS$MAKE_ALL_TRIGGERS

Процедура `FTS$TRIGGER_HELPER.FTS$MAKE_ALL_TRIGGERS` генерирует исходные коды триггеров для поддержания всех полнотекстовых индексов в актуальном состоянии.

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

Входные параметры:

- FTS$MULTI_ACTION - универсальный флаг триггера. Если установлено значение TRUE, 
то будет сгенерирован скрипт триггера для нескольких действий, в противном случае для каждого действия будет сгенерирован скрипт отдельного триггера;
- FTS$POSITION - позиция триггеров. 

Выходные параметры:

- FTS$TRIGGER_NAME - имя триггера;
- FTS$TRIGGER_RELATION - таблица для которой создаётся триггер;
- FTS$TRIGGER_EVENTS - события триггера;
- FTS$TRIGGER_POSITION - позиция триггера;
- FTS$TRIGGER_SOURCE - исходный кода тела триггера;
- FTS$TRIGGER_SCRIPT - скрипт создания. 


### Пакет FTS$STATISTICS

Пакет `FTS$STATISTICS` содержит процедуры и функции для получения информации о полнотекстовых индексах и их статистике.
Этот пакет предназначен прежде всего для администраторов баз данных.

#### Функция FTS$STATISTICS.FTS$LUCENE_VERSION

Функция `FTS$STATISTICS.FTS$LUCENE_VERSION` возвращает версию библиотеки lucene++ на основе которой построен полнотекстовый поиск.

```sql
  FUNCTION FTS$LUCENE_VERSION ()
  RETURNS VARCHAR(20) CHARACTER SET UTF8 
  DETERMINISTIC;
```

#### Функция FTS$STATISTICS.FTS$GET_DIRECTORY

Функция `FTS$STATISTICS.FTS$GET_DIRECTORY` возвращает директорию в которой расположены файлы и папки полнотекстовых индексов для 
текущей базы данных.

```sql
  FUNCTION FTS$GET_DIRECTORY ()
  RETURNS VARCHAR(255) CHARACTER SET UTF8 
  DETERMINISTIC;
```

#### Процедура FTS$STATISTICS.FTS$INDEX_STATISTICS

Процедура `FTS$STATISTICS.FTS$INDEX_STATISTICS` возвращает низкоуровневую информацию и статистику для указанного индекса.

```sql
  PROCEDURE FTS$INDEX_STATISTICS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
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
      FTS$INDEX_SIZE       INTEGER
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса.

Выходные параметры:

- FTS$ANALYZER - имя анализатора;
- FTS$INDEX_STATUS - статус индекса:
    - I - неактивный;
    - N - новый индекс (требуется перестроение);
    - С - завершённый и активный;
    - U - обновлены метаданные (требуется перестроение);
- FTS$INDEX_DIRECTORY - каталог расположения индекса;
- FTS$INDEX_EXISTS - существует ли индекс физически;
- FTS$HAS_DELETIONS - были ли удаления документов из индекса;
- FTS$NUM_DOCS - количество проиндексированных документов;
- FTS$NUM_DELETED_DOCS - количество удаленных документов (до оптимизации);
- FTS$NUM_FIELDS - количество полей внутреннего индекса;
- FTS$INDEX_SIZE - размер индекса в байтах.


#### Процедура FTS$STATISTICS.FTS$INDICES_STATISTICS

Процедура `FTS$STATISTICS.FTS$INDICES_STATISTICS` возвращает низкоуровневую информацию и статистику для всех полнотекстовых индексов. 

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
      FTS$INDEX_SIZE       INTEGER
  );
```

Выходные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$ANALYZER - имя анализатора;
- FTS$INDEX_STATUS - статус индекса:
    - I - неактивный;
    - N - новый индекс (требуется перестроение);
    - С - завершённый и активный;
    - U - обновлены метаданные (требуется перестроение);
- FTS$INDEX_DIRECTORY - каталог расположения индекса;
- FTS$INDEX_EXISTS - существует ли индекс физически;
- FTS$HAS_DELETIONS - были ли удаления документов из индекса;
- FTS$NUM_DOCS - количество проиндексированных документов;
- FTS$NUM_DELETED_DOCS - количество удаленных документов (до оптимизации);
- FTS$NUM_FIELDS - количество полей внутреннего индекса;
- FTS$INDEX_SIZE - размер индекса в байтах.


#### Процедура FTS$STATISTICS.FTS$INDEX_SEGMENT_INFOS

Процедура `FTS$STATISTICS.FTS$INDEX_SEGMENT_INFOS` возвращает информацию о сегментах индекса.
Здесь сегмент определяется с точки зрения Lucene.

```sql
  PROCEDURE FTS$INDEX_SEGMENT_INFOS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  RETURNS (
      FTS$SEGMENT_NAME      VARCHAR(63) CHARACTER SET UTF8,
      FTS$DOC_COUNT         INTEGER,
      FTS$SEGMENT_SIZE      INTEGER,
      FTS$USE_COMPOUND_FILE BOOLEAN,
      FTS$HAS_DELETIONS     BOOLEAN,
      FTS$DEL_COUNT         INTEGER,
      FTS$DEL_FILENAME      VARCHAR(255) CHARACTER SET UTF8
  );
```
   
Входные параметры:

- FTS$INDEX_NAME - имя индекса.
   
Выходные параметры:

- FTS$SEGMENT_NAME - имя сегмента;
- FTS$DOC_COUNT - количество документов в сегменте;
- FTS$SEGMENT_SIZE - размер сегмента в байтах;
- FTS$USE_COMPOUND_FILE - сегмент использует составной файл;
- FTS$HAS_DELETIONS - были удаления документов из сегмента;
- FTS$DEL_COUNT - количество удаленных документов (до оптимизации);
- FTS$DEL_FILENAME - файл с удаленными документами.


#### Процедура FTS$STATISTICS.FTS$INDEX_FIELDS

Процедура `FTS$STATISTICS.FTS$INDEX_FIELDS` возвращает имена внутренних полей индекса.

```sql
  PROCEDURE FTS$INDEX_FIELDS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  RETURNS (
      FTS$FIELD_NAME VARCHAR(127) CHARACTER SET UTF8
  );
```
   
Входные параметры:

- FTS$INDEX_NAME - имя индекса.
   
Выходные параметры:

- FTS$FIELD_NAME - имя поля.


#### Процедура FTS$STATISTICS.FTS$INDEX_FILES

Процедура `FTS$STATISTICS.FTS$INDEX_FILES` возвращает информацию об индексных файлах.

```sql
  PROCEDURE FTS$INDEX_FILES (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  RETURNS (
      FTS$FILE_NAME VARCHAR(127) CHARACTER SET UTF8,
      FTS$FILE_TYPE VARCHAR(63) CHARACTER SET UTF8,
      FTS$FILE_SIZE INTEGER
  );
```
   
Входные параметры:

- FTS$INDEX_NAME - имя индекса.
   
Выходные параметры:

- FTS$FILE_NAME - имя файла;
- FTS$FILE_TYPE - тип файла;
- FTS$FILE_SIZE - размер файла в байтах.


#### Процедура FTS$STATISTICS.FTS$INDEX_FIELD_INFOS

Процедура `FTS$STATISTICS.FTS$INDEX_FIELD_INFOS` возвращает информацию о полях индекса.

```sql
  PROCEDURE FTS$INDEX_FIELD_INFOS (
      FTS$INDEX_NAME   VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$SEGMENT_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL
  )
  RETURNS (
      FTS$FIELD_NAME                      VARCHAR(127) CHARACTER SET UTF8,
      FTS$FIELD_NUMBER                    SMALLINT,
      FTS$IS_INDEXED                      BOOLEAN,
      FTS$STORE_TERM_VECTOR               BOOLEAN,
      FTS$STORE_OFFSET_TERM_VECTOR        BOOLEAN,
      FTS$STORE_POSITION_TERM_VECTOR      BOOLEAN,
      FTS$OMIT_NORMS                      BOOLEAN,
      FTS$OMIT_TERM_FREQ_AND_POS          BOOLEAN,
      FTS$STORE_PAYLOADS                  BOOLEAN
  );
```
   
Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$SEGMENT_NAME - имя сегмента индекса,
          если не указано, то берется активный сегмент.
   
Выходные параметры:

- FTS$FIELD_NAME - имя поля;
- FTS$FIELD_NUMBER - номер поля;
- FTS$IS_INDEXED - поле проиндексировано;
- FTS$STORE_TERM_VECTOR - зарезервировано;
- FTS$STORE_OFFSET_TERM_VECTOR - зарезервировано;
- FTS$STORE_POSITION_TERM_VECTOR - зарезервировано;
- FTS$OMIT_NORMS - зарезервировано;
- FTS$OMIT_TERM_FREQ_AND_POS - зарезервировано;
- FTS$STORE_PAYLOADS - зарезервировано.


#### Процедура FTS$STATISTICS.FTS$INDEX_TERMS

Процедура `FTS$STATISTICS.FTS$INDEX_TERMS` возвращает термы (слова) содержащиеся в индексе.

С помощью этой процедуры удобно производить отладку полнотекстового индекса.

```sql
  PROCEDURE FTS$INDEX_TERMS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  RETURNS (
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8,
      FTS$TERM       VARCHAR(8191) CHARACTER SET UTF8,
      FTS$DOC_FREQ   INTEGER
  );
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса.
   
Выходные параметры:

- FTS$FIELD_NAME - имя поля;
- FTS$TERM - терм (слово);
- FTS$DOC_FREQ - количество документов содержащих заданный терм (слово).
