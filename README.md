# Lucene UDR

В Firebird отсутсвует встроенная подсистема полнотекстового поиска. Библиотека Lucene UDR реализует
процедуры и функции полнотекстового поиска с помощью основан на свободно распространяемой библиотеки Lucene. Оригинальный поисковый движок
Lucene написан на языке Java. К сожалению плагин FB Java для написания внешних хранимых процедур и функций
пока ещё в стадии Beta версии. Поэтому Lucene UDR использует порт Lucene на язык C++  
[Lucene++](https://github.com/luceneplusplus/LucenePlusPlus). Lucene++ чуть более быстрый, чем оригинальный движок Lucene,
но обладает немного меньшими возможностями.

## Установка Lucene UDR

Для установки Lucene UDR необходимо:

1. Распаковать zip архив с динамическими библиотеками в каталог `plugins\udr`
2. Выполнить скрипт [fts$install.sql](https://github.com/sim1984/lucene_udr/blob/main/fts%24install.sql) для регистраци процедур и функций в индексируемой БД.

Скачать готовые сборки можно по ссылкам:
* [LuceneUdr_Win_x64.zip](https://github.com/sim1984/lucene_udr/releases/download/0.8/LuceneUdr_Win_x64.zip)
* [LuceneUdr_Win_x86.zip](https://github.com/sim1984/lucene_udr/releases/download/0.8/LuceneUdr_Win_x86.zip)

В настоящий момент других сборок нет.


## Описание процедур и функций для работы с полнотекстовым поиском

### Пакет FTS$MANAGEMENT

Пакет `FTS$MANAGEMENT` содержит процедуры и функции для управления полнотекстовыми индексами. Этот пакет предназначен
для администраторов базы данных.

Заголовок этого пакета выглядит следующим образом:

```sql
CREATE OR ALTER PACKAGE FTS$MANAGEMENT
AS
BEGIN
  /**
   * Returns the directory where the files and folders
   * of the full-text index for the current database are located.
  **/
  FUNCTION FTS$GET_DIRECTORY ()
  RETURNS VARCHAR(255) CHARACTER SET UTF8;

  /**
   * Returns a list of available analyzers.
   *
   * Output parameters:
   *   FTS$ANALYZER - analyzer name.
  **/
  PROCEDURE FTS$ANALYZERS
  RETURNS (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8);

  /**
   * Create a new full-text index.
   *
   * Input parameters:
   *   FTS$INDEX_NAME - name of the index;
   *   FTS$ANALYZER - analyzer name;
   *   FTS$DESCRIPTION - description of the index.
  **/
  PROCEDURE FTS$CREATE_INDEX (
      FTS$INDEX_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$ANALYZER    VARCHAR(63) CHARACTER SET UTF8 DEFAULT 'STANDARD',
      FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8 DEFAULT NULL);

  /**
   * Delete the full-text index.
   *
   * Input parameters:
   *   FTS$INDEX_NAME - name of the index.
  **/
  PROCEDURE FTS$DROP_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL);

  /**
   * Allows to make the index active or inactive.
   *
   * Input parameters:
   *   FTS$INDEX_NAME - name of the index;
   *   FTS$INDEX_ACTIVE - activity flag.
  **/
  PROCEDURE FTS$SET_INDEX_ACTIVE (
      FTS$INDEX_NAME   VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$INDEX_ACTIVE BOOLEAN NOT NULL);

  /**
   * Add a new segment (indexed table field) of the full-text index.
   *
   * Input parameters:
   *   FTS$INDEX_NAME - name of the index;
   *   FTS$RELATION_NAME - name of the table to be indexed;
   *   FTS$FIELD_NAME - the name of the field to be indexed;
   *   FTS$BOOST - the coefficient of increasing the significance of the segment.
  **/
  PROCEDURE FTS$ADD_INDEX_FIELD (
      FTS$INDEX_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$BOOST         DOUBLE PRECISION DEFAULT NULL);

  /**
   * Delete a segment (indexed table field) of the full-text index.
   *
   * Input parameters:
   *   FTS$INDEX_NAME - index name;
   *   FTS$RELATION_NAME - table name;
   *   FTS$FIELD_NAME - field name.
  **/
  PROCEDURE FTS$DROP_INDEX_FIELD (
      FTS$INDEX_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL);


  /**
   * Rebuild the full-text index.
   *
   * Input parameters:
   *   FTS$INDEX_NAME - index name.
   **/
  PROCEDURE FTS$REBUILD_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL);

  /**
   * Rebuild all full-text indexes for the specified table.
   *
   * Input parameters:
   *   FTS$RELATION_NAME - table name.
  **/
  PROCEDURE FTS$REINDEX_TABLE (
      FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL);

  /**
   * Rebuild all full-text indexes in the database.
  **/
  PROCEDURE FTS$FULL_REINDEX;
END
```

#### Функция FTS$GET_DIRECTORY

Функция `FTS$GET_DIRECTORY()` возвращает директорию в которой располены файлы и папки полнотекстового индекса для текущей базы данных.

#### Процедура FTS$ANALYZERS

Процедура `FTS$ANALYZERS` возвращает список доступных анализаторов.

Выходные параметры:

- FTS$ANALYZER - имя анализатора.

#### Процедура FTS$CREATE_INDEX

Процедура `FTS$CREATE_INDEX()` создаёт новый полнотекстовый индекс. 

Входные параметры:

- FTS$INDEX_NAME - имя индекса. Должно быть уникальным среди имён полнотекстовых индексов;
- FTS$ANALYZER - имя анализатора. Если не задано используется анализатор STANDARD (StandardAnalyzer);
- FTS$DESCRIPTION - описание индекса.

#### Процедура FTS$DROP_INDEX

Процедура `FTS$DROP_INDEX()` удаляет полнотекстовый индекс. 

Входные параметры:

- FTS$INDEX_NAME - имя индекса.

#### Процедура SET_INDEX_ACTIVE

Процедура `SET_INDEX_ACTIVE()` позволяет сделать индекс активным или неактивным. 

Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$INDEX_AVTIVE - флаг активности.

#### Процедура FTS$ADD_INDEX_FIELD

Процедура `FTS$ADD_INDEX_FIELD()` добавляет новый сегмент (индексируемое поле таблицы) полнотекстового индекса. 

Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$RELATION_NAME - имя таблицы, котоая должна быть проиндексиована;
- FTS$FIELD_NAME - имя поля, которое должно быть проиндексировано;
- FTS$BOOST - коэффициент увеличения значимости сегмента (по умолчанию 1.0).

#### Процедура FTS$DROP_INDEX_FIELD

Процедура `FTS$DROP_INDEX_FIELD()` удаляет сегмент (индексируемое поле таблицы) полнотекстового индекса. 

Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$RELATION_NAME - имя таблицы;
- FTS$FIELD_NAME - имя поля.

#### Процедура FTS$REBUILD_INDEX

Процедура `FTS$REBUILD_INDEX()` перестраивает полнотекстовый индекс. 

Входные параметры:

- FTS$INDEX_NAME - имя индекса.

#### Процедура FTS$REINDEX_TABLE

Процедура `FTS$REINDEX_TABLE()` перестраивает все полнотекстовые индексы для указанной таблицы.

Входные параметры:

- FTS$RELATION_NAME - имя таблицы.

#### Процедура FTS$FULL_REINDEX

Процедура `FTS$FULL_REINDEX()` перестраивает все полнотекстовые индексы в базе данных.


### Процедура FTS$SEARCH

Процедура `FTS$SEARCH` осуществляет полнотекстовый поиск по заданному индексу.

Входные параметры:

- FTS$INDEX_NAME - имя полнотекстового индекса, в котором осуществляется поиск;
- FTS$SEARCH_RELATION - имя таблицы, ограничивает поиск только заданной таблицей. Если таблица не задана, то поиск делается по всем сегментам индекса;
- FTS$QUERY - выражение для полнотекстового поиска;
- FTS$LIMIT - ограничение на количество записей (результата поиска). По умолчанию 1000;
- FTS$EXPLAIN - объяснять ли результат поиска. По умолчанию FALSE.

Выходные параметры:

- FTS$RELATION_NAME - имя таблицы в которой найден документ;
- FTS$REC_ID - ссылка на запись в таблице в которой был найден документ (соответствует псевдо полю RDB$DB_KEY);
- FTS$SCORE - степень соответсвия поисковому запросу;
- FTS$EXPLANATION - объяснение результата запроса.

### Процедура FTS$LOG_CHANGE

Процедура `FTS$LOG_CHANGE` добавляет запись об изменении одного из полей входящих в полнтекствые индексы, построенные на таблице, в журнал изменений `FTS$LOG`,
на оcнове которого будут обновляться полнотекстовые индексы.

Входные параметры:

- FTS$RELATION_NAME - имя таблицы для которой добавляется ссылка на запись;
- FTS$REC_ID - ссылка на запись в таблице (соответствует псевдо полю RDB$DB_KEY);
- FTS$CHANGE_TYPE - тип изменения (I - INSERT, U - UPDATE, D - DELETE).

### Процедура FTS$CLEAR_LOG

Процедура `FTS$CLEAR_LOG` очищает журнал изменений `FTS$LOG`, на оcнове которого обновляются полнотекстовые индексы.

### Процедура FTS$UPDATE_INDEXES

Процедура `FTS$UPDATE_INDEXES` обвновляет полнотекстовые индексы по записям в журнале изменений `FTS$LOG`. 
Эта процедура обычно запускается по расписанию (cron) в отдельной сессии с некотоым интервалов, например 5 секунд.

### Пакет FTS$HIGHLIGHTER

Пакет `FTS$HIGHLIGHTER` содержит процедуры и функции возаращающие фрагменты текста, в котором найдена исходная фраза, и выделяет найденые слова.

Заголовок этого пакета выглядит следующим образом:

```sql
CREATE OR ALTER PACKAGE FTS$HIGHLIGHTER
AS
BEGIN
  /**
   * The FTS$BEST_FRAGMENT function returns a text fragment with highlighted
   * occurrences of words from the search query.
   *
   * Input parameters:
   *   FTS$TEXT - the text in which the phrase is searched;
   *   FTS$QUERY - full-text search expression;
   *   FTS$ANALYZER - analyzer;
   *   FTS$FIELD_NAME - the name of the field that is being searched;
   *   FTS$FRAGMENT_SIZE - the length of the returned fragment.
   *       No less than is required to return whole words;
   *   FTS$LEFT_TAG - the left tag to highlight;
   *   FTS$RIGHT_TAG - the right tag to highlight.
  **/
  FUNCTION FTS$BEST_FRAGMENT (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>')
  RETURNS VARCHAR(8191) CHARACTER SET UTF8;

  /**
   * The FTS$BEST_FRAGMENTS procedure returns text fragments with highlighted
   * occurrences of words from the search query.
   *
   * Input parameters:
   *   FTS$TEXT - the text in which the phrase is searched;
   *   FTS$QUERY - full-text search expression;
   *   FTS$ANALYZER - analyzer;
   *   FTS$FIELD_NAME - the name of the field that is being searched;
   *   FTS$FRAGMENT_SIZE - the length of the returned fragment.
   *       No less than is required to return whole words;
   *   FTS$MAX_NUM_FRAGMENTS - maximum number of fragments.
   *   FTS$LEFT_TAG - the left tag to highlight;
   *   FTS$RIGHT_TAG - the right tag to highlight.
   *
   * Output parameters:
   *   FTS$FRAGMENT - text fragment in which the searched phrase was found. 
  **/
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
END
```

#### Процедура FTS$BEST_FRAGMENT

Функция `FTS$BEST_FRAGMENT()` возращает лучший фрагмент текста, который соответвует выражению полнотекстового поиска,
и выделяет в нем найденные слова.

Входные параметры:

- FTS$TEXT - текст, в котором ищется фраза;
- FTS$QUERY - выражение полнотекстового поиска;
- FTS$ANALYZER - анализатор;
- FTS$FIELD_NAME — имя поля, в котором выполняется поиск;
- FTS$FRAGMENT_SIZE - длина возвращаемого фрагмента.
- Не меньше, чем требуется для возврата целых слов;
- FTS$MAX_NUM_FRAGMENTS - максимальное количество фрагментов.
- FTS$LEFT_TAG - левый тег для выделения;
- FTS$RIGHT_TAG - правильный тег для выделения. 



### Пакет FTS$TRIGGER_HELPER

Пакет `FTS$TRIGGER_HELPER` содержит процедуры и функции помогающие создавать триггеры для поддержки актуальности полнотекстовых индексов.

Заголовок этого пакета выглядит следующим образом:

```sql
CREATE OR ALTER PACKAGE FTS$TRIGGER_HELPER
AS
BEGIN
  /**
   * The FTS$MAKE_TRIGGERS procedure generates trigger source codes for
   * a given table to keep full-text indexes up-to-date.
   *
   * Input parameters:
   *   FTS$RELATION_NAME - table name for which triggers are created;
   *   FTS$MULTI_ACTION - universal trigger flag. If set to TRUE,
   *      then a trigger for multiple actions will be created,
   *      otherwise a separate trigger will be created for each action.
   *
   * Output parameters:
   *   FTS$TRIGGER_SOURCE - the text of the source code of the trigger.
  **/
  PROCEDURE FTS$MAKE_TRIGGERS (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
    FTS$MULTI_ACTION BOOLEAN DEFAULT TRUE
  )
  RETURNS (
    FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  );

  /**
   * The FTS$MAKE_TRIGGERS_BY_INDEX procedure generates trigger source codes
   * for a given index to keep the full-text index up to date.
   *
   * Input parameters:
   *   FTS$INDEX_NAME - index name for which triggers are created; 
   *   FTS$MULTI_ACTION - universal trigger flag. If set to TRUE,
   *      then a trigger for multiple actions will be created,
   *      otherwise a separate trigger will be created for each action.
   *
   * Output parameters:
   *   FTS$TRIGGER_SOURCE - the text of the source code of the trigger.
  **/
  PROCEDURE FTS$MAKE_TRIGGERS_BY_INDEX (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8,
    FTS$MULTI_ACTION BOOLEAN DEFAULT TRUE
  )
  RETURNS (
    FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  );


  /**
   * The FTS$MAKE_ALL_TRIGGERS procedure generates trigger source codes
   * to keep all full-text indexes up to date.
   *
   * Input parameters:
   *   FTS$MULTI_ACTION - universal trigger flag. If set to TRUE,
   *      then a trigger for multiple actions will be created,
   *      otherwise a separate trigger will be created for each action.
   *
   * Output parameters:
   *   FTS$TRIGGER_SOURCE - the text of the source code of the trigger.
  **/
  PROCEDURE FTS$MAKE_ALL_TRIGGERS (
    FTS$MULTI_ACTION BOOLEAN DEFAULT TRUE
  )
  RETURNS (
    FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  );
   
END
```

## Статусы индекса

Описание индексов хранится в служебной таблице `FTS$INDICES`.

Поле `FTS$INDEX_STATUS` хранит статус индекса. Индекс может иметь 4 статуса:

*N* - New index. Новый индекс. Устанавливается при создани индекса, в котором ещё нет ни одного сегмента.

*U* - Updated metadata. Устанавливается каждый раз, когда изменяются метаданные индекса, например при добавлени или удалени сегмента индекса. 
Если индекс имеет такой статус, то он требует перестроения, чтобы поиск по нему работал корректно.

*I* - Inactive. Неактивный индекс. Неактивные индексы не обновляются процедурой `FTS$UPDATE_INDEXES`.

*C* - Complete. Активный индекс. Такие индексы обновляются процедурой `FTS$UPDATE_INDEXES`. Индекс переходит в это состония только после полного построения или перестроения.

## Создание полнотекствых индексов и поиск по ним

Перед использованием полнотекстового поиска в вашей базе данных необходимо произвести предварительную настройку.
Настройки Lucene UDR находятся в файле `$(root)\fts.ini`. Если этого файла нет, то создайте его самостоятельно.

В этом файле задаётся путь к папке в котрой будут создаваться полнотекстовые индексы для конретной базы данных.

В качестве имени сексции ini файла должен быть задан полный путь к базе данных или алиас (в зависимост от значения параметра `DatabaseAccess` в `firebird.conf`
Путь к директории полнотекстовых индексов указывается в ключе `ftsDirectory`. 

```ini
[horses]
ftsDirectory=f:\fbdata\3.0\fts\horses

[f:\fbdata\3.0\horses.fdb]
ftsDirectory=f:\fbdata\3.0\fts\horses
```

Важно: пользователь или группа, под которым выполняется служба Firebird, должен иметь права на чтение и запись для директории с полнотекстовыми индексами.

Получить расположение директории для полнотекстовых индексов можно с пмомщью запроса:

```sql
SELECT FTS$MANAGEMENT.FTS$GET_DIRECTORY() AS DIR_NAME
FROM RDB$DATABASE
```

Для создания полнотекстового индекса необходимо выполнить последовательно три шага:

1. Создание индекса с помощь процедуры `FTS$MANAGEMENT.FTS$CREATE_INDEX()`. На этом
шаге задаётся имя индекса, используемый анализатор и описание индекса. Список доступных анализаторов 
можно узнать с помощью процедуры `FTS$MANAGEMENT.FTS$ANALYZERS()`

2. Добавление сегментов индекса с помощью процедуры `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD()`.
Под сегментом индекса понимается имя таблицы и имя индексирруемого поля. 
Один полнотекстовый индекс может индексировать сразу несколько таблиц.

3. Построение индекса с помощью процедуру `FTS$MANAGEMENT.FTS$REBUILD_INDEX()`.
На этом этапе читается все таблицы, которые были указаны в сегментах индекса и содержимое полей, указанных в сегметах
обрабатывается и попадает в индекс. При повтороном вызове, файлы индекса полностью уничтожаются и происходит повторная индексация.

Список доступных анализаторов:

* STANDARD - StandardAnalyzer (Английский язык);

* ARABIC - ArabicAnalyzer (Арабский язык);

* BRAZILIAN - BrazilianAnalyzer (Бразильский язык);

* CHINESE - ChineseAnalyzer (Китайский язык);

* CJK - CJKAnalyzer (Китайское письмо);

* CZECH - CzechAnalyzer (Чешский язык);

* DUTCH - DutchAnalyzer (Голландский язык);

* ENGLISH - StandardAnalyzer (Английский язык);

* FRENCH - FrenchAnalyzer (Французский язык);

* GERMAN - GermanAnalyzer (Немецкий язык);

* GREEK - GreekAnalyzer (Греческий язык);

* PERSIAN - PersianAnalyzer (Персидский язык);

* RUSSIAN - RussianAnalyzer (Русский язык).


Далее будут привдеены примеры полнотекстовых индексов.

### Пример создания односегментного индекса для полнотекстового поиска на английском языке

Ниже создаётся односегментный индекс для полнотекстового поиска на английском языке, поскольку по умолчанию
используется анализатор STANDART.

```sql
-- создание индекса
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_REMARK');

COMMIT;

-- добавление сегмента (поля REMARK таблицы HORSE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_REMARK', 'HORSE', 'REMARK');

COMMIT;

-- построение индекса
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_REMARK');

COMMIT;
```

Поиск по такому индексу можно делать следующим образом:

```sql
SELECT
    FTS.*,
    HORSE.CODE_HORSE,
    HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK', 'HORSE', 'паспорт') FTS
    LEFT JOIN HORSE ON
          HORSE.RDB$DB_KEY = FTS.FTS$REC_ID  

```

В качестве второго параметра можно указать NULL, поскольку индекс обрабатывает только одну таблицу.

```sql
SELECT
    FTS.*,
    HORSE.CODE_HORSE,
    HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK', NULL, 'паспорт') FTS
    LEFT JOIN HORSE ON
          HORSE.RDB$DB_KEY = FTS.FTS$REC_ID  

```

### Пример создания односегментного индекса для полнотекстового поиска на русском языке


```sql
-- создание индекса
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_REMARK_RU', 'RUSSIAN');

COMMIT;

-- добавление сегмента (поля REMARK таблицы HORSE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_REMARK_RU', 'HORSE', 'REMARK');

COMMIT;

-- построение индекса
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_REMARK_RU');

COMMIT;
```

Поиск по такому индексу можно делать следующим образом:

```sql
SELECT
    FTS.*,
    HORSE.CODE_HORSE,
    HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK_RU', 'HORSE', 'паспорт') FTS
    LEFT JOIN HORSE ON
          HORSE.RDB$DB_KEY = FTS.FTS$REC_ID 

```

Обратите внимание. Результаты поиска по индексам IDX_HORSE_REMARK и IDX_HORSE_REMARK_RU будут отличаться.

### Пример полнотекстового индекса с двумя полями одной таблицы

```sql
-- создание индекса
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_NOTES', 'RUSSIAN');

COMMIT;

-- добавление сегмента (поля REMARK таблицы NOTE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_NOTES', 'NOTE', 'REMARK');
-- добавление сегмента (поля REMARK_EN таблицы NOTE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_NOTES', 'NOTE', 'REMARK_EN');

COMMIT;

-- построение индекса
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_NOTES');

COMMIT;
```

Поиск по такому индексу можно делать следующим образом:

```sql
SELECT
    FTS.*,
    NOTE.CODE_HORSE,
    NOTE.CODE_NOTETYPE,
    NOTE.REMARK,
    NOTE.REMARK_EN
FROM FTS$SEARCH('IDX_HORSE_NOTE', 'NOTE', 'неровно') FTS
    LEFT JOIN NOTE ON
          NOTE.RDB$DB_KEY = FTS.FTS$REC_ID  
```

### Пример полнотекстового индекса с двумя таблицами

```sql
-- создание индекса
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_NOTE_2', 'RUSSIAN');

COMMIT;

-- добавление сегмента (поля REMARK таблицы NOTE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_NOTE_2', 'NOTE', 'REMARK');
-- добавление сегмента (поля REMARK таблицы HORSE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_NOTE_2', 'HORSE', 'REMARK');

COMMIT;

-- построение индекса
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_NOTE_2');

COMMIT;
```

Поиск по такому индексу можно делать следующим образом:


```sql
-- поиск в таблице NOTE
SELECT
    FTS.*,
    NOTE.CODE_HORSE,
    NOTE.CODE_NOTETYPE,
    NOTE.REMARK,
    NOTE.REMARK_EN
FROM FTS$SEARCH('IDX_HORSE_NOTE_2', 'NOTE', 'неровно') FTS
    LEFT JOIN NOTE ON
          NOTE.RDB$DB_KEY = FTS.FTS$REC_ID 
  
-- поиск в таблице HORSE          
SELECT
    FTS.*,
    HORSE.CODE_HORSE,
    HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK', 'HORSE', 'паспорт') FTS
    LEFT JOIN HORSE ON
          HORSE.RDB$DB_KEY = FTS.FTS$REC_ID  

-- выдаст имя таблицы и DB_KEY из всех таблиц
SELECT
    FTS.*
FROM FTS$SEARCH('IDX_HORSE_NOTE_2', NULL, 'паспорт') FTS

-- Получение записей из двух таблиц
SELECT
    FTS.*,
    COALESCE(HORSE.CODE_HORSE, NOTE.CODE_HORSE) AS CODE_HORSE,
    HORSE.REMARK AS HORSEREMARK,
    NOTE.REMARK AS NOTEREMARK
FROM FTS$SEARCH('IDX_HORSE_NOTE_2', NULL, 'паспорт') FTS
    LEFT JOIN HORSE ON
          HORSE.RDB$DB_KEY = FTS.FTS$REC_ID AND
          FTS.FTS$RELATION_NAME = 'HORSE'
    LEFT JOIN NOTE ON
          NOTE.RDB$DB_KEY = FTS.FTS$REC_ID AND
          FTS.FTS$RELATION_NAME = 'NOTE'
```

## Поддержание актуальности данных в полнотекстовых индексах

Для поддержки актуальности полнотекстовых индексов существует несколлько способов:

1. Периодически вызывать процедуру `FTS$MANAGEMENT.FTS$REBUILD_INDEX` для заданного индекса. 
Этот способ полностью перестраивает полнотекстовый индекс. В этом случае читаются все записи всех таблиц входящих в заданный индекс.

2. Поддерживать полнотекстовые индексы можно с помощью триггеров и вызова процедуры `FTS$LOG_CHANGE`.
В этом случае запись об измении добавляется в специальную таблицу `FTS$LOG` (журнал изменений).
Изменения из журнала переносятся в полнотекстовые индексы с помощью вызова процедуры `FTS$UPDATE_INDEXES`.
Вызов этой процедуры необходимо делать в отдельном скрипте, который можно поставить в планировщик заданий (Windows) или cron (Linux) с некоторой периодичностью, например 5 минут.

3. Отложенное обновление полнотекстовых индексов, с помомщью технологии FirebirdStreaming. В этом случае специальная служба читает логи репликации и извлекает из них
информацию необходимую для обновления полнотекстовых индексов (в процессе разработки).


### Пример триггеров для поддержки актуальности полнотекстовых индексов

Для поддержки актуальности полнотекстовых индексов можно создать следующие триггеры:

```sql
SET TERM ^ ;

CREATE OR ALTER TRIGGER TR_FTS$HORSE_AIUD FOR HORSE
ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 100
AS
BEGIN
  IF (INSERTING AND (NEW.REMARK IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_CHANGE('HORSE', NEW.RDB$DB_KEY, 'I');
  IF (UPDATING AND (NEW.REMARK IS DISTINCT FROM OLD.REMARK)) THEN
    EXECUTE PROCEDURE FTS$LOG_CHANGE('HORSE', OLD.RDB$DB_KEY, 'U');
  IF (DELETING AND (OLD.REMARK IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_CHANGE('HORSE', OLD.RDB$DB_KEY, 'D');
END
^

SET TERM ; ^
```

В данном примере созданы триггеры для поддержки актуальности полнотекстового построенного на поле REMARK таблицы HORSE.

Обновление индексов осуществляется в сцециальном скрипте `fts$update.sql`

```sql
EXECUTE PROCEDURE FTS$UPDATE_INDEXES;
```

Правила написания триггеров для поддержки полнотекстовых индексов:

1. В триггер должны быть условия по всем поля которые участвуют хотя бы в одном полнотекстовом индексе.
Такие условия должны быть объеденены через OR.

2. Для операции INSERT необходимо проверять все поля, входящие в полнотекстовые индексы значение которых отличается от NULL.
Если это условие соблюдается, то необходимо выполнить процедуру `FTS$LOG_CHANGE('<имя таблицы>', NEW.RDB$DB_KEY, 'I');`.

3. Для операции UPDATE необходимо проверять все поля, входящие в полнотекстовые индексы значение которых изменилось.
Если это условие соблюдается, то необходимо выполнить процедуру `FTS$LOG_CHANGE('<имя таблицы>', OLD.RDB$DB_KEY, 'U');`.

4. Для операции DELETE необходимо проверять все поля, входящие в полнотекстовые индексы значение которых отличается от NULL.
Если это условие соблюдается, то необходимо выполнить процедуру `FTS$LOG_CHANGE('<имя таблицы>', OLD.RDB$DB_KEY, 'D');`.
