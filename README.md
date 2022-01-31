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
2. Выполнить скрипт fts$install.sql для регистраци процедур и функций. В индексируемой БД.

Скачать готовые сборки можно по ссылкам:
* [LuceneUdr_Win_x64.zip](https://github.com/sim1984/lucene_udr/releases/download/1.0/LuceneUdr_Win_x64.zip)

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
  FUNCTION FTS$GET_DIRECTORY ()
  RETURNS VARCHAR(255) CHARACTER SET UTF8;

  PROCEDURE FTS$CREATE_INDEX (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
     FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
     FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8 DEFAULT NULL
  );

  PROCEDURE FTS$DROP_INDEX (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );

  PROCEDURE FTS$ADD_INDEX_FIELD (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );

  PROCEDURE FTS$DROP_INDEX_FIELD (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );

  PROCEDURE FTS$REBUILD_INDEX (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
END
```

#### Функция FTS$GET_DIRECTORY

Функция `FTS$GET_DIRECTORY()` возвращает директорию в которой располены файлы и папки полнотекстового индекса для текущей базы данных.

#### Процедура FTS$CREATE_INDEX

Процедура `FTS$CREATE_INDEX()` создаёт новый полнотекстовый индекс. 

Входные параметры:

- FTS$INDEX_NAME - имя индекса. Должно быть уникальным среди имён полнотекстовых индексов;
- FTS$ANALYZER - имя анализатора. Если не задано используется анализатор STANDART (StandartAnalyzer);
- FTS$DESCRIPTION - описание индекса.

Замечание: в настоящее время FTS$ANALYZER не учитывается. Эта возможность будет добавлена позже.

#### Процедура FTS$DROP_INDEX

Процедура `FTS$DROP_INDEX()` удаляет полнотекстовый индекс. 

Входные параметры:

- FTS$INDEX_NAME - имя индекса.

#### Процедура FTS$ADD_INDEX_FIELD

Процедура `FTS$ADD_INDEX_FIELD()` добавляет новый сегмент (индексируемое поле таблицы) полнотекстового индекса. 

Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$RELATION_NAME - имя таблицы, котоая должна быть проиндексиована;
- FTS$FIELD_NAME - имя поля, которое должно быть проиндексировано.

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

### Процедура FTS$SEARCH

Процедура `FTS$SEARCH` осуществляет полнотекстовый поиск по заданному индексу.

Входные параметры:

- FTS$INDEX_NAME - имя полнотекстового индекса, в котором осуществляется поиск;
- FTS$RELATION_NAME - имя таблицы, ограничивает поиск только заданной таблицей. Если таблица не задана, то поиск делается по всем сегментам индекса;
- FTS$FILTER - выражение полнотекстового поиска.

Выходные параметры:

- RDB$RELATION_NAME - имя таблицы в которой наден документ;
- FTS$DB_KEY - ссылка на запись в таблице в которой был найден документ (соответствует псевдо полю RDB$DB_KEY);
- FTS$SCORE - степень соответсвия поисковому запросу.

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

Получить расположение директори для полнотекстовых индексов можно с пмомщью запроса:

```sql
SELECT FTS$MANAGEMENT.FTS$GET_DIRECTORY() AS DIR_NAME
FROM RDB$DATABASE
```

Для создания полнотекстового индекса необходимо выполнить последовательно три шага:

1. Создание индекса с помощь процедуры `FTS$MANAGEMENT.FTS$CREATE_INDEX()`. На этом
шаге задаётся имя индекса, используемый анализатор и описание индекса.

2. Добавление сегментов индекса с помощью процедуры `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD()`.
Под сегментом индекса понимается имя таблицы и имя индексирруемого поля. 
Один полнотекстовый индекс может индексировать сразу несколько таблиц.

3. Построение индекса с помощью процедуру `FTS$MANAGEMENT.FTS$REBUILD_INDEX()`.
На этом этапе читается все таблицы, которые были указаны в сегментах индекса и содержимое полей, указанных в сегметах
обрабатывается и попадает в индекс. При повтороном вызове, файлы индекса полностью уничтожаются и происходит повторная индексация.

Далее будут привдеены примеры полнотекстовых индексов.

### Пример односегментного полнотекстового индекса

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

```
SELECT
    FTS.*,
    HORSE.CODE_HORSE,
    HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK', 'HORSE', 'паспорт') FTS
    LEFT JOIN HORSE ON
          HORSE.RDB$DB_KEY = FTS.FTS$DB_KEY  

```

В качестве второго параметра можно указать NULL, поскольку индекс обрабатывает толко одну таблицу.


### Пример полнотекстового индекса с двумя полями одной таблицы

```sql
-- создание индекса
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_NOTES');

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
          NOTE.RDB$DB_KEY = FTS.FTS$DB_KEY  
```

### Пример полнотекстового индекса с двумя таблицами

```sql
-- создание индекса
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_NOTE_2');;

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
          NOTE.RDB$DB_KEY = FTS.FTS$DB_KEY 
  
-- поиск в таблице HORSE          
SELECT
    FTS.*,
    HORSE.CODE_HORSE,
    HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK', 'HORSE', 'паспорт') FTS
    LEFT JOIN HORSE ON
          HORSE.RDB$DB_KEY = FTS.FTS$DB_KEY  

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
          HORSE.RDB$DB_KEY = FTS.FTS$DB_KEY AND
          FTS.RDB$RELATION_NAME = 'HORSE'
    LEFT JOIN NOTE ON
          NOTE.RDB$DB_KEY = FTS.FTS$DB_KEY AND
          FTS.RDB$RELATION_NAME = 'NOTE'
```
