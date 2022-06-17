# UDR full-text search based on Lucene++

В Firebird отсутствует встроенная подсистема полнотекстового поиска. Библиотека Lucene UDR реализует
процедуры и функции полнотекстового поиска с помощью основан на свободно распространяемой библиотеки Lucene. 
Оригинальный поисковый движок Lucene написан на языке Java. К сожалению плагин FB Java для написания внешних 
хранимых процедур и функций пока ещё в стадии Beta версии. Поэтому Lucene UDR использует порт Lucene на язык C++  
[Lucene++](https://github.com/luceneplusplus/LucenePlusPlus). Lucene++ чуть более быстрый, чем оригинальный движок 
Lucene, но обладает немного меньшими возможностями.

## Установка Lucene UDR

Для установки Lucene UDR необходимо:

1. Распаковать zip архив с динамическими библиотеками в каталог `plugins\udr`
2. Выполнить скрипт [fts$install.sql](https://github.com/sim1984/lucene_udr/blob/main/sql/fts%24install.sql) 
для регистрации процедур и функций в индексируемой БД. 
Для баз данных 1 SQL диалекта [fts$install_1.sql](https://github.com/sim1984/lucene_udr/blob/main/sql/fts%24install_1.sql) 

Скачать готовые сборки можно по ссылкам:
* [LuceneUdr_Win_x64.zip](https://github.com/sim1984/lucene_udr/releases/download/1.0/LuceneUdr_Win_x64.zip)
* [LuceneUdr_Win_x86.zip](https://github.com/sim1984/lucene_udr/releases/download/1.0/LuceneUdr_Win_x86.zip)

Под ОС Linux вы можете скомпилировать библиотеку самостоятельно.

## Сборка и установка библиотеки под Linux

Поскольку Lucene UDR построена на основе [Lucene++](https://github.com/luceneplusplus/LucenePlusPlus) вам предварительно 
потребуется скачать и собрать её из исходников. 

```
$ git clone https://github.com/luceneplusplus/LucenePlusPlus.git
$ cd LucenePlusPlus
$ mkdir build; cd build
$ cmake ..
$ make
$ sudo make install
```

Чтобы lucene++ была установлена в `/usr/lib`, а не в `/usr/local/lib`, выполните `cmake -DCMAKE_INSTALL_PREFIX=/usr ..` вместо `cmake ..`

Более подробно сборка библиотеки lucene++ описана в [BUILDING.md](https://github.com/luceneplusplus/LucenePlusPlus/blob/master/doc/BUILDING.md).

Теперь можно приступать к сборке UDR Lucene.

```
$ git clone https://github.com/sim1984/lucene_udr.git
$ cd lucene_udr
$ mkdir build; cd build
$ cmake ..
$ make
$ sudo make install
```

В процессе выполнения `cmake ..` может возникнуть следующая ошибка

```
CMake Error at /usr/lib64/cmake/liblucene++/liblucene++Config.cmake:41 (message):
  File or directory /usr/lib64/usr/include/lucene++/ referenced by variable
  liblucene++_INCLUDE_DIRS does not exist !
Call Stack (most recent call first):
  /usr/lib64/cmake/liblucene++/liblucene++Config.cmake:47 (set_and_check)
  CMakeLists.txt:78 (find_package)
```

Для её исправления необходимо исправить файл `liblucene++Config.cmake` и `liblucene++-contribConfig.cmake` 

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

В этом файле задаётся путь к папке в которой будут создаваться полнотекстовые индексы для конкретной базы данных.

В качестве имени секции ini файла должен быть задан полный путь к базе данных или алиас (в зависимости от значения 
параметра `DatabaseAccess` в `firebird.conf`). Путь к директории полнотекстовых индексов указывается в ключе `ftsDirectory`. 

```ini
[horses]
ftsDirectory=f:\fbdata\3.0\fts\horses

[f:\fbdata\3.0\horses.fdb]
ftsDirectory=f:\fbdata\3.0\fts\horses
```

Важно: пользователь или группа, под которым выполняется служба Firebird, должен иметь права на чтение и запись для 
директории с полнотекстовыми индексами.

Получить расположение директории для полнотекстовых индексов можно с помощью запроса:

```sql
SELECT FTS$MANAGEMENT.FTS$GET_DIRECTORY() AS DIR_NAME
FROM RDB$DATABASE
```

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
Если параметр не задан, то будет использован анализатор STANDARD (для английского языка). Список доступных анализаторов 
можно узнать с помощью процедуры `FTS$MANAGEMENT.FTS$ANALYZERS`.

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

Пример ниже создаёт индекс `IDX_HORSE_REMARK` для таблицы `HORSE` с использованием анализатора `STANDARD`. 
Возвращается поле `CODE_HORSE`. Его имя было автоматически извлечено из первичного ключа таблицы `HORSE`.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_REMARK', 'HORSE');

COMMIT;
```

Следующий пример создаст индекс `IDX_HORSE_REMARK_RU` с использованием анализатора `RUSSIAN`.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_REMARK_RU', 'HORSE', 'RUSSIAN');

COMMIT;
```

Можно указать конкретное имя поля которое будет возвращено в качестве результата поиска.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_REMARK_2_RU', 'HORSE', 'RUSSIAN', 'CODE_HORSE');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_REMARK_DBKEY_RU', 'HORSE', 'RUSSIAN', 'RDB$DB_KEY');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_REMARK_UUID_RU', 'HORSE', 'RUSSIAN', 'UUID');

COMMIT;
```

### Добавление полей для индексирования

После создания индекса, необходимо добавить поля по которым будет производиться поиск с помощью
процедуры `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD`. Первым параметром указывается имя индекса, вторым имя добавляемого поля.
Третьим необязательным параметром можно указать множитель значимости для поля. По умолчанию значимость всех полей индекса одинакова и равна 1.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_REMARK_RU', 'REMARK');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_REMARK_DBKEY_RU', 'REMARK');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_REMARK_UUID_RU', 'REMARK');


EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_REMARK_2_RU', 'REMARK');
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_REMARK_2_RU', 'RUNTOTAL');

COMMIT;
```

В индексах `IDX_HORSE_REMARK_RU`, `IDX_HORSE_REMARK_DBKEY_RU` и `IDX_HORSE_REMARK_DBKEY_RU` обрабатывается одно поле `REMARK`, 
а в индексе `IDX_HORSE_REMARK_2_RU` - два поля `REMARK` и `RUNTOTAL`.

В следующем примере показано создание индекса с 2 полями `REMARK` и `RUNTOTAL`. Значимость поля `RUNTOTAL` в 4 раз выше значимости поля `REMARK`.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_REMARK_2X_RU', 'HORSE', 'RUSSIAN');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_REMARK_2X_RU', 'REMARK');
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_REMARK_2X_RU', 'RUNTOTAL', 4);

COMMIT;
```

### Построение индекса

Для построения индекса используется процедура `FTS$MANAGEMENT.FTS$REBUILD_INDEX()`. В качестве 
входного параметра необходимо указать имя полнотекстового индекса.

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_REMARK_RU');

COMMIT;

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_REMARK_DBKEY_RU');

COMMIT;

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_REMARK_UUID_RU');

COMMIT;

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_REMARK_2_RU');

COMMIT;

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_REMARK_2X_RU');

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
FROM FTS$SEARCH('IDX_HORSE_REMARK_RU', 'паспорт') 
```

Выходные параметры:

- FTS$RELATION_NAME - имя таблицы в которой найден документ;
- FTS$KEY_FIELD_NAME - имя ключевого поля в таблице;
- FTS$DB_KEY - значение ключевого поля в формате RDB$DB_KEY;
- FTS$ID - значение ключевого поля типа BIGINT или INTEGER;
- FTS$UUID - значение ключевого поля типа BINARY(16). Такой тип используется для хранения GUID;
- FTS$SCORE - степень соответствия поисковому запросу;
- FTS$EXPLANATION - объяснение результатов поиска.

Результат запроса будет доступен в одном из полей `FTS$DB_KEY`, `FTS$ID`, `FTS$UUID` в зависимости от того какое результирующие поле было указано при создании индекса.

Для извлечения данных из целевой таблицы достаточно просто выполнить с ней соединение условие которого зависит от того как создавался индекс.

Вот примеры различных вариантов соединения:

```sql
SELECT
    FTS.FTS$SCORE,
    HORSE.CODE_HORSE,
    HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK', 'паспорт') FTS
    JOIN HORSE ON
          HORSE.CODE_HORSE = FTS.FTS$ID;

SELECT
  FTS.FTS$SCORE,
  HORSE.UUID,
  HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK_UUID_RU', 'паспорт') FTS
JOIN HORSE ON HORSE.UUID = FTS.FTS$UUID;

SELECT
  FTS.FTS$SCORE,
  HORSE.CODE_HORSE,
  HORSE.RDB$DB_KEY,
  HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK_DBKEY_RU', 'паспорт') FTS
JOIN HORSE ON HORSE.RDB$DB_KEY = FTS.FTS$DB_KEY;
```

Для поиска сразу по двум полям используем индекс `IDX_HORSE_REMARK_2_RU`, в котором при создании были заданы поля `REMARK` и `RUNTOTAL`.

```sql
SELECT
  FTS.FTS$SCORE,
  HORSE.CODE_HORSE,
  HORSE.REMARK,
  HORSE.RUNTOTAL
FROM FTS$SEARCH('IDX_HORSE_REMARK_2_RU', 'паспорт') FTS
JOIN HORSE ON HORSE.CODE_HORSE = FTS.FTS$ID
```

Для объяснения результатов поиска, установите последний параметр в TRUE

```sql
SELECT
  FTS.FTS$SCORE,
  FTS.FTS$EXPLANATION,
  HORSE.CODE_HORSE,
  HORSE.REMARK,
  HORSE.RUNTOTAL
FROM FTS$SEARCH('IDX_HORSE_REMARK_2_RU', 'германия', 5, TRUE) FTS
JOIN HORSE ON HORSE.CODE_HORSE = FTS.FTS$ID
```

Поле `FTS$EXPLANATION` будет содержать объяснение результата.

```
2.92948 = (MATCH) sum of:
  1.33056 = (MATCH) weight(REMARK:герман in 22194), product of:
    0.673941 = queryWeight(REMARK:герман), product of:
      7.89718 = idf(docFreq=61, maxDocs=61348)
      0.0853394 = queryNorm
    1.9743 = (MATCH) fieldWeight(REMARK:герман in 22194), product of:
      1 = tf(termFreq(REMARK:герман)=1)
      7.89718 = idf(docFreq=61, maxDocs=61348)
      0.25 = fieldNorm(field=REMARK, doc=22194)
  1.59892 = (MATCH) weight(RUNTOTAL:герман in 22194), product of:
    0.738785 = queryWeight(RUNTOTAL:герман), product of:
      8.65702 = idf(docFreq=28, maxDocs=61348)
      0.0853394 = queryNorm
    2.16426 = (MATCH) fieldWeight(RUNTOTAL:герман in 22194), product of:
      1 = tf(termFreq(RUNTOTAL:герман)=1)
      8.65702 = idf(docFreq=28, maxDocs=61348)
      0.25 = fieldNorm(field=RUNTOTAL, doc=22194)

```

Для сравнения показано объяснение результатов поиска по индексу с полями у которых указан разный коэффициент значимости.

```sql
SELECT
  FTS.FTS$SCORE,
  FTS.FTS$EXPLANATION,
  HORSE.CODE_HORSE,
  HORSE.REMARK,
  HORSE.RUNTOTAL
FROM FTS$SEARCH('IDX_HORSE_REMARK_2X_RU', 'германия', 5, TRUE) FTS
JOIN HORSE ON HORSE.CODE_HORSE = FTS.FTS$ID
```

```
7.72624 = (MATCH) sum of:
  1.33056 = (MATCH) weight(REMARK:герман in 22194), product of:
    0.673941 = queryWeight(REMARK:герман), product of:
      7.89718 = idf(docFreq=61, maxDocs=61348)
      0.0853394 = queryNorm
    1.9743 = (MATCH) fieldWeight(REMARK:герман in 22194), product of:
      1 = tf(termFreq(REMARK:герман)=1)
      7.89718 = idf(docFreq=61, maxDocs=61348)
      0.25 = fieldNorm(field=REMARK, doc=22194)
  6.39568 = (MATCH) weight(RUNTOTAL:герман in 22194), product of:
    0.738785 = queryWeight(RUNTOTAL:герман), product of:
      8.65702 = idf(docFreq=28, maxDocs=61348)
      0.0853394 = queryNorm
    8.65702 = (MATCH) fieldWeight(RUNTOTAL:герман in 22194), product of:
      1 = tf(termFreq(RUNTOTAL:герман)=1)
      8.65702 = idf(docFreq=28, maxDocs=61348)
      1 = fieldNorm(field=RUNTOTAL, doc=22194)

```

## Синтаксис поисковых запросов

### Термы

Поисковые запросы (фразы поиска) состоят из термов и операторов. Lucene поддерживает простые и сложные термы. 
Простые термы состоят из одного слова, сложные из нескольких. Первые из них, это обычные слова, 
например, "привет", "тест". Второй же тип термов это группа слов, например, "Привет как дела". 
Несколько термов можно связывать вместе при помощи логических операторов.

### Поля

Lucene поддерживает поиск по нескольким полям. По умолчанию поиск осуществляется во всех полях полнотекстового индекса, 
выражение по каждому полю повторяется и соединяется оператором `OR`. Например, если у вас индекс содержащий 
поля `REMARK` и `RUNTOTAL`, то запрос

```
Привет мир
```

будет эквивалентен запросу

```
(REMARK: "Привет мир") OR (RUNTOTAL: "Привет мир")
```

Вы можете указать по какому полю вы хотите произвести поиск, для этого в запросе необходимо указать имя поля, символ двоеточия ":", 
после чего поисковую фразу для этого поля.

Пример поиска слова "Россия" в поле RUNTOTAL таблицы HORSE и слов "паспорт выдан" в поле REMARK таблицы HORSE:

```sql
SELECT
    FTS.FTS$SCORE
  , HORSE.CODE_HORSE
  , HORSE.REMARK
  , HORSE.RUNTOTAL
  , FTS.FTS$EXPLANATION
FROM FTS$SEARCH('IDX_HORSE_REMARK_2_RU', 'RUNTOTAL: (Россия) AND REMARK: (паспорт выдан)', 10, TRUE) FTS
JOIN HORSE
     ON HORSE.CODE_HORSE = FTS.FTS$ID 
```

### Маска

Lucene позволяет производить поиск документов по маске, используя в термах символы "?" и "\*". В этом случае символ "?" 
заменяет один любой символ, а "\*" - любое количество символов, например

```
"te?t" "test*" "tes*t"
```

Поисковый запрос нельзя начинать с символов "?" или "\*".

### Нечёткий поиск

Для выполнения нечёткого поиска в конец терма следует добавить тильду "~". В этом случае будут искаться все 
похожие слова, например при поиске "roam\~" будут также найдены слова "foam" и "roams".

### Усиление термов

Lucene позволяет изменять значимость термов во фразе поиска. Например, вы ищете фразу "Hello world" и хотите, 
чтобы слово «world» было более значимым. Значимость терма во фразе поиска можно увеличить, используя символ «ˆ», 
после которого указывается коэффициент усиления. В следующем примере значимость слова «world» в четыре раза больше 
значимости слова «Hello», которая по умолчанию равна единице.

```
"Hello worldˆ4"
```

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

#### Группировка логических операторов

Анализатор запросов Lucene поддерживает группировку логических операторов. Допустим, требуется найти либо слово "word", 
либо слово "dolly" и обязательно слово "hello", для этого используется такой запрос:

```
"Hello" && ("world" || "dolly")
```

### Экранирование специальных символов

Для включения специальных символов во фразу поиска выполняется их экранирование с помощью "\". 
Ниже приведен список специальных символов, используемых в Lucene на данный момент:

```
+ - && || ! ( ) { } [ ] ˆ " ˜ * ? : \
```

Фраза поиска для выражения "(1 + 1) : 2" будет иметь вид:

```
\( 1 \+ 1 \) \: 2
```

Для экранирования специальных символов вы можете воспользоваться функцией `FTS$ESCAPE_QUERY`.

```sql
  FTS$ESCAPE_QUERY('(1 + 1) : 2')
```

Более подробное англоязычное описание синтаксиса расположено на официальном сайте
Lucene: [https://lucene.apache.org](https://lucene.apache.org).

## Индексация представлений

Вы можете индексировать не только постоянные таблицы, но и сложные представления.

Для того чтобы индексировать представление должно быть соблюдено одно требование: 
в представлении должно быть поле, по которому вы можете однозначно идентифицировать запись.

Допустим у вас есть представление `V_FARM`, где `CODE_FARM` первичный ключ хозяйства:

```sql
CREATE OR ALTER VIEW V_FARM(
    CODE_FARM,
    CODE_COUNTRY,
    CODE_REGION,
    FARMNAME,
    COUNTRYNAME,
    REGIONNAME,
    ADDRESS,
    PHONE,
    FAX,
    EMAIL)
AS
SELECT
    FARM.CODE_FARM,
    FARM.CODE_COUNTRY,
    FARM.CODE_REGION,
    FARM.NAME AS FARMNAME,
    COUNTRY.NAME AS COUNTRYNAME,
    REGION.NAME AS REGIONNAME,
    FARM.ADDRESS,
    FARM.PHONE,
    FARM.FAX,
    FARM.EMAIL
FROM FARM
JOIN COUNTRY ON COUNTRY.CODE_COUNTRY = FARM.CODE_COUNTRY
JOIN REGION ON REGION.CODE_REGION = FARM.CODE_REGION
;
```

Вы хотите производить поиск по адресу хозяйства, но страна и регион находятся в справочных таблицах.
В этом случае можно создать следующий полнотекстовый индекс:

```sql
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_V_FARM_ADDRESS_RU', 'V_FARM', 'RUSSIAN', 'CODE_FARM');

COMMIT;

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_V_FARM_ADDRESS_RU', 'COUNTRYNAME');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_V_FARM_ADDRESS_RU', 'REGIONNAME');

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_V_FARM_ADDRESS_RU', 'ADDRESS');

COMMIT;

EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_V_FARM_ADDRESS_RU');

COMMIT;
```

Поиск хозяйства по адресу такого представления выглядит так:

```sql
SELECT
    FTS.FTS$SCORE
  , V_FARM.CODE_FARM
  , V_FARM.FARMNAME
  , V_FARM.COUNTRYNAME
  , V_FARM.REGIONNAME
  , V_FARM.ADDRESS
FROM FTS$SEARCH('IDX_V_FARM_ADDRESS_RU', 'Воронеж') FTS
JOIN V_FARM
     ON V_FARM.CODE_FARM = FTS.FTS$ID
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
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>')
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
    'Однажды в студёную зимнюю пору
    Я из лесу вышел был сильный мороз
    Гляжу поднимается медленно в гору
    Лошадка везущая хворосту воз',
    'сильный мороз',
    'RUSSIAN',
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
  CODE_HORSE TYPE OF COLUMN HORSE.CODE_HORSE,
  REMARK TYPE OF COLUMN HORSE.REMARK,
  RUNTOTAL TYPE OF COLUMN HORSE.RUNTOTAL,
  HIGHTLIGHT_REMARK VARCHAR(8191) CHARACTER SET UTF8,
  HIGHTLIGHT_RUNTOTAL VARCHAR(8191) CHARACTER SET UTF8
)
AS
BEGIN
  FOR
    SELECT
      FTS.FTS$SCORE,
      HORSE.CODE_HORSE,
      HORSE.REMARK,
      HORSE.RUNTOTAL,
      FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT(HORSE.REMARK, :FTS$QUERY, 'RUSSIAN', 'REMARK') AS HIGHTLIGHT_REMARK,
      FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT(HORSE.RUNTOTAL, :FTS$QUERY, 'RUSSIAN', 'RUNTOTAL') AS HIGHTLIGHT_RUNTOTAL
    FROM FTS$SEARCH('IDX_HORSE_REMARK_2_RU', :FTS$QUERY, 25) FTS
    JOIN HORSE ON HORSE.CODE_HORSE = FTS.FTS$ID
  INTO
    FTS$SCORE,
    CODE_HORSE,
    REMARK,
    RUNTOTAL,
    HIGHTLIGHT_REMARK,
    HIGHTLIGHT_RUNTOTAL
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
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>')
  RETURNS (
      FTS$FRAGMENT VARCHAR(8191) CHARACTER SET UTF8);
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
FROM BOOKS
LEFT JOIN FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS(
  BOOKS.CONTENT,
  'толстый',
  'RUSSIAN'
) F ON TRUE
WHERE BOOKS.ID = 8
```


## Поддержание актуальности данных в полнотекстовых индексах

Для поддержки актуальности полнотекстовых индексов существует несколько способов:

1. Периодически вызывать процедуру `FTS$MANAGEMENT.FTS$REBUILD_INDEX` для заданного индекса. 
Этот способ полностью перестраивает полнотекстовый индекс. В этом случае читаются все записи таблицы или представления 
для которой создан индекс.

2. Поддерживать полнотекстовые индексы можно с помощью триггеров и вызова внутри них одной из процедур `FTS$LOG_BY_ID`,
`FTS$LOG_BY_UUID` или `FTS$LOG_BY_DBKEY`. Какую из процедур вызывать 
зависит от того какой тип поля выбран в качестве ключевого (целочисленный, UUID (GIUD) или RDB$DB_KEY).
При вызове этих процедур запись об изменении добавляется в специальную таблицу `FTS$LOG` (журнал изменений).
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
от `NULL`. Если это условие соблюдается, то необходимо выполнить одну из процедур 
`FTS$LOG_BY_DBKEY('<имя таблицы>', NEW.RDB$DB_KEY, 'I');` или `FTS$LOG_BY_ID('<имя таблицы>', NEW.<ключевое поле>, 'I')`
или `FTS$LOG_BY_UUID('<имя таблицы>', NEW.<ключевое поле>, 'I')`.

3. Для операции `UPDATE` необходимо проверять все поля, входящие в полнотекстовые индексы значение которых изменилось.
Если это условие соблюдается, то необходимо выполнить процедуру `FTS$LOG_BY_DBKEY('<имя таблицы>', OLD.RDB$DB_KEY, 'U');`
или `FTS$LOG_BY_ID('<имя таблицы>', OLD.<ключевое поле>, 'U')` или `FTS$LOG_BY_UUID('<имя таблицы>', OLD.<ключевое поле>, 'U')`.

4. Для операции `DELETE` необходимо проверять все поля, входящие в полнотекстовые индексы значение которых отличается 
от `NULL`. Если это условие соблюдается, то необходимо выполнить процедуру 
`FTS$LOG_CHANGE('<имя таблицы>', OLD.RDB$DB_KEY, 'D');`.

Для облегчения задачи написания таких триггеров существует специальный пакет `FTS$TRIGGER_HELPER`, в котором 
расположены процедуры генерирования исходных текстов триггеров. Так например, для того чтобы сгенерировать триггеры 
для поддержки полнотекстовых индексов созданных для таблицы `HORSE`, необходимо выполнить следующий запрос:

```sql
SELECT
    FTS$TRIGGER_SCRIPT
FROM FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS('HORSE', TRUE)
```

Этот запрос вернёт следующий текст триггера для всех созданных FTS индексов на таблице `HORSE`:

```sql
CREATE OR ALTER TRIGGER FTS$HORSE_AIUD FOR HORSE
ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 100
AS
BEGIN
  /* Block for key CODE_HORSE */
  IF (INSERTING AND (NEW.REMARK IS NOT NULL
      OR NEW.RUNTOTAL IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_ID('HORSE', NEW.CODE_HORSE, 'I');
  IF (UPDATING AND (NEW.REMARK IS DISTINCT FROM OLD.REMARK
      OR NEW.RUNTOTAL IS DISTINCT FROM OLD.RUNTOTAL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_ID('HORSE', OLD.CODE_HORSE, 'U');
  IF (DELETING AND (OLD.REMARK IS NOT NULL
      OR OLD.RUNTOTAL IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_ID('HORSE', OLD.CODE_HORSE, 'D');
  /* Block for key RDB$DB_KEY */
  IF (INSERTING AND (NEW.REMARK IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_DBKEY('HORSE', NEW.RDB$DB_KEY, 'I');
  IF (UPDATING AND (NEW.REMARK IS DISTINCT FROM OLD.REMARK)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_DBKEY('HORSE', OLD.RDB$DB_KEY, 'U');
  IF (DELETING AND (OLD.REMARK IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_DBKEY('HORSE', OLD.RDB$DB_KEY, 'D');
  /* Block for key UUID */
  IF (INSERTING AND (NEW.REMARK IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_UUID('HORSE', NEW.UUID, 'I');
  IF (UPDATING AND (NEW.REMARK IS DISTINCT FROM OLD.REMARK)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_UUID('HORSE', OLD.UUID, 'U');
  IF (DELETING AND (OLD.REMARK IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_UUID('HORSE', OLD.UUID, 'D');
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
`V_FARM`.

```sql
SET TERM ^;

-- Field ADDRESS from table FARM
CREATE OR ALTER TRIGGER FTS$V_FARM_AIUD_1 FOR FARM
ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 100
AS
BEGIN
  IF (INSERTING AND (NEW.ADDRESS IS NOT NULL
                  OR NEW.CODE_COUNTRY IS NOT NULL
                  OR NEW.CODE_REGION IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_ID('V_FARM', NEW.CODE_FARM, 'I');
  IF (UPDATING AND (NEW.ADDRESS IS DISTINCT FROM OLD.ADDRESS
                 OR NEW.CODE_COUNTRY IS DISTINCT FROM OLD.CODE_COUNTRY
                 OR NEW.CODE_REGION IS DISTINCT FROM OLD.CODE_REGION)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_ID('V_FARM', OLD.CODE_FARM, 'U');
  IF (DELETING AND (OLD.ADDRESS IS NOT NULL
                 OR OLD.CODE_COUNTRY IS NOT NULL
                 OR OLD.CODE_REGION IS NOT NULL)) THEN
    EXECUTE PROCEDURE FTS$LOG_BY_ID('V_FARM', OLD.CODE_FARM, 'D');
END
^

-- Field COUNTRYNAME from table COUNTRY (COUNTRY.NAME)
CREATE OR ALTER TRIGGER FTS$V_FARM_AU_2 FOR COUNTRY
ACTIVE AFTER UPDATE POSITION 100
AS
DECLARE CODE_FARM INTEGER;
BEGIN
  IF (NEW.NAME IS DISTINCT FROM OLD.NAME) THEN
  BEGIN
    FOR
      SELECT CODE_FARM
      FROM FARM
      WHERE CODE_COUNTRY = OLD.CODE_COUNTRY
      INTO :CODE_FARM
    DO
      EXECUTE PROCEDURE FTS$LOG_BY_ID('V_FARM', :CODE_FARM, 'U');
  END
END
^

-- Field REGIONNAME from table REGION (REGION.NAME)
CREATE OR ALTER TRIGGER FTS$V_FARM_AU_3 FOR REGION
ACTIVE AFTER UPDATE POSITION 100
AS
DECLARE CODE_FARM INTEGER;
BEGIN
  IF (NEW.NAME IS DISTINCT FROM OLD.NAME) THEN
  BEGIN
    FOR
      SELECT CODE_FARM
      FROM FARM
      WHERE CODE_REGION = OLD.CODE_REGION
      INTO :CODE_FARM
    DO
      EXECUTE PROCEDURE FTS$LOG_BY_ID('V_FARM', :CODE_FARM, 'U');
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

Функция `FTS$MANAGEMENT.FTS$GET_DIRECTORY` возвращает директорию в которой расположены файлы и папки полнотекстового индекса для текущей базы данных.

```sql
  FUNCTION FTS$GET_DIRECTORY ()
  RETURNS VARCHAR(255) CHARACTER SET UTF8
  DETERMINISTIC;
```

#### Процедура FTS$MANAGEMENT.FTS$ANALYZERS

Процедура `FTS$MANAGEMENT.FTS$ANALYZERS` возвращает список доступных анализаторов.

```sql
  PROCEDURE FTS$ANALYZERS
  RETURNS (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8);
```

Выходные параметры:

- FTS$ANALYZER - имя анализатора.

#### Процедура FTS$MANAGEMENT.FTS$CREATE_INDEX

Процедура `FTS$MANAGEMENT.FTS$CREATE_INDEX` создаёт новый полнотекстовый индекс. 

```sql
  PROCEDURE FTS$CREATE_INDEX (
      FTS$INDEX_NAME     VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$ANALYZER       VARCHAR(63) CHARACTER SET UTF8 DEFAULT 'STANDARD',
      FTS$KEY_FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
      FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8 DEFAULT NULL);
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса. Должно быть уникальным среди имён полнотекстовых индексов;
- FTS$RELATION_NAME - имя таблицы, которая должна быть проиндексирована;
- FTS$ANALYZER - имя анализатора. Если не задано используется анализатор STANDARD (StandardAnalyzer);
- FTS$KEY_FIELD_NAME - имя поля значение которого будет возращено процедурой поиска `FTS$SEARCH()`, обычно это ключевое поле таблицы;
- FTS$DESCRIPTION - описание индекса.

#### Процедура FTS$MANAGEMENT.FTS$DROP_INDEX

Процедура `FTS$MANAGEMENT.FTS$DROP_INDEX` удаляет полнотекстовый индекс.

```sql
  PROCEDURE FTS$DROP_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса.

#### Процедура FTS$MANAGEMENT.SET_INDEX_ACTIVE

Процедура `FTS$MANAGEMENT.SET_INDEX_ACTIVE` позволяет сделать индекс активным или неактивным. 

```sql
  PROCEDURE FTS$SET_INDEX_ACTIVE (
      FTS$INDEX_NAME   VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$INDEX_ACTIVE BOOLEAN NOT NULL);
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса;
- FTS$INDEX_ACTIVE - флаг активности.

#### Процедура FTS$MANAGEMENT.FTS$COMMENT_ON_INDEX

Процедура `FTS$MANAGEMENT.FTS$COMMENT_ON_INDEX` добавляет или удаляет пользовательский комментарий к индексу.

```sql
  PROCEDURE FTS$COMMENT_ON_INDEX (
      FTS$INDEX_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8);
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
      FTS$BOOST         DOUBLE PRECISION DEFAULT NULL);
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
      FTS$FIELD_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
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
      FTS$BOOST DOUBLE PRECISION);
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
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
```

Входные параметры:

- FTS$INDEX_NAME - имя индекса.

#### Процедура FTS$MANAGEMENT.FTS$REINDEX_TABLE

Процедура `FTS$MANAGEMENT.FTS$REINDEX_TABLE` перестраивает все полнотекстовые индексы для указанной таблицы.

```sql
  PROCEDURE FTS$REINDEX_TABLE (
      FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL);
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
- FTS$DB_KEY - значение ключевого поля в формате RDB$DB_KEY;
- FTS$ID - значение ключевого поля типа BIGINT или INTEGER;
- FTS$UUID - значение ключевого поля типа BINARY(16). Такой тип используется для хранения GUID;
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

- FTS$QUERY - поисквоый запрос или его часть, в котором необходимо экранировать специальные символы.

### Процедура FTS$LOG_BY_ID

Процедура `FTS$LOG_BY_ID` добавляет запись об изменении одного из полей входящих в полнотекстовые индексы, 
построенные на таблице, в журнал изменений `FTS$LOG`, на основе которого будут обновляться полнотекстовые индексы.
Эту процедуру следует применять если в качестве первичного ключа используется целочисленное поле. Такие ключи
часто генерируются с помощью генераторов/последовательностей.

```sql
PROCEDURE FTS$LOG_BY_ID (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$ID            BIGINT NOT NULL,
    FTS$CHANGE_TYPE   FTS$D_CHANGE_TYPE NOT NULL
)
```

Входные параметры:

- FTS$RELATION_NAME - имя таблицы для которой добавляется запись об изменении;
- FTS$ID - значение ключевого поля;
- FTS$CHANGE_TYPE - тип изменения (I - INSERT, U - UPDATE, D - DELETE).


### Процедура FTS$LOG_BY_UUID

Процедура `FTS$LOG_BY_UUID` добавляет запись об изменении одного из полей входящих в полнотекстовые индексы, 
построенные на таблице, в журнал изменений `FTS$LOG`, на основе которого будут обновляться полнотекстовые индексы.
Эту процедуру следует применять если в качестве первичного ключа используется UUID (GUID). Такие ключи
часто генерируются с помощью функции `GEN_UUID`. 

```sql
PROCEDURE FTS$LOG_BY_UUID (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$UUID          CHAR(16) CHARACTER SET OCTETS NOT NULL,
    FTS$CHANGE_TYPE   FTS$D_CHANGE_TYPE NOT NULL
)
```

Входные параметры:

- FTS$RELATION_NAME - имя таблицы для которой добавляется запись об изменении;
- FTS$UUID - значение ключевого поля;
- FTS$CHANGE_TYPE - тип изменения (I - INSERT, U - UPDATE, D - DELETE).


### Процедура FTS$LOG_BY_DBKEY

Процедура `FTS$LOG_BY_DBKEY` добавляет запись об изменении одного из полей входящих в полнотекстовые индексы, 
построенные на таблице, в журнал изменений `FTS$LOG`, на основе которого будут обновляться полнотекстовые индексы.
Эту процедуру следует применять если в качестве первичного ключа используется псевдо поле `RDB$DB_KEY`. 

```sql
PROCEDURE FTS$LOG_BY_DBKEY (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$DBKEY         CHAR(8) CHARACTER SET OCTETS NOT NULL,
    FTS$CHANGE_TYPE   FTS$D_CHANGE_TYPE NOT NULL
)
```

Входные параметры:

- FTS$RELATION_NAME - имя таблицы для которой добавляется запись об изменении;
- FTS$DBKEY - значение псевдо поля `RDB$DB_KEY`;
- FTS$CHANGE_TYPE - тип изменения (I - INSERT, U - UPDATE, D - DELETE).


### Процедура FTS$CLEAR_LOG

Процедура `FTS$CLEAR_LOG` очищает журнал изменений `FTS$LOG`, на основе которого обновляются полнотекстовые индексы.

### Процедура FTS$UPDATE_INDEXES

Процедура `FTS$UPDATE_INDEXES` обновляет полнотекстовые индексы по записям в журнале изменений `FTS$LOG`. 
Эта процедура обычно запускается по расписанию (cron) в отдельной сессии с некоторым интервалом, например 5 секунд.

### Пакет FTS$HIGHLIGHTER

Пакет `FTS$HIGHLIGHTER` содержит процедуры и функции возвращающие фрагменты текста, в котором найдена исходная фраза, 
и выделяет найденные слова.

#### Функция FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT

Функция `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENT` возвращает лучший фрагмент текста, который соответствует выражению полнотекстового поиска,
и выделяет в нем найденные слова.

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

Входные параметры:

- FTS$TEXT - текст, в котором ищется фраза;
- FTS$QUERY - выражение полнотекстового поиска;
- FTS$ANALYZER - анализатор;
- FTS$FIELD_NAME — имя поля, в котором выполняется поиск;
- FTS$FRAGMENT_SIZE - длина возвращаемого фрагмента. Не меньше, чем требуется для возврата целых слов;
- FTS$LEFT_TAG - левый тег для выделения;
- FTS$RIGHT_TAG - правильный тег для выделения. 

#### Процедура FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS

Процедура `FTS$HIGHLIGHTER.FTS$BEST_FRAGMENTS` возвращает лучшие фрагменты текста, которые соответствуют выражению полнотекстового поиска,
и выделяет в них найденные слова.

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

Входные параметры:

- FTS$TEXT - текст, в котором ищется фраза;
- FTS$QUERY - выражение полнотекстового поиска;
- FTS$ANALYZER - анализатор;
- FTS$FIELD_NAME — имя поля, в котором выполняется поиск;
- FTS$FRAGMENT_SIZE - длина возвращаемого фрагмента. Не меньше, чем требуется для возврата целых слов;
- FTS$MAX_NUM_FRAGMENTS - максимальное количество фрагментов;
- FTS$LEFT_TAG - левый тег для выделения;
- FTS$RIGHT_TAG - правильный тег для выделения. 

Выходные параметры:

- FTS$FRAGMENT - фрагмент текста, соответствующий поисковой фразе.


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
- FTS$TRIGGER_SCRIPT - скрипт создания. 

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

Функция `FTS$STATISTICS.FTS$GET_DIRECTORY` возвращает директорию в которой расположены файлы и папки полнотекстового индекса для 
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
      FTS$INDEX_SIZE       INTEGER);
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
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  RETURNS (
      FTS$FIELD_NAME VARCHAR(127) CHARACTER SET UTF8);
```
   
Входные параметры:

- FTS$INDEX_NAME - имя индекса.
   
Выходные параметры:

- FTS$FIELD_NAME - имя поля.


#### Процедура FTS$STATISTICS.FTS$INDEX_FILES

Процедура `FTS$STATISTICS.FTS$INDEX_FILES` возвращает информацию об индексных файлах.

```sql
  PROCEDURE FTS$INDEX_FILES (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  RETURNS (
      FTS$FILE_NAME VARCHAR(127) CHARACTER SET UTF8,
      FTS$FILE_TYPE VARCHAR(63) CHARACTER SET UTF8,
      FTS$FILE_SIZE INTEGER);
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
   
Входные параметры:

- FTS$INDEX_NAME - название индекса;
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





