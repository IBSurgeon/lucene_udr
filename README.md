# Lucene UDR

� Firebird ���������� ���������� ���������� ��������������� ������. ���������� Lucene UDR ���������
��������� � ������� ��������������� ������ � ������� ������� �� �������� ���������������� ���������� Lucene. ������������ ��������� ������
Lucene ������� �� ����� Java. � ��������� ������ FB Java ��� ��������� ������� �������� �������� � �������
���� ��� � ������ Beta ������. ������� Lucene UDR ���������� ���� Lucene �� ���� C++  
[Lucene++](https://github.com/luceneplusplus/LucenePlusPlus). Lucene++ ���� ����� �������, ��� ������������ ������ Lucene,
�� �������� ������� �������� �������������.

## ��������� Lucene UDR

��� ��������� Lucene UDR ����������:

1. ����������� zip ����� � ������������� ������������ � ������� `plugins\udr`
2. ��������� ������ fts$install.sql ��� ���������� �������� � �������. � ������������� ��.

������� ������� ������ ����� �� �������:
* [LuceneUdr_Win_x64.zip](https://github.com/sim1984/lucene_udr/releases/download/1.0/LuceneUdr_Win_x64.zip)

� ��������� ������ ������ ������ ���.

## �������� �������� � ������� ��� ������ � �������������� �������

### ����� FTS$MANAGEMENT

����� `FTS$MANAGEMENT` �������� ��������� � ������� ��� ���������� ��������������� ���������. ���� ����� ������������
��� ��������������� ���� ������.

��������� ����� ������ �������� ��������� �������:

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

#### ������� FTS$GET_DIRECTORY

������� `FTS$GET_DIRECTORY()` ���������� ���������� � ������� ��������� ����� � ����� ��������������� ������� ��� ������� ���� ������.

#### ��������� FTS$CREATE_INDEX

��������� `FTS$CREATE_INDEX()` ������ ����� �������������� ������. 

������� ���������:

- FTS$INDEX_NAME - ��� �������. ������ ���� ���������� ����� ��� �������������� ��������;
- FTS$ANALYZER - ��� �����������. ���� �� ������ ������������ ���������� STANDART (StandartAnalyzer);
- FTS$DESCRIPTION - �������� �������.

���������: � ��������� ����� FTS$ANALYZER �� �����������. ��� ����������� ����� ��������� �����.

#### ��������� FTS$DROP_INDEX

��������� `FTS$DROP_INDEX()` ������� �������������� ������. 

������� ���������:

- FTS$INDEX_NAME - ��� �������.

#### ��������� FTS$ADD_INDEX_FIELD

��������� `FTS$ADD_INDEX_FIELD()` ��������� ����� ������� (������������� ���� �������) ��������������� �������. 

������� ���������:

- FTS$INDEX_NAME - ��� �������;
- FTS$RELATION_NAME - ��� �������, ������ ������ ���� ���������������;
- FTS$FIELD_NAME - ��� ����, ������� ������ ���� ����������������.

#### ��������� FTS$DROP_INDEX_FIELD

��������� `FTS$DROP_INDEX_FIELD()` ������� ������� (������������� ���� �������) ��������������� �������. 

������� ���������:

- FTS$INDEX_NAME - ��� �������;
- FTS$RELATION_NAME - ��� �������;
- FTS$FIELD_NAME - ��� ����.

#### ��������� FTS$REBUILD_INDEX

��������� `FTS$REBUILD_INDEX()` ������������� �������������� ������. 

������� ���������:

- FTS$INDEX_NAME - ��� �������.

### ��������� FTS$SEARCH

��������� `FTS$SEARCH` ������������ �������������� ����� �� ��������� �������.

������� ���������:

- FTS$INDEX_NAME - ��� ��������������� �������, � ������� �������������� �����;
- FTS$RELATION_NAME - ��� �������, ������������ ����� ������ �������� ��������. ���� ������� �� ������, �� ����� �������� �� ���� ��������� �������;
- FTS$FILTER - ��������� ��������������� ������.

�������� ���������:

- RDB$RELATION_NAME - ��� ������� � ������� ����� ��������;
- FTS$DB_KEY - ������ �� ������ � ������� � ������� ��� ������ �������� (������������� ������ ���� RDB$DB_KEY);
- FTS$SCORE - ������� ����������� ���������� �������.

## �������� ������������� �������� � ����� �� ���

����� �������������� ��������������� ������ � ����� ���� ������ ���������� ���������� ��������������� ���������.
��������� Lucene UDR ��������� � ����� `$(root)\fts.ini`. ���� ����� ����� ���, �� �������� ��� ��������������.

� ���� ����� ������� ���� � ����� � ������ ����� ����������� �������������� ������� ��� ��������� ���� ������.

� �������� ����� ������� ini ����� ������ ���� ����� ������ ���� � ���� ������ ��� ����� (� ���������� �� �������� ��������� `DatabaseAccess` � `firebird.conf`
���� � ���������� �������������� �������� ����������� � ����� `ftsDirectory`. 

```ini
[horses]
ftsDirectory=f:\fbdata\3.0\fts\horses

[f:\fbdata\3.0\horses.fdb]
ftsDirectory=f:\fbdata\3.0\fts\horses
```

�����: ������������ ��� ������, ��� ������� ����������� ������ Firebird, ������ ����� ����� �� ������ � ������ ��� ���������� � ��������������� ���������.

�������� ������������ ��������� ��� �������������� �������� ����� � ������� �������:

```sql
SELECT FTS$MANAGEMENT.FTS$GET_DIRECTORY() AS DIR_NAME
FROM RDB$DATABASE
```

��� �������� ��������������� ������� ���������� ��������� ��������������� ��� ����:

1. �������� ������� � ������ ��������� `FTS$MANAGEMENT.FTS$CREATE_INDEX()`. �� ����
���� ������� ��� �������, ������������ ���������� � �������� �������.

2. ���������� ��������� ������� � ������� ��������� `FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD()`.
��� ��������� ������� ���������� ��� ������� � ��� ��������������� ����. 
���� �������������� ������ ����� ������������� ����� ��������� ������.

3. ���������� ������� � ������� ��������� `FTS$MANAGEMENT.FTS$REBUILD_INDEX()`.
�� ���� ����� �������� ��� �������, ������� ���� ������� � ��������� ������� � ���������� �����, ��������� � ��������
�������������� � �������� � ������. ��� ���������� ������, ����� ������� ��������� ������������ � ���������� ��������� ����������.

����� ����� ��������� ������� �������������� ��������.

### ������ ��������������� ��������������� �������

```sql
-- �������� �������
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_REMARK');

COMMIT;

-- ���������� �������� (���� REMARK ������� HORSE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_REMARK', 'HORSE', 'REMARK');

COMMIT;

-- ���������� �������
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_REMARK');

COMMIT;
```

����� �� ������ ������� ����� ������ ��������� �������:

```
SELECT
    FTS.*,
    HORSE.CODE_HORSE,
    HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK', 'HORSE', '�������') FTS
    LEFT JOIN HORSE ON
          HORSE.RDB$DB_KEY = FTS.FTS$DB_KEY  

```

� �������� ������� ��������� ����� ������� NULL, ��������� ������ ������������ ����� ���� �������.


### ������ ��������������� ������� � ����� ������ ����� �������

```sql
-- �������� �������
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_NOTES');

COMMIT;

-- ���������� �������� (���� REMARK ������� NOTE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_NOTES', 'NOTE', 'REMARK');
-- ���������� �������� (���� REMARK_EN ������� NOTE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_NOTES', 'NOTE', 'REMARK_EN');

COMMIT;

-- ���������� �������
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_NOTES');

COMMIT;
```

����� �� ������ ������� ����� ������ ��������� �������:

```sql
SELECT
    FTS.*,
    NOTE.CODE_HORSE,
    NOTE.CODE_NOTETYPE,
    NOTE.REMARK,
    NOTE.REMARK_EN
FROM FTS$SEARCH('IDX_HORSE_NOTE', 'NOTE', '�������') FTS
    LEFT JOIN NOTE ON
          NOTE.RDB$DB_KEY = FTS.FTS$DB_KEY  
```

### ������ ��������������� ������� � ����� ���������

```sql
-- �������� �������
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$CREATE_INDEX('IDX_HORSE_NOTE_2');;

COMMIT;

-- ���������� �������� (���� REMARK ������� NOTE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_NOTE_2', 'NOTE', 'REMARK');
-- ���������� �������� (���� REMARK ������� HORSE)
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$ADD_INDEX_FIELD('IDX_HORSE_NOTE_2', 'HORSE', 'REMARK');

COMMIT;

-- ���������� �������
EXECUTE PROCEDURE FTS$MANAGEMENT.FTS$REBUILD_INDEX('IDX_HORSE_NOTE_2');

COMMIT;
```

����� �� ������ ������� ����� ������ ��������� �������:


```sql
-- ����� � ������� NOTE
SELECT
    FTS.*,
    NOTE.CODE_HORSE,
    NOTE.CODE_NOTETYPE,
    NOTE.REMARK,
    NOTE.REMARK_EN
FROM FTS$SEARCH('IDX_HORSE_NOTE_2', 'NOTE', '�������') FTS
    LEFT JOIN NOTE ON
          NOTE.RDB$DB_KEY = FTS.FTS$DB_KEY 
  
-- ����� � ������� HORSE          
SELECT
    FTS.*,
    HORSE.CODE_HORSE,
    HORSE.REMARK
FROM FTS$SEARCH('IDX_HORSE_REMARK', 'HORSE', '�������') FTS
    LEFT JOIN HORSE ON
          HORSE.RDB$DB_KEY = FTS.FTS$DB_KEY  

-- ������ ��� ������� � DB_KEY �� ���� ������
SELECT
    FTS.*
FROM FTS$SEARCH('IDX_HORSE_NOTE_2', NULL, '�������') FTS

-- ��������� ������� �� ���� ������
SELECT
    FTS.*,
    COALESCE(HORSE.CODE_HORSE, NOTE.CODE_HORSE) AS CODE_HORSE,
    HORSE.REMARK AS HORSEREMARK,
    NOTE.REMARK AS NOTEREMARK
FROM FTS$SEARCH('IDX_HORSE_NOTE_2', NULL, '�������') FTS
    LEFT JOIN HORSE ON
          HORSE.RDB$DB_KEY = FTS.FTS$DB_KEY AND
          FTS.RDB$RELATION_NAME = 'HORSE'
    LEFT JOIN NOTE ON
          NOTE.RDB$DB_KEY = FTS.FTS$DB_KEY AND
          FTS.RDB$RELATION_NAME = 'NOTE'
```
