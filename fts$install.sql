CREATE TABLE FTS$INDICES(
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
   FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
   CONSTRAINT PK_FTS$INDEX_NAME PRIMARY KEY(FTS$INDEX_NAME)
);

COMMENT ON TABLE FTS$INDICES IS
'Indexes for full-text search.';

COMMENT ON COLUMN FTS$INDICES.FTS$INDEX_NAME IS
'Full-text index name.';

COMMENT ON COLUMN FTS$INDICES.FTS$ANALYZER IS
'The analyzer. If not specified, it uses STANDART (StandartAnalyzer) by default.';

COMMENT ON COLUMN FTS$INDICES.FTS$DESCRIPTION IS
'Description of the full-text index.';

CREATE TABLE FTS$INDEX_SEGMENTS(
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   CONSTRAINT UK_FTS$INDEX_SEGMENTS UNIQUE(FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$FIELD_NAME),
   CONSTRAINT FK_FTS$INDEX_SEGMENTS FOREIGN KEY(FTS$INDEX_NAME) REFERENCES FTS$INDICES(FTS$INDEX_NAME)
);

CREATE INDEX IDX_FTS$INDEX_SEGMENTS_REL
ON FTS$INDEX_SEGMENTS (FTS$RELATION_NAME);


COMMENT ON TABLE FTS$INDEX_SEGMENTS IS
'Segments of the full-text index.';

COMMENT ON COLUMN FTS$INDEX_SEGMENTS.FTS$INDEX_NAME IS
'Full-text index name.';

COMMENT ON COLUMN FTS$INDEX_SEGMENTS.FTS$RELATION_NAME IS
'Name of the indexed table.';

COMMENT ON COLUMN FTS$INDEX_SEGMENTS.FTS$FIELD_NAME IS
'Name of the indexed field.';

CREATE DOMAIN FTS$CHANGE_TYPE
CHAR(1) CHARACTER SET UTF8
CHECK (VALUE IN ('I', 'U', 'D'))
COLLATE UNICODE_CI;

COMMENT ON DOMAIN FTS$CHANGE_TYPE IS
'Type of record change.';

CREATE TABLE FTS$LOG (
  ID BIGINT /*INT*/ GENERATED BY DEFAULT AS IDENTITY,
  RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
  DB_KEY CHAR(8) CHARACTER SET OCTETS NOT NULL,
  CHANGE_TYPE FTS$CHANGE_TYPE NOT NULL,
  CONSTRAINT PK_FTS$LOG_ID PRIMARY KEY(ID)
);

COMMENT ON TABLE FTS$LOG IS
'Changelog for maintaining full-text indexes.';

COMMENT ON COLUMN FTS$LOG.ID IS
'Identifier.';

COMMENT ON COLUMN FTS$LOG.RELATION_NAME IS
'Name of the indexed table.';

COMMENT ON COLUMN FTS$LOG.DB_KEY IS
'Link to indexed record.';

COMMENT ON COLUMN FTS$LOG.CHANGE_TYPE IS
'Type of record change.';

SET TERM ^ ;

CREATE OR ALTER PACKAGE FTS$MANAGEMENT
AS
BEGIN
  FUNCTION FTS$GET_DIRECTORY ()
  RETURNS VARCHAR(255) CHARACTER SET UTF8;

  PROCEDURE FTS$ANALYZERS 
  RETURNS (
     FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8
  );

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
END^

RECREATE PACKAGE BODY FTS$MANAGEMENT
AS
BEGIN
  FUNCTION FTS$GET_DIRECTORY ()
  RETURNS VARCHAR(255) CHARACTER SET UTF8
  EXTERNAL NAME 'luceneudr!getFTSDirectory'
  ENGINE UDR;

  PROCEDURE FTS$ANALYZERS 
  RETURNS (
     FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8
  )
  EXTERNAL NAME 'luceneudr!getAnalyzers'
  ENGINE UDR;

  PROCEDURE FTS$CREATE_INDEX (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
     FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
     FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  )
  EXTERNAL NAME 'luceneudr!createIndex'
  ENGINE UDR;

  PROCEDURE FTS$DROP_INDEX (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  EXTERNAL NAME 'luceneudr!dropIndex'
  ENGINE UDR;

  PROCEDURE FTS$ADD_INDEX_FIELD (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  EXTERNAL NAME 'luceneudr!addIndexField'
  ENGINE UDR;

  PROCEDURE FTS$DROP_INDEX_FIELD (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  EXTERNAL NAME 'luceneudr!dropIndexField'
  ENGINE UDR;

  PROCEDURE FTS$REBUILD_INDEX (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  EXTERNAL NAME 'luceneudr!rebuildIndex'
  ENGINE UDR;
END^

SET TERM ; ^

COMMENT ON PACKAGE FTS$MANAGEMENT IS
'Procedures and functions for managing full-text indexes.';

GRANT ALL ON TABLE FTS$INDICES TO PACKAGE FTS$MANAGEMENT;
GRANT ALL ON TABLE FTS$INDEX_SEGMENTS TO PACKAGE FTS$MANAGEMENT;

CREATE OR ALTER PROCEDURE FTS$SEARCH (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
   FTS$FILTER VARCHAR(8191) CHARACTER SET UTF8,
   FTS$LIMIT BIGINT /*INT*/ NOT NULL DEFAULT 1000
)
RETURNS (
   RDB$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
   FTS$DB_KEY CHAR(8) CHARACTER SET OCTETS,
   FTS$SCORE DOUBLE PRECISION
)
EXTERNAL NAME 'luceneudr!ftsSearch'
ENGINE UDR;

COMMENT ON PROCEDURE FTS$SEARCH IS
'Performs a full-text search at the specified index.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$INDEX_NAME IS
'Name of the full-text index to search.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$RELATION_NAME IS
'The name of the table, restricts the search to the specified table only. If the table is not specified, then the search is done on all index segments.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$FILTER IS
'Full text search expression.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$LIMIT IS
'Limit on the number of records (search result).';

COMMENT ON PARAMETER FTS$SEARCH.RDB$RELATION_NAME IS
'The name of the table in which the document is found.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$DB_KEY IS
'Reference to the record in the table where the document was found (corresponds to the RDB$DB_KEY pseudo field).';

COMMENT ON PARAMETER FTS$SEARCH.FTS$SCORE IS
'The degree of match to the search query.';

GRANT SELECT ON TABLE FTS$INDICES TO PROCEDURE FTS$SEARCH;
GRANT SELECT ON TABLE FTS$INDEX_SEGMENTS TO PROCEDURE FTS$SEARCH;

CREATE OR ALTER PROCEDURE FTS$UPDATE_INDEXES
EXTERNAL NAME 'luceneudr!updateFtsIndexes' 
ENGINE UDR;

COMMENT ON PROCEDURE FTS$UPDATE_INDEXES IS
'Updates full-text indexes on entries in the FTS$LOG change log.';

GRANT SELECT ON TABLE FTS$INDICES TO PROCEDURE FTS$UPDATE_INDEXES;
GRANT SELECT ON TABLE FTS$INDEX_SEGMENTS TO PROCEDURE FTS$UPDATE_INDEXES;
GRANT SELECT, DELETE ON TABLE FTS$LOG TO PROCEDURE FTS$UPDATE_INDEXES;

CREATE PROCEDURE FTS$LOG_CHANGE (
    RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	DB_KEY         CHAR(8) CHARACTER SET OCTETS NOT NULL,
	CHANGE_TYPE    FTS$CHANGE_TYPE NOT NULL
)
EXTERNAL NAME 'luceneudr!ftsLogChange'
ENGINE UDR;

COMMENT ON PROCEDURE FTS$LOG_CHANGE IS
'Adds a change record for one of the fields included in full-text indexes built on the table to the change log, based on which full-text indexes will be updated.';

COMMENT ON PARAMETER FTS$LOG_CHANGE.RELATION_NAME IS
'The name of the table for which the link to the record is added.';

COMMENT ON PARAMETER FTS$LOG_CHANGE.DB_KEY IS
'Link to a record in the table (corresponds to the RDB$DB_KEY pseudo field).';

COMMENT ON PARAMETER FTS$LOG_CHANGE.CHANGE_TYPE IS
'Change type (I - INSERT, U - UPDATE, D - DELETE).';

GRANT INSERT ON TABLE FTS$LOG TO PROCEDURE FTS$LOG_CHANGE;

CREATE PROCEDURE FTS$CLEAR_LOG
EXTERNAL NAME 'luceneudr!ftsClearLog'
ENGINE UDR;

COMMENT ON PROCEDURE FTS$CLEAR_LOG IS
'Clears the FTS$LOG entry change log.';

GRANT SELECT, DELETE ON TABLE FTS$LOG TO PROCEDURE FTS$CLEAR_LOG;
