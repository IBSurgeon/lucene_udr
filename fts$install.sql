CREATE DOMAIN FTS$D_INDEX_STATUS
CHAR(1) CHARACTER SET UTF8
CHECK (VALUE IN ('I', 'N', 'C', 'U'));

COMMENT ON DOMAIN FTS$D_INDEX_STATUS IS
'Full-text index status. I - Inactive, N - New index (need rebuild), C - complete and active, U - updated metadata (need rebuild).';

CREATE DOMAIN FTS$D_CHANGE_TYPE
CHAR(1) CHARACTER SET UTF8
CHECK (VALUE IN ('I', 'U', 'D'));

COMMENT ON DOMAIN FTS$D_CHANGE_TYPE IS
'Type of record change. I - INSERT, U - UPDATE, D - DELETE.';

CREATE DOMAIN FTS$D_REC_ID
CHAR(8) CHARACTER SET OCTETS;

COMMENT ON DOMAIN FTS$D_REC_ID IS
'Record ID (RDB$DB_KEY).';

CREATE TABLE FTS$INDICES(
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 DEFAULT 'STANDARD' NOT NULL,
   FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
   FTS$INDEX_STATUS FTS$D_INDEX_STATUS DEFAULT 'N' NOT NULL,
   CONSTRAINT PK_FTS$INDEX_NAME PRIMARY KEY(FTS$INDEX_NAME)
);

COMMENT ON TABLE FTS$INDICES IS
'Indexes for full-text search.';

COMMENT ON COLUMN FTS$INDICES.FTS$INDEX_NAME IS
'Full-text index name.';

COMMENT ON COLUMN FTS$INDICES.FTS$ANALYZER IS
'The analyzer. If not specified, it uses STANDARD (StandardAnalyzer) by default.';

COMMENT ON COLUMN FTS$INDICES.FTS$DESCRIPTION IS
'Description of the full-text index.';

COMMENT ON COLUMN FTS$INDICES.FTS$DESCRIPTION IS
'Full-text index status.';

CREATE TABLE FTS$INDEX_SEGMENTS(
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$BOOST DOUBLE PRECISION,
   CONSTRAINT UK_FTS$INDEX_SEGMENTS UNIQUE(FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$FIELD_NAME),
   CONSTRAINT FK_FTS$INDEX_SEGMENTS FOREIGN KEY(FTS$INDEX_NAME) REFERENCES FTS$INDICES(FTS$INDEX_NAME) ON DELETE CASCADE
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

COMMENT ON COLUMN FTS$INDEX_SEGMENTS.FTS$BOOST IS 
'Boost significance';



CREATE TABLE FTS$LOG (
  FTS$LOG_ID BIGINT /*INT*/ GENERATED BY DEFAULT AS IDENTITY,
  FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
  FTS$REC_ID FTS$D_REC_ID NOT NULL,
  FTS$CHANGE_TYPE FTS$D_CHANGE_TYPE NOT NULL,
  CONSTRAINT PK_FTS$LOG_ID PRIMARY KEY(FTS$LOG_ID)
);

COMMENT ON TABLE FTS$LOG IS
'Changelog for maintaining full-text indexes.';

COMMENT ON COLUMN FTS$LOG.FTS$LOG_ID IS
'Identifier.';

COMMENT ON COLUMN FTS$LOG.FTS$RELATION_NAME IS
'Name of the indexed table.';

COMMENT ON COLUMN FTS$LOG.FTS$REC_ID IS
'Record ID.';

COMMENT ON COLUMN FTS$LOG.FTS$CHANGE_TYPE IS
'Type of record change.';

SET TERM ^ ;

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

  PROCEDURE FTS$OPTIMIZE_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  );
END^

RECREATE PACKAGE BODY FTS$MANAGEMENT
AS
BEGIN
  FUNCTION FTS$GET_DIRECTORY ()
  RETURNS VARCHAR(255) CHARACTER SET UTF8
  EXTERNAL NAME 'luceneudr!getFTSDirectory' ENGINE UDR;


  PROCEDURE FTS$ANALYZERS
  RETURNS (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8)
  EXTERNAL NAME 'luceneudr!getAnalyzers' ENGINE UDR;


  PROCEDURE FTS$CREATE_INDEX (
      FTS$INDEX_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$ANALYZER    VARCHAR(63) CHARACTER SET UTF8,
      FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8)
  EXTERNAL NAME 'luceneudr!createIndex' ENGINE UDR;


  PROCEDURE FTS$DROP_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  EXTERNAL NAME 'luceneudr!dropIndex' ENGINE UDR;


  PROCEDURE FTS$SET_INDEX_ACTIVE (
      FTS$INDEX_NAME   VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$INDEX_ACTIVE BOOLEAN NOT NULL)
  EXTERNAL NAME 'luceneudr!setIndexActive' ENGINE UDR;


  PROCEDURE FTS$ADD_INDEX_FIELD (
      FTS$INDEX_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$BOOST         DOUBLE PRECISION)
  EXTERNAL NAME 'luceneudr!addIndexField' ENGINE UDR;


  PROCEDURE FTS$DROP_INDEX_FIELD (
      FTS$INDEX_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME    VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  EXTERNAL NAME 'luceneudr!dropIndexField' ENGINE UDR;


  PROCEDURE FTS$REBUILD_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  EXTERNAL NAME 'luceneudr!rebuildIndex' ENGINE UDR;


  PROCEDURE FTS$REINDEX_TABLE (
      FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL)
  AS
  BEGIN
    FOR
      SELECT DISTINCT
        FTS$INDEX_NAME
      FROM FTS$INDEX_SEGMENTS
      WHERE FTS$RELATION_NAME = :FTS$RELATION_NAME
      AS CURSOR C
    DO
      EXECUTE PROCEDURE FTS$REBUILD_INDEX(:C.FTS$INDEX_NAME);
   END

  PROCEDURE FTS$FULL_REINDEX
  AS
  BEGIN
    FOR
      SELECT
        FTS$INDEX_NAME
      FROM FTS$INDICES
      AS CURSOR C
    DO
      EXECUTE PROCEDURE FTS$REBUILD_INDEX(:C.FTS$INDEX_NAME);
  END

  PROCEDURE FTS$OPTIMIZE_INDEX (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  EXTERNAL NAME 'luceneudr!optimizeIndex'
  ENGINE UDR;
END^

SET TERM ; ^

COMMENT ON PACKAGE FTS$MANAGEMENT IS
'Procedures and functions for managing full-text indexes.';

GRANT ALL ON TABLE FTS$INDICES TO PACKAGE FTS$MANAGEMENT;
GRANT ALL ON TABLE FTS$INDEX_SEGMENTS TO PACKAGE FTS$MANAGEMENT;

CREATE OR ALTER PROCEDURE FTS$SEARCH (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$SEARCH_RELATION VARCHAR(63) CHARACTER SET UTF8,
   FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
   FTS$LIMIT INT /*BIGINT*/ NOT NULL DEFAULT 1000,
   FTS$EXPLAIN BOOLEAN DEFAULT FALSE
)
RETURNS (
   FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
   FTS$REC_ID FTS$D_REC_ID,
   FTS$SCORE DOUBLE PRECISION,
   FTS$EXPLANATION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!ftsSearch'
ENGINE UDR;

COMMENT ON PROCEDURE FTS$SEARCH IS
'Performs a full-text search at the specified index.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$INDEX_NAME IS
'Name of the full-text index to search.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$SEARCH_RELATION IS
'The name of the table, restricts the search to the specified table only. If the table is not specified, then the search is done on all index segments.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$QUERY IS
'Full text search expression.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$LIMIT IS
'Limit on the number of records (search result).';

COMMENT ON PARAMETER FTS$SEARCH.FTS$EXPLAIN IS
'Explain the search results';

COMMENT ON PARAMETER FTS$SEARCH.FTS$RELATION_NAME IS
'The name of the table in which the document is found.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$REC_ID IS
'Reference to the record in the table where the document was found (corresponds to the RDB$DB_KEY pseudo field).';

COMMENT ON PARAMETER FTS$SEARCH.FTS$SCORE IS
'The degree of match to the search query.';

COMMENT ON PARAMETER FTS$SEARCH.FTS$EXPLANATION IS
'Explanation of the search result';

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

CREATE OR ALTER PROCEDURE FTS$LOG_CHANGE (
    FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$REC_ID         FTS$D_REC_ID NOT NULL,
    FTS$CHANGE_TYPE    FTS$D_CHANGE_TYPE NOT NULL
)
EXTERNAL NAME 'luceneudr!ftsLogChange'
ENGINE UDR;

COMMENT ON PROCEDURE FTS$LOG_CHANGE IS
'Adds a change record for one of the fields included in full-text indexes built on the table to the change log, based on which full-text indexes will be updated.';

COMMENT ON PARAMETER FTS$LOG_CHANGE.FTS$RELATION_NAME IS
'The name of the table for which the link to the record is added.';

COMMENT ON PARAMETER FTS$LOG_CHANGE.FTS$REC_ID IS
'Record ID(corresponds to the RDB$DB_KEY pseudo field).';

COMMENT ON PARAMETER FTS$LOG_CHANGE.FTS$CHANGE_TYPE IS
'Change type (I - INSERT, U - UPDATE, D - DELETE).';

GRANT INSERT ON TABLE FTS$LOG TO PROCEDURE FTS$LOG_CHANGE;

CREATE OR ALTER PROCEDURE FTS$CLEAR_LOG
EXTERNAL NAME 'luceneudr!ftsClearLog'
ENGINE UDR;

COMMENT ON PROCEDURE FTS$CLEAR_LOG IS
'Clears the FTS$LOG entry change log.';

GRANT SELECT, DELETE ON TABLE FTS$LOG TO PROCEDURE FTS$CLEAR_LOG;

SET TERM ^ ;

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
   
END^

RECREATE PACKAGE BODY FTS$TRIGGER_HELPER
AS
BEGIN

  PROCEDURE FTS$MAKE_TRIGGERS (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
    FTS$MULTI_ACTION BOOLEAN
  )
  RETURNS (
    FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  )
  EXTERNAL NAME 'luceneudr!ftsMakeTrigger'
  ENGINE UDR;

  PROCEDURE FTS$MAKE_TRIGGERS_BY_INDEX (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8,
    FTS$MULTI_ACTION BOOLEAN
  )
  RETURNS (
    FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  )
  AS
  BEGIN
    FOR
      SELECT
        FTS$RELATION_NAME
      FROM FTS$INDEX_SEGMENTS S
      WHERE S.FTS$INDEX_NAME = :FTS$INDEX_NAME
      GROUP BY 1
      AS CURSOR C
    DO
    BEGIN
      FOR
        SELECT FTS$TRIGGER_SOURCE
        FROM FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS(:C.FTS$RELATION_NAME, :FTS$MULTI_ACTION)
        INTO :FTS$TRIGGER_SOURCE
      DO
        SUSPEND;
    END
  END

  PROCEDURE FTS$MAKE_ALL_TRIGGERS (
    FTS$MULTI_ACTION BOOLEAN
  )
  RETURNS (
    FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8
  )
  AS
  BEGIN
    FOR
      SELECT
        FTS$RELATION_NAME
      FROM FTS$INDEX_SEGMENTS S
      GROUP BY 1
      AS CURSOR C
    DO
    BEGIN
      FOR
        SELECT FTS$TRIGGER_SOURCE
        FROM FTS$TRIGGER_HELPER.FTS$MAKE_TRIGGERS(:C.FTS$RELATION_NAME, :FTS$MULTI_ACTION)
        INTO :FTS$TRIGGER_SOURCE
      DO
        SUSPEND;
    END
  END

END^

SET TERM ; ^

COMMENT ON PACKAGE FTS$TRIGGER_HELPER IS
'Utilities for creating triggers that support full-text search indexes.';

GRANT EXECUTE ON PACKAGE FTS$TRIGGER_HELPER TO PACKAGE FTS$TRIGGER_HELPER;
GRANT SELECT ON FTS$INDEX_SEGMENTS TO PACKAGE FTS$TRIGGER_HELPER;

SET TERM ^ ;

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
   *   FTS$MAX_NUM_FRAGMENTS - maximum number of fragments;
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
END^

RECREATE PACKAGE BODY FTS$HIGHLIGHTER
AS
BEGIN
  FUNCTION FTS$BEST_FRAGMENT (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL,
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL)
  RETURNS VARCHAR(8191) CHARACTER SET UTF8
  EXTERNAL NAME 'luceneudr!bestFragementHighligh' ENGINE UDR;

  PROCEDURE FTS$BEST_FRAGMENTS (
      FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
      FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
      FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8,
      FTS$FRAGMENT_SIZE SMALLINT NOT NULL,
      FTS$MAX_NUM_FRAGMENTS INTEGER NOT NULL,
      FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL,
      FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL)
  RETURNS (
      FTS$FRAGMENT VARCHAR(8191) CHARACTER SET UTF8)
  EXTERNAL NAME 'luceneudr!bestFragementsHighligh' ENGINE UDR;
END^

SET TERM ; ^

COMMENT ON PACKAGE FTS$HIGHLIGHTER IS
'Procedures and functions for highlighting found fragments';

SET TERM ^ ;

CREATE OR ALTER PACKAGE FTS$STATISTICS
AS
BEGIN
  PROCEDURE FTS$INDEX_STATISITCS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  RETURNS (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
      FTS$INDEX_STATUS TYPE OF FTS$D_INDEX_STATUS,
      FTS$INDEX_DIRECTORY VARCHAR(255) CHARACTER SET UTF8,
      FTS$INDEX_EXISTS BOOLEAN,
      FTS$INDEX_OPTIMIZED BOOLEAN,
      FTS$HAS_DELETIONS BOOLEAN,
      FTS$NUM_DOCS INTEGER,
      FTS$NUM_DELETED_DOCS INTEGER,
      FTS$NUM_FIELDS SMALLINT,
      FTS$INDEX_SIZE DOUBLE PRECISION
  );

  PROCEDURE FTS$INDICES_STATISITCS
  RETURNS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
      FTS$INDEX_STATUS TYPE OF FTS$D_INDEX_STATUS,
      FTS$INDEX_DIRECTORY VARCHAR(255) CHARACTER SET UTF8,
      FTS$INDEX_EXISTS BOOLEAN,
      FTS$INDEX_OPTIMIZED BOOLEAN,
      FTS$HAS_DELETIONS BOOLEAN,
      FTS$NUM_DOCS INTEGER,
      FTS$NUM_DELETED_DOCS INTEGER,
      FTS$NUM_FIELDS SMALLINT,
      FTS$INDEX_SIZE DOUBLE PRECISION
  );

  PROCEDURE FTS$INDEX_SEGMENTS_INFO (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
   )
   RETURNS (
      FTS$SEGMENT_NAME VARCHAR(255) CHARACTER SET UTF8,
      FTS$DOC_COUNT INTEGER,
      FTS$SEGMENT_SIZE DOUBLE PRECISION,
      FTS$HAS_DELETIONS BOOLEAN,
      FTS$DEL_COUNT INTEGER,
      FTS$DEL_FILENAME VARCHAR(255) CHARACTER SET UTF8
   );

   PROCEDURE FTS$INDEX_FIELDS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
   )
   RETURNS (
      FTS$FIELD_NAME VARCHAR(127) CHARACTER SET UTF8
   );
END^

RECREATE PACKAGE BODY FTS$STATISTICS
AS
BEGIN
  PROCEDURE FTS$INDEX_STATISITCS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
  )
  RETURNS (
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
      FTS$INDEX_STATUS TYPE OF FTS$D_INDEX_STATUS,
      FTS$INDEX_DIRECTORY VARCHAR(255) CHARACTER SET UTF8,
      FTS$INDEX_EXISTS BOOLEAN,
      FTS$INDEX_OPTIMIZED BOOLEAN,
      FTS$HAS_DELETIONS BOOLEAN,
      FTS$NUM_DOCS INTEGER,
      FTS$NUM_DELETED_DOCS INTEGER,
      FTS$NUM_FIELDS SMALLINT,
      FTS$INDEX_SIZE BIGINT /*DOUBLE PRECISION*/
  )
  EXTERNAL NAME 'luceneudr!getIndexStatistics'
  ENGINE UDR;

  PROCEDURE FTS$INDICES_STATISITCS
  RETURNS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8,
      FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
      FTS$INDEX_STATUS TYPE OF FTS$D_INDEX_STATUS,
      FTS$INDEX_DIRECTORY VARCHAR(255) CHARACTER SET UTF8,
      FTS$INDEX_EXISTS BOOLEAN,
      FTS$INDEX_OPTIMIZED BOOLEAN,
      FTS$HAS_DELETIONS BOOLEAN,
      FTS$NUM_DOCS INTEGER,
      FTS$NUM_DELETED_DOCS INTEGER,
      FTS$NUM_FIELDS SMALLINT,
      FTS$INDEX_SIZE BIGINT /*DOUBLE PRECISION*/
  )
  AS
  BEGIN
    FOR
      SELECT
        FTS$INDICES.FTS$INDEX_NAME,
        FTS$INDICES.FTS$ANALYZER,
        STAT.FTS$INDEX_STATUS,
        STAT.FTS$INDEX_DIRECTORY,
        STAT.FTS$INDEX_EXISTS,
        STAT.FTS$INDEX_OPTIMIZED,
        STAT.FTS$HAS_DELETIONS,
        STAT.FTS$NUM_DOCS,
        STAT.FTS$NUM_DELETED_DOCS,
        STAT.FTS$NUM_FIELDS,
        STAT.FTS$INDEX_SIZE
      FROM FTS$INDICES
      LEFT JOIN FTS$STATISTICS.FTS$INDEX_STATISITCS(FTS$INDICES.FTS$INDEX_NAME) STAT ON TRUE
      INTO
        FTS$INDEX_NAME,
        FTS$ANALYZER,
        FTS$INDEX_STATUS,
        FTS$INDEX_DIRECTORY,
        FTS$INDEX_EXISTS,
        FTS$INDEX_OPTIMIZED,
        FTS$HAS_DELETIONS,
        FTS$NUM_DOCS,
        FTS$NUM_DELETED_DOCS,
        FTS$NUM_FIELDS,
        FTS$INDEX_SIZE
    DO
      SUSPEND;
  END

  PROCEDURE FTS$INDEX_SEGMENTS_INFO (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
   )
   RETURNS (
      FTS$SEGMENT_NAME VARCHAR(255) CHARACTER SET UTF8,
      FTS$DOC_COUNT INTEGER,
      FTS$SEGMENT_SIZE BIGINT /*DOUBLE PRECISION*/,
      FTS$HAS_DELETIONS BOOLEAN,
      FTS$DEL_COUNT INTEGER,
      FTS$DEL_FILENAME VARCHAR(255) CHARACTER SET UTF8
   )
   EXTERNAL NAME 'luceneudr!getIndexSegments'
   ENGINE UDR;

   PROCEDURE FTS$INDEX_FIELDS (
      FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
   )
   RETURNS (
      FTS$FIELD_NAME VARCHAR(127) CHARACTER SET UTF8
   )
   EXTERNAL NAME 'luceneudr!getIndexFields'
   ENGINE UDR;
END^

SET TERM ; ^

COMMENT ON PACKAGE FTS$STATISTICS IS
'Low-level full-text index statistics';


GRANT SELECT ON FTS$INDICES TO PACKAGE FTS$STATISTICS;


