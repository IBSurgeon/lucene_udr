/**
 *  Lucene UDR library uninstallation script.
 *
 *  The original code was created by Simonov Denis
 *  for the open source Lucene UDR full-text search library for Firebird DBMS.
 *
 *  Copyright (c) 2022 Simonov Denis <sim-mail@list.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/

DROP PACKAGE FTS$MANAGEMENT;
DROP PACKAGE FTS$TRIGGER_HELPER;
DROP PACKAGE FTS$HIGHLIGHTER;
DROP PACKAGE FTS$STATISTICS;
DROP PROCEDURE FTS$SEARCH;
DROP PROCEDURE FTS$UPDATE_INDEXES;
DROP PROCEDURE FTS$LOG_CHANGE;
DROP PROCEDURE FTS$CLEAR_LOG;
DROP TABLE FTS$LOG;
DROP TABLE FTS$INDEX_SEGMENTS;
DROP TABLE FTS$INDICES;
DROP DOMAIN FTS$D_INDEX_STATUS;
DROP DOMAIN FTS$D_CHANGE_TYPE;
