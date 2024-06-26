/**
 *  IBSurgeon Full Text Search UDR library uninstallation script.
 *
 *  The original code was created by Simonov Denis
 *  for the open source project "IBSurgeon Full Text Search UDR".
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
DROP PROCEDURE FTS$ANALYZE;
DROP PROCEDURE FTS$UPDATE_INDEXES;
DROP FUNCTION FTS$ESCAPE_QUERY;
DROP TABLE FTS$LOG;
DROP TABLE FTS$INDEX_SEGMENTS;
DROP TABLE FTS$INDICES;
DROP TABLE FTS$STOP_WORDS;
DROP TABLE FTS$ANALYZERS;
DROP DOMAIN FTS$D_INDEX_STATUS;
DROP DOMAIN FTS$D_CHANGE_TYPE;
DROP EXCEPTION FTS$EXCEPTION;

COMMIT;
