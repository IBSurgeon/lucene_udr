#ifndef LUCENE_ANALYZERS_H
#define LUCENE_ANALYZERS_H

/**
 *  Utilities for getting and managing metadata for analyzers.
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

#include "LuceneUdr.h"
#include "LuceneHeaders.h"
#include <list>
#include <string>

using namespace Firebird;
using namespace Lucene;
using namespace std;

namespace LuceneUDR 
{
	class LuceneAnalyzerFactory;
	struct AnalyzerInfo;
}

namespace FTSMetadata
{
	class AnalyzerRepository final
	{
	private:
		IMaster* const m_master = nullptr;
		unique_ptr<LuceneUDR::LuceneAnalyzerFactory> const m_analyzerFactory = nullptr;

		// prepared statements
		AutoRelease<IStatement> m_stmt_get_analyzer{ nullptr };
		AutoRelease<IStatement> m_stmt_get_analyzers{ nullptr };
		AutoRelease<IStatement> m_stmt_has_analyzer{ nullptr };
		AutoRelease<IStatement> m_stmt_get_stopwords{ nullptr };
		AutoRelease<IStatement> m_stmt_insert_stopword{ nullptr };
		AutoRelease<IStatement> m_stmt_delete_stopword{ nullptr };

		// SQL texts
		const char* SQL_ANALYZER_INFO =
			R"SQL(
SELECT
    A.FTS$ANALYZER_NAME
  , A.FTS$BASE_ANALYZER
  , A.FTS$DESCRIPTION
FROM FTS$ANALYZERS A
WHERE A.FTS$ANALYZER_NAME = ?
)SQL";

		const char* SQL_ANALYZER_INFOS =
			R"SQL(
SELECT
    A.FTS$ANALYZER_NAME
  , A.FTS$BASE_ANALYZER
  , A.FTS$DESCRIPTION
FROM FTS$ANALYZERS A
ORDER BY A.FTS$ANALYZER_NAME
)SQL";

		const char* SQL_ANALYZER_EXISTS =
			R"SQL(
SELECT COUNT(*) AS CNT
FROM FTS$ANALYZERS A
WHERE A.FTS$ANALYZER_NAME = ?
)SQL";

		const char* SQL_INSERT_ANALYZER =
			R"SQL(
INSERT INTO FTS$ANALYZERS (
    FTS$ANALYZER_NAME,
    FTS$BASE_ANALYZER,
    FTS$DESCRIPTION)
VALUES (
    ?,
    ?,
    ?)
)SQL";

		const char* SQL_DELETE_ANALYZER =
			R"SQL(
DELETE FROM FTS$ANALYZERS A
WHERE A.FTS$ANALYZER_NAME = ?
)SQL";

		const char* SQL_STOP_WORDS =
			R"SQL(
SELECT
    W.FTS$WORD
FROM FTS$STOP_WORDS W
WHERE W.FTS$ANALYZER_NAME = ?
)SQL";

		const char* SQL_INSERT_STOP_WORD =
			R"SQL(
EXECUTE BLOCK (
    FTS$ANALYZER_NAME VARCHAR(63) CHARACTER SET UTF8 = ?,
    FTS$WORD          VARCHAR(63) CHARACTER SET UTF8 = ?)
AS
BEGIN
  INSERT INTO FTS$STOP_WORDS (
      FTS$ANALYZER_NAME,
      FTS$WORD)
  VALUES (
      :FTS$ANALYZER_NAME,
      LOWER(:FTS$WORD));

  WHEN GDSCODE UNIQUE_KEY_VIOLATION DO
    EXCEPTION FTS$EXCEPTION 'Stop word "' || FTS$WORD || '" already exists for analyzer "' || FTS$ANALYZER_NAME || '"';
END
)SQL";

		const char* SQL_DELETE_STOP_WORD =
			R"SQL(
DELETE FROM FTS$STOP_WORDS
WHERE FTS$ANALYZER_NAME = ? AND FTS$WORD = ?
)SQL";
	public:
		AnalyzerRepository() = delete;
		explicit AnalyzerRepository(IMaster* const master);

		~AnalyzerRepository();


		AnalyzerPtr createAnalyzer (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& analyzerName
		);

		const LuceneUDR::AnalyzerInfo getAnalyzerInfo (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& analyzerName
		);

		list<LuceneUDR::AnalyzerInfo> getAnalyzerInfos (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect
		);

		bool hasAnalyzer (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& analyzerName
		);

		void addAnalyzer (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& analyzerName,
			const string& baseAnalyzer,
			const string& description
		);

		void deleteAnalyzer(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& analyzerName
		);

		const HashSet<String> getStopWords (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& analyzerName
		);

		void addStopWord (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& analyzerName,
			const string& stopWord
		);

		void deleteStopWord(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& analyzerName,
			const string& stopWord
		);
	};
}

#endif // LUCENE_ANALYZERS_H