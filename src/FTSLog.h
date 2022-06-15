#ifndef FTS_LOG_H
#define FTS_LOG_H

/**
 *  Utilities for maintaining the change log. 
 *  The journal is used to keep full-text indexes up-to-date.
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

#include "LuceneUdr.h"

using namespace Firebird;
using namespace std;

namespace LuceneUDR
{
	class FTSLogRepository final
	{
	private:
		IMaster* m_master;
		// prepared statements
		AutoRelease<IStatement> stmt_append_log;
		AutoRelease<IStatement> stmt_delete_log;

		const char* SQL_APPEND_LOG = 
R"SQL(
INSERT INTO FTS$LOG (
  FTS$RELATION_NAME,
  FTS$DB_KEY,
  FTS$REC_UUID,
  FTS$REC_ID,
  FTS$CHANGE_TYPE
)
VALUES(?, ?, ?, ?, ?)
)SQL";

		const char* SQL_DELETE_LOG =
R"SQL(
DELETE FROM FTS$LOG
WHERE FTS$LOG_ID = ?
)SQL";
	public:
		FTSLogRepository()
			: FTSLogRepository(nullptr)
		{}

		FTSLogRepository(IMaster* master)
			: m_master(master)
			, stmt_append_log(nullptr)
			, stmt_delete_log(nullptr)
		{
		}


		/// <summary>
		/// Adds an entry from the changelog.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="dbKey">Record ID</param>
		/// <param name="changeType">Type of change</param>
		void appendLogByDbKey (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& relationName,
			const string& dbKey,
			const string& changeType);

		/// <summary>
		/// Adds an entry from the changelog.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="dbKey">Record ID</param>
		/// <param name="changeType">Type of change</param>
		void appendLogById (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& relationName,
			const ISC_INT64 recId,
			const string& changeType);

		/// <summary>
		/// Adds an entry from the changelog.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="dbKey">Record ID</param>
		/// <param name="changeType">Type of change</param>
		void appendLogByUuid (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& relationName,
			const string& uuid,
			const string& changeType);

	};
}

#endif	// FTS_LOG_H