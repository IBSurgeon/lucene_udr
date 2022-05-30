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
			"INSERT INTO FTS$LOG (\n"
			"  FTS$RELATION_NAME,\n"
			"  FTS$REC_ID,\n"
			"  FTS$REC_UUID,\n"
			"  FTS$REC_ID,\n"
			"  FTS$CHANGE_TYPE\n"
			")\n"
			"VALUES(?, ?, ?, ?, ?)"
			"\0";
	public:
		FTSLogRepository()
			: FTSLogRepository(nullptr)
		{}

		FTSLogRepository(IMaster* master)
			: m_master(master),
			stmt_append_log(nullptr)
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
		void appendLogByDbKey(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
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
		void appendLogById(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
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
		void appendLogByUuid(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string& relationName,
			const string& uuid,
			const string& changeType);


		/// <summary>
		/// Removes an entry from the changelog.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="id">Identifier</param>
		void deleteLog(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const ISC_INT64 id);

		/// <summary>
		/// Clears the changelog.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		void clearLog(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect);
	};
}

#endif	// FTS_LOG_H