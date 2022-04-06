#ifndef FTS_LOG_H
#define FTS_LOG_H

#include "LuceneUdr.h"

using namespace Firebird;
using namespace std;

namespace LuceneFTS
{

	class FTSLogRepository final
	{
	private:
		IMaster* m_master;
		// prepared statements
		AutoRelease<IStatement> stmt_append_log;
		AutoRelease<IStatement> stmt_delete_log;
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
		/// <param name="recId">Record ID</param>
		/// <param name="changeType">Type of change</param>
		void appendLog(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string relationName,
			const string recId,
			const string changeType);


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