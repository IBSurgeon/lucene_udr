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
		// подготовленные запросы
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

		//
		// Добавлении записи в журнал изменений
		//
		void appendLog(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra,
			string relationName,
			string dbKey,
			string changeType);

		//
		// Удаление записи из журнала изменений
		//
		void deleteLog(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra,
			ISC_INT64 id);

		//
		// Очистка журнала изменений
		//
		void clearLog(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra);
	};
}

#endif	// FTS_LOG_H