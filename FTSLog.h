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
		// �������������� �������
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
		// ���������� ������ � ������ ���������
		//
		void appendLog(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra,
			string relationName,
			string dbKey,
			string changeType);

		//
		// �������� ������ �� ������� ���������
		//
		void deleteLog(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra,
			ISC_INT64 id);

		//
		// ������� ������� ���������
		//
		void clearLog(
			ThrowStatusWrapper status,
			IAttachment* att,
			ITransaction* tra);
	};
}

#endif	// FTS_LOG_H