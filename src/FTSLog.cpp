#include "FTSLog.h"

using namespace Firebird;
using namespace std;


namespace LuceneUDR {

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
	void FTSLogRepository::appendLog(
		ThrowStatusWrapper* status,
		IAttachment* att,
		ITransaction* tra,
		const unsigned int sqlDialect,
		const string& relationName,
		const string& recId,
		const string& changeType)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
			(FB_INTL_VARCHAR(8, CS_BINARY), rec_id)
			(FB_INTL_VARCHAR(4, CS_UTF8), change_type)
		) input(status, m_master);

		input.clear();

		input->relation_name.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relation_name.str, input->relation_name.length);

		input->rec_id.length = static_cast<ISC_USHORT>(recId.length());
		recId.copy(input->rec_id.str, input->rec_id.length);

		input->change_type.length = static_cast<ISC_USHORT>(changeType.length());
		changeType.copy(input->change_type.str, input->change_type.length);

		if (!stmt_append_log.hasData()) {
			stmt_append_log.reset(att->prepare(
				status,
				tra,
				0,
				"INSERT INTO FTS$LOG (\n"
				"  FTS$RELATION_NAME,\n"
				"  FTS$REC_ID,\n"
				"  FTS$CHANGE_TYPE\n"
				")\n"
				"VALUES(?, ?, ?)",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		stmt_append_log->execute(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
	}

	/// <summary>
	/// Removes an entry from the changelog.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="id">Identifier</param>
	void FTSLogRepository::deleteLog(
		ThrowStatusWrapper* status,
		IAttachment* att,
		ITransaction* tra,
		const unsigned int sqlDialect,
		const ISC_INT64 id)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_BIGINT, id)
		) input(status, m_master);

		input.clear();
		input->idNull = false;
		input->id = id;

		if (!stmt_delete_log.hasData()) {
			stmt_delete_log.reset(att->prepare(
				status,
				tra,
				0,
				"DELETE FROM FTS$LOG\n"
				"WHERE FTS$LOG_ID = ?",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		stmt_delete_log->execute(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
	}

	/// <summary>
	/// Clears the changelog.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	void FTSLogRepository::clearLog(
		ThrowStatusWrapper* status,
		IAttachment* att,
		ITransaction* tra,
		const unsigned int sqlDialect)
	{
		att->execute(
			status,
			tra,
			0,
			"DELETE FROM FTS$LOG",
			sqlDialect,
			nullptr,
			nullptr,
			nullptr,
			nullptr
		);
	}

}