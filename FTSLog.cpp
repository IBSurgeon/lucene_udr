#include "FTSLog.h"

using namespace Firebird;
using namespace std;
using namespace LuceneFTS;

//
// Добавлении записи в журнал изменений
//
void FTSLogRepository::appendLog(
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect,
	string relationName,
	string dbKey,
	string changeType)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_INTL_VARCHAR(8, CS_BINARY), db_key)
		(FB_INTL_VARCHAR(4, CS_UTF8), change_type)
	) input(status, m_master);

	input.clear();

	input->relation_name.length = relationName.length();
	relationName.copy(input->relation_name.str, input->relation_name.length);

	input->db_key.length = dbKey.length();
	dbKey.copy(input->db_key.str, input->db_key.length);

	input->change_type.length = changeType.length();
	changeType.copy(input->change_type.str, input->change_type.length);

	if (!stmt_append_log.hasData()) {
		stmt_append_log.reset(att->prepare(
			status,
			tra,
			0,
			"INSERT INTO FTS$LOG (\n"
			"  RELATION_NAME,\n"
			"  DB_KEY,\n"
			"  CHANGE_TYPE\n"
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

//
// Удаление записи из журнала изменений
//
void FTSLogRepository::deleteLog(
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect,
	ISC_INT64 id)
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
			"WHERE ID = ?",
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

//
// Очистка журнала изменений
//
void FTSLogRepository::clearLog(
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect)
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