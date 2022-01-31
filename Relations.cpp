
#include "Relations.h"

using namespace Firebird;
using namespace std;
using namespace LuceneFTS;

bool RelationHelper::relationExists(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string relationName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
	) input(&status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTEGER, cnt)
	) output(&status, m_master);

	input.clear();
	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);

	if (!stmt_exists_relation.hasData()) {
		stmt_exists_relation.reset(att->prepare(
			&status,
			tra,
			0,
			"SELECT COUNT(*) AS CNT\n"
			"FROM RDB$RELATIONS\n"
			"WHERE RDB$RELATION_NAME = ?",
			UDR_SQL_DIALECT,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	}
	AutoRelease<IResultSet> rs(stmt_exists_relation->openCursor(
		&status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	bool foundFlag = false;
	if (rs->fetchNext(&status, output.getData()) == IStatus::RESULT_OK) {
		foundFlag = (output->cnt > 0);
	}
	rs->close(&status);

	return foundFlag;
}

bool RelationHelper::fieldExists(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string relationName, string fieldName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
	) input(&status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTEGER, cnt)
	) output(&status, m_master);

	input.clear();
	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);
	input->fieldName.length = fieldName.length();
	fieldName.copy(input->fieldName.str, input->fieldName.length);

	if (!stmt_exists_field.hasData()) {
		stmt_exists_field.reset(att->prepare(
			&status,
			tra,
			0,
			"SELECT COUNT(*) AS CNT\n"
			"FROM RDB$RELATION_FIELDS\n"
			"WHERE RDB$RELATION_NAME = ? AND RDB$FIELD_NAME = ?",
			UDR_SQL_DIALECT,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	}
	AutoRelease<IResultSet> rs(stmt_exists_field->openCursor(
		&status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	bool foundFlag = false;
	if (rs->fetchNext(&status, output.getData()) == IStatus::RESULT_OK) {
		foundFlag = (output->cnt > 0);
	}
	rs->close(&status);

	return foundFlag;
}
