/**
 *  Firebird Relation Helper.
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

#include "Relations.h"

using namespace Firebird;
using namespace std;

namespace LuceneUDR
{

	/// <summary>
	/// Returns information about the relation.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// 
	/// <returns>Returns information about the relation.</returns>
	RelationInfoPtr RelationHelper::getRelationInfo(
		ThrowStatusWrapper* status,
		IAttachment* att,
		ITransaction* tra,
		const unsigned int sqlDialect,
		const string& relationName) 
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTEGER, relationType)
			(FB_SMALLINT, systemFlag)
		) output(status, m_master);

		input.clear();
		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		if (!stmt_get_relation.hasData()) {
			stmt_get_relation.reset(att->prepare(
				status,
				tra,
				0,
				"SELECT\n"
				"  TRIM(R.RDB$RELATION_NAME) AS RDB$RELATION_NAME,\n"
				"  CASE\n"
				"    WHEN R.RDB$RELATION_TYPE IS NOT NULL THEN R.RDB$RELATION_TYPE\n"
				"    ELSE IIF(R.RDB$VIEW_BLR IS NULL, 0, 1)\n"
				"  END AS RDB$RELATION_TYPE,\n"
				"  COALESCE(R.RDB$SYSTEM_FLAG, 0) AS RDB$SYSTEM_FLAG\n"
				"FROM RDB$RELATIONS R\n"
				"WHERE R.RDB$RELATION_NAME = ?",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}
		AutoRelease<IResultSet> rs(stmt_get_relation->openCursor(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			output.getMetadata(),
			0
		));

		auto relationInfo = make_unique<RelationInfo>();

		bool foundFlag = false;
		if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			foundFlag = true;

			relationInfo->relationName.assign(output->relationName.str, output->relationName.length);
			relationInfo->relationType = static_cast<RelationType>(output->relationType);
			relationInfo->systemFlag = static_cast<bool>(output->systemFlag);
		}
		rs->close(status);
		if (!foundFlag) {
			const string error_message = string_format("Relation \"%s\" not exists", relationName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		return std::move(relationInfo);
	}

	/// <summary>
	/// Checks if the given relation exists.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// 
	/// <returns>Returns true if the relation exists, false otherwise.</returns>
	bool RelationHelper::relationExists(
		ThrowStatusWrapper* status,
		IAttachment* att,
		ITransaction* tra,
		const unsigned int sqlDialect,
		const string& relationName)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTEGER, cnt)
		) output(status, m_master);

		input.clear();
		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		if (!stmt_exists_relation.hasData()) {
			stmt_exists_relation.reset(att->prepare(
				status,
				tra,
				0,
				"SELECT COUNT(*) AS CNT\n"
				"FROM RDB$RELATIONS\n"
				"WHERE RDB$RELATION_NAME = ?",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}
		AutoRelease<IResultSet> rs(stmt_exists_relation->openCursor(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			output.getMetadata(),
			0
		));
		bool foundFlag = false;
		if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			foundFlag = (output->cnt > 0);
		}
		rs->close(status);

		return foundFlag;
	}

	/// <summary>
	/// Returns a list of relations fields.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// 
	/// <returns>Returns a list of relations fields.</returns>
	RelationFieldList RelationHelper::getFields(
		ThrowStatusWrapper* status,
		IAttachment* att,
		ITransaction* tra,
		const unsigned int sqlDialect,
		const string& relationName) 
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
			(FB_SMALLINT, fieldType)
			(FB_SMALLINT, fieldLength)
			(FB_SMALLINT, charLength)
			(FB_SMALLINT, charsetId)
			(FB_SMALLINT, fieldSubType)
			(FB_SMALLINT, fieldPrecision)
			(FB_SMALLINT, fieldScale)
		) output(status, m_master);

		input.clear();
		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		if (!stmt_relation_fields.hasData()) {
			stmt_relation_fields.reset(att->prepare(
				status,
				tra,
				0,
				"SELECT\n"
				"    TRIM(RF.RDB$RELATION_NAME) AS RDB$RELATION_NAME\n"
				"  , TRIM(RF.RDB$FIELD_NAME) AS RDB$FIELD_NAME\n"
				"  , F.RDB$FIELD_TYPE\n"
				"  , F.RDB$FIELD_LENGTH\n"
				"  , F.RDB$CHARACTER_LENGTH\n"
				"  , F.RDB$CHARACTER_SET_ID\n"
				"  , F.RDB$FIELD_SUB_TYPE\n"
				"  , F.RDB$FIELD_PRECISION\n"
				"  , F.RDB$FIELD_SCALE\n"
				"FROM RDB$RELATION_FIELDS RF\n"
				"JOIN RDB$FIELDS F\n"
				"  ON F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE\n"
				"WHERE RF.RDB$RELATION_NAME = ?",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}
		
		AutoRelease<IResultSet> rs(stmt_relation_fields->openCursor(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			output.getMetadata(),
			0
		));

		RelationFieldList fieldList;
		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			auto fieldInfo = make_unique<RelationFieldInfo>();

			fieldInfo->relationName.assign(output->relationName.str, output->relationName.length);
			fieldInfo->fieldName.assign(output->fieldName.str, output->fieldName.length);
			fieldInfo->fieldType = output->fieldType;
			fieldInfo->fieldLength = output->fieldLength;
			fieldInfo->charLength = output->charLength;
			fieldInfo->charsetId = output->charsetId;
			fieldInfo->fieldSubType = output->fieldSubType;
			fieldInfo->fieldPrecision = output->fieldPrecision;
			fieldInfo->fieldScale = output->fieldScale;

			fieldList.push_back(std::move(fieldInfo));
		}

		rs->close(status);
		return fieldList;
	}

	/// <summary>
	/// Returns a list of relations primary key fields.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// 
	/// <returns>Returns a list of relations primary key fields.</returns>
	RelationFieldList RelationHelper::getPrimaryKeyFields(
		ThrowStatusWrapper* status,
		IAttachment* att,
		ITransaction* tra,
		const unsigned int sqlDialect,
		const string& relationName
	)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
			(FB_SMALLINT, fieldType)
			(FB_SMALLINT, fieldLength)
			(FB_SMALLINT, charLength)
			(FB_SMALLINT, charsetId)
			(FB_SMALLINT, fieldSubType)
			(FB_SMALLINT, fieldPrecision)
			(FB_SMALLINT, fieldScale)
		) output(status, m_master);

		input.clear();
		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		if (!stmt_pk_fields.hasData()) {
			stmt_pk_fields.reset(att->prepare(
				status,
				tra,
				0,
				"SELECT\n"
				"    TRIM(RF.RDB$RELATION_NAME) AS RDB$RELATION_NAME\n"
				"  , TRIM(RF.RDB$FIELD_NAME) AS RDB$FIELD_NAME\n"
				"  , F.RDB$FIELD_TYPE\n"
				"  , F.RDB$FIELD_LENGTH\n"
				"  , F.RDB$CHARACTER_LENGTH\n"
				"  , F.RDB$CHARACTER_SET_ID\n"
				"  , F.RDB$FIELD_SUB_TYPE\n"
				"  , F.RDB$FIELD_PRECISION\n"
				"  , F.RDB$FIELD_SCALE\n"
				"FROM RDB$RELATION_CONSTRAINTS RC\n"
				"JOIN RDB$INDEX_SEGMENTS INDS\n"
				"  ON INDS.RDB$INDEX_NAME = RC.RDB$INDEX_NAME\n"
				"JOIN RDB$RELATION_FIELDS RF\n"
				"  ON RF.RDB$RELATION_NAME = RC.RDB$RELATION_NAME\n"
				" AND RF.RDB$FIELD_NAME = INDS.RDB$FIELD_NAME\n"
				"JOIN RDB$FIELDS F\n"
				"  ON F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE\n"
				"WHERE RC.RDB$RELATION_NAME = ?\n"
				"  AND RC.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		AutoRelease<IResultSet> rs(stmt_pk_fields->openCursor(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			output.getMetadata(),
			0
		));

		RelationFieldList fieldList;
		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			auto fieldInfo = make_unique<RelationFieldInfo>();

			fieldInfo->relationName.assign(output->relationName.str, output->relationName.length);
			fieldInfo->fieldName.assign(output->fieldName.str, output->fieldName.length);
			fieldInfo->fieldType = output->fieldType;
			fieldInfo->fieldLength = output->fieldLength;
			fieldInfo->charLength = output->charLength;
			fieldInfo->charsetId = output->charsetId;
			fieldInfo->fieldSubType = output->fieldSubType;
			fieldInfo->fieldPrecision = output->fieldPrecision;
			fieldInfo->fieldScale = output->fieldScale;

			fieldList.push_back(std::move(fieldInfo));
		}

		rs->close(status);
		return fieldList;
	}

	/// <summary>
	/// Returns information about the field.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// <param name="fieldName">Field name</param>
	/// 
	/// <returns>Returns information about the field.</returns>
	RelationFieldInfoPtr RelationHelper::getField(
		ThrowStatusWrapper* status,
		IAttachment* att,
		ITransaction* tra,
		const unsigned int sqlDialect,
		const string& relationName,
		const string& fieldName)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
			(FB_SMALLINT, fieldType)
			(FB_SMALLINT, fieldLength)
			(FB_SMALLINT, charLength)
			(FB_SMALLINT, charsetId)
			(FB_SMALLINT, fieldSubType)
			(FB_SMALLINT, fieldPrecision)
			(FB_SMALLINT, fieldScale)
		) output(status, m_master);

		input.clear();

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		input->fieldName.length = static_cast<ISC_USHORT>(fieldName.length());
		fieldName.copy(input->fieldName.str, input->fieldName.length);

		if (!stmt_get_field.hasData()) {
			stmt_get_field.reset(att->prepare(
				status,
				tra,
				0,
				"SELECT\n"
				"    TRIM(RF.RDB$RELATION_NAME) AS RDB$RELATION_NAME\n"
				"  , TRIM(RF.RDB$FIELD_NAME) AS RDB$FIELD_NAME\n"
				"  , F.RDB$FIELD_TYPE\n"
				"  , F.RDB$FIELD_LENGTH\n"
				"  , F.RDB$CHARACTER_LENGTH\n"
				"  , F.RDB$CHARACTER_SET_ID\n"
				"  , F.RDB$FIELD_SUB_TYPE\n"
				"  , F.RDB$FIELD_PRECISION\n"
				"  , F.RDB$FIELD_SCALE\n"
				"FROM RDB$RELATION_FIELDS RF\n"
				"JOIN RDB$FIELDS F\n"
				"  ON F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE\n"
				"WHERE RF.RDB$RELATION_NAME = ? AND RF.RDB$FIELD_NAME = ?",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		AutoRelease<IResultSet> rs(stmt_get_field->openCursor(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			output.getMetadata(),
			0
		));

		auto fieldInfo = make_unique<RelationFieldInfo>();
		bool foundFlag = false;
		if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			foundFlag = true;

			fieldInfo->relationName.assign(output->relationName.str, output->relationName.length);
			fieldInfo->fieldName.assign(output->fieldName.str, output->fieldName.length);
			fieldInfo->fieldType = output->fieldType;
			fieldInfo->fieldLength = output->fieldLength;
			fieldInfo->charLength = output->charLength;
			fieldInfo->charsetId = output->charsetId;
			fieldInfo->fieldSubType = output->fieldSubType;
			fieldInfo->fieldPrecision = output->fieldPrecision;
			fieldInfo->fieldScale = output->fieldScale;
		}
		rs->close(status);

		if (!foundFlag) {
			const string error_message = string_format("Field \"%s\" not found in relation \"%s\".", fieldName, relationName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		return std::move(fieldInfo);
	}

	/// <summary>
	/// Checks if the specified column exists in the relation. 
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// <param name="fieldName">Field name</param>
	/// 
	/// <returns>Returns true if the column exists, false otherwise.</returns>
	bool RelationHelper::fieldExists(
		ThrowStatusWrapper* status,
		IAttachment* att,
		ITransaction* tra,
		const unsigned int sqlDialect,
		const string& relationName,
		const string& fieldName)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTEGER, cnt)
		) output(status, m_master);

		input.clear();

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		input->fieldName.length = static_cast<ISC_USHORT>(fieldName.length());
		fieldName.copy(input->fieldName.str, input->fieldName.length);

		if (!stmt_exists_field.hasData()) {
			stmt_exists_field.reset(att->prepare(
				status,
				tra,
				0,
				"SELECT COUNT(*) AS CNT\n"
				"FROM RDB$RELATION_FIELDS\n"
				"WHERE RDB$RELATION_NAME = ? AND RDB$FIELD_NAME = ?",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}
		AutoRelease<IResultSet> rs(stmt_exists_field->openCursor(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			output.getMetadata(),
			0
		));
		bool foundFlag = false;
		if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			foundFlag = (output->cnt > 0);
		}
		rs->close(status);

		return foundFlag;
	}

}
