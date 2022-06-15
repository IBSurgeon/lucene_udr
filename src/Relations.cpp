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
				SQL_RELATION_INFO,
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
			const string error_message = string_format(R"(Relation "%s" not exists)"s, relationName);
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
				SQL_RELATION_EXISTS,
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
	/// <param name="fields">List of relations fields</param>
	/// 
	void RelationHelper::fillRelationFields(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& relationName,
		RelationFieldList& fields
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

		if (!stmt_relation_fields.hasData()) {
			stmt_relation_fields.reset(att->prepare(
				status,
				tra,
				0,
				SQL_RELATION_FIELDS,
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

			fields.push_back(std::move(fieldInfo));
		}

		rs->close(status);
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
	/// <param name="keyFields">List of relations primary key fields</param>
	/// 
	void RelationHelper::fillPrimaryKeyFields(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& relationName,
		RelationFieldList& keyFields
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
				SQL_RELATION_KEY_FIELDS,
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

			keyFields.push_back(std::move(fieldInfo));
		}

		rs->close(status);
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
				SQL_RELATION_FIELD,
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
			const string error_message = string_format(R"(Field "%s" not found in relation "%s".)"s, fieldName, relationName);
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
				SQL_RELATION_FIELD_EXISTS,
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
