/**
 *  Utilities for building FTS triggers for full-text indexes.
 *
 *  The original code was created by Simonov Denis
 *  for the open source project "IBSurgeon Full Text Search UDR".
 *
 *  Copyright (c) 2022 Simonov Denis <sim-mail@list.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/


#include "FTSTrigger.h"
#include "FBUtils.h"
#include "LazyFactory.h"

using namespace LuceneUDR;

namespace FTSMetadata
{

	const string FTSTrigger::getHeader(unsigned int sqlDialect)
	{
		string triggerHeader =
			"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
			"ACTIVE AFTER " + triggerEvents + "\n"
			"POSITION " + std::to_string(position) + "\n";
		return triggerHeader;
	}

	const string FTSTrigger::getScript(unsigned int sqlDialect)
	{
		return getHeader(sqlDialect) + triggerSource;
	}

	FTSTriggerHelper::FTSTriggerHelper(IMaster* master)
		: m_master(master)
	{
	}

	FTSTriggerHelper::~FTSTriggerHelper() = default;



	/// <summary>
	/// Returns a map of field blocks by table keys to create triggers that support full-text indexes.
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// <param name="keyFieldBlocks">Map of field blocks by table keys</param>
	/// 
	void FTSTriggerHelper::fillKeyFieldBlocks(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& relationName,
		FTSKeyFieldBlockMap& keyFieldBlocks
	)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), keyFieldName)
			(FB_INTL_VARCHAR(24, CS_UTF8), keyFieldType)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		) output(status, m_master);

		input.clear();

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		AutoRelease<IStatement> stmt(att->prepare(
			status,
			tra,
			0,
			R"SQL(
WITH T AS (
SELECT
    I.FTS$INDEX_NAME,
    MAX(IIF(SEG.FTS$KEY IS TRUE, SEG.FTS$FIELD_NAME, NULL)) OVER(PARTITION BY SEG.FTS$INDEX_NAME) AS FTS$KEY_FIELD_NAME,
    MAX(
    CASE
      WHEN SEG.FTS$KEY IS TRUE AND F.RDB$FIELD_TYPE = 14 AND F.RDB$CHARACTER_SET_ID = 1 AND F.RDB$FIELD_LENGTH = 16 THEN 'UUID'
      WHEN SEG.FTS$KEY IS TRUE AND SEG.FTS$FIELD_NAME = 'RDB$DB_KEY' THEN 'DBKEY'
      WHEN SEG.FTS$KEY IS TRUE AND F.RDB$FIELD_TYPE IN (7, 8, 16) AND F.RDB$FIELD_SCALE = 0 THEN 'INT_ID'
    END) OVER(PARTITION BY SEG.FTS$INDEX_NAME) AS FTS$KEY_FIELD_TYPE,
    SEG.FTS$FIELD_NAME
FROM FTS$INDICES I
    JOIN FTS$INDEX_SEGMENTS SEG ON
          SEG.FTS$INDEX_NAME = I.FTS$INDEX_NAME
    LEFT JOIN RDB$RELATION_FIELDS RF ON
          RF.RDB$RELATION_NAME = I.FTS$RELATION_NAME AND
          RF.RDB$FIELD_NAME = SEG.FTS$FIELD_NAME
    LEFT JOIN RDB$FIELDS F ON
          F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE
WHERE I.FTS$RELATION_NAME = ? AND
      I.FTS$INDEX_STATUS = 'C' AND
      (RF.RDB$FIELD_NAME IS NOT NULL OR SEG.FTS$FIELD_NAME = 'RDB$DB_KEY')
)
SELECT DISTINCT
    FTS$KEY_FIELD_NAME,
    TRIM(FTS$KEY_FIELD_TYPE) AS FTS$KEY_FIELD_TYPE,
    FTS$FIELD_NAME
FROM T
WHERE FTS$KEY_FIELD_NAME <> FTS$FIELD_NAME
ORDER BY FTS$KEY_FIELD_NAME
)SQL",
sqlDialect,
IStatement::PREPARE_PREFETCH_METADATA
));

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(stmt->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
			0
		));

		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			const string keyFieldName(output->keyFieldName.str, output->keyFieldName.length);
			const string keyFieldType(output->keyFieldType.str, output->keyFieldType.length);
			const string fieldName(output->fieldName.str, output->fieldName.length);

			auto [it, result] = keyFieldBlocks.try_emplace(keyFieldName, lazy_convert_construct([] { return std::make_unique<FTSKeyFieldBlock>(); }));
			const auto& block = it->second;
			if (result) {
				block->keyFieldName = keyFieldName;
				if (keyFieldType == "UUID") {
					block->keyFieldType = FTSKeyType::UUID;
				}
				if (keyFieldType == "DBKEY") {
					block->keyFieldType = FTSKeyType::DB_KEY;
				}
				if (keyFieldType == "INT_ID") {
					block->keyFieldType = FTSKeyType::INT_ID;
				}
			}
			block->fieldNames.insert(fieldName);

		}
		rs->close(status);
	}


	/// <summary>
	/// Returns a list of trigger source codes to support full-text indexes by relation name. 
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="relationName">Relation name</param>
	/// <param name="multiAction">Flag for generating multi-event triggers</param>
	/// <param name="position">Trigger position</param>
	/// <param name="triggers">Triggers list</param>
	/// 
	void FTSTriggerHelper::makeTriggerSourceByRelation(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& relationName,
		const bool multiAction,
		const unsigned short position,
		FTSTriggerList& triggers)
	{

		FTSKeyFieldBlockMap keyFieldBlocks;

		fillKeyFieldBlocks(status, att, tra, sqlDialect, relationName, keyFieldBlocks);
		for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
			if (keyFieldBlock->fieldNames.empty()) {
				continue;
			}

			for (const auto& fieldName : keyFieldBlock->fieldNames) {
				const string metaFieldName = escapeMetaName(sqlDialect, fieldName);

				if (!keyFieldBlock->insertingCondition.empty()) {
					keyFieldBlock->insertingCondition += "\n      OR ";
				}
				keyFieldBlock->insertingCondition += "NEW." + metaFieldName + " IS NOT NULL";

				if (!keyFieldBlock->updatingCondition.empty()) {
					keyFieldBlock->updatingCondition += "\n      OR ";
				}
				keyFieldBlock->updatingCondition += "NEW." + metaFieldName + " IS DISTINCT FROM " + "OLD." + metaFieldName;

				if (!keyFieldBlock->deletingCondition.empty()) {
					keyFieldBlock->deletingCondition += "\n      OR ";
				}
				keyFieldBlock->deletingCondition += "OLD." + metaFieldName + " IS NOT NULL";
			}
		}


		if (multiAction) {
			const string triggerName = "FTS$" + relationName + "_AIUD";
			const auto& source = makeTriggerSourceByRelationMulti(keyFieldBlocks, sqlDialect, relationName);
			auto trigger = make_unique<FTSTrigger>(
				triggerName,
				relationName,
				"INSERT OR UPDATE OR DELETE",
				position,
				source
				);
			triggers.push_back(std::move(trigger));
		}
		else {
			{
				// INSERT
				const string triggerName = "FTS$" + relationName + "_AI";
				const auto& source = makeTriggerSourceByRelationInsert(keyFieldBlocks, sqlDialect, relationName);
				auto trigger = make_unique<FTSTrigger>(
					triggerName,
					relationName,
					"INSERT",
					position,
					source
					);
				triggers.push_back(std::move(trigger));
			}
			{
				// UPDATE
				const string triggerName = "FTS$" + relationName + "_AU";
				const auto& source = makeTriggerSourceByRelationUpdate(keyFieldBlocks, sqlDialect, relationName);
				auto trigger = make_unique<FTSTrigger>(
					triggerName,
					relationName,
					"UPDATE",
					position,
					source
					);
				triggers.push_back(std::move(trigger));
			}
			{
				// DELETE
				const string triggerName = "FTS$" + relationName + "_AD";
				const auto& source = makeTriggerSourceByRelationDelete(keyFieldBlocks, sqlDialect, relationName);
				auto trigger = make_unique<FTSTrigger>(
					triggerName,
					relationName,
					"DELETE",
					position,
					source
					);
				triggers.push_back(std::move(trigger));
			}
		}
	}

	const string FTSTriggerHelper::makeTriggerSourceByRelationMulti(
		const FTSKeyFieldBlockMap& keyFieldBlocks,
		const unsigned int sqlDialect,
		const string& relationName
	)
	{
		string triggerSource =
			"AS\n"
			"BEGIN\n";

		for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
			const auto& procedureName = keyFieldBlock->getProcedureName();
			const string metaKeyFieldName = escapeMetaName(sqlDialect, keyFieldName);
			const string keycodeBlock =
				"  /* Block for key " + keyFieldName + " */\n"
				"  IF (INSERTING AND (" + keyFieldBlock->insertingCondition + ")) THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', NEW." + metaKeyFieldName + ", 'I');\n"
				"  IF (UPDATING AND (" + keyFieldBlock->updatingCondition + ")) THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', OLD." + metaKeyFieldName + ", 'U');\n"
				"  IF (DELETING AND (" + keyFieldBlock->deletingCondition + ")) THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', OLD." + metaKeyFieldName + ", 'D');\n";
			triggerSource += keycodeBlock;
		}

		triggerSource +=
			"END";
		return triggerSource;
	}

	const string FTSTriggerHelper::makeTriggerSourceByRelationInsert(
		const FTSKeyFieldBlockMap& keyFieldBlocks,
		const unsigned int sqlDialect,
		const string& relationName
	)
	{
		string triggerSource =
			"AS\n"
			"BEGIN\n";

		for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
			const auto& procedureName = keyFieldBlock->getProcedureName();
			const string metaKeyFieldName = escapeMetaName(sqlDialect, keyFieldName);
			const string keycodeBlock =
				"  /* Block for key " + keyFieldName + " */\n"
				"  IF (" + keyFieldBlock->insertingCondition + ") THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', NEW." + metaKeyFieldName + ", 'I');\n";
			triggerSource += keycodeBlock;
		}

		triggerSource +=
			"END";
		return triggerSource;
	}

	const string FTSTriggerHelper::makeTriggerSourceByRelationUpdate(
		const FTSKeyFieldBlockMap& keyFieldBlocks,
		const unsigned int sqlDialect,
		const string& relationName
	)
	{
		string triggerSource =
			"AS\n"
			"BEGIN\n";

		for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
			const auto& procedureName = keyFieldBlock->getProcedureName();
			const string metaKeyFieldName = escapeMetaName(sqlDialect, keyFieldName);
			const string keycodeBlock =
				"  /* Block for key " + keyFieldName + " */\n"
				"  IF (" + keyFieldBlock->updatingCondition + ") THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', OLD." + metaKeyFieldName + ", 'U');\n";
			triggerSource += keycodeBlock;
		}

		triggerSource +=
			"END";
		return triggerSource;
	}

	const string FTSTriggerHelper::makeTriggerSourceByRelationDelete(
		const FTSKeyFieldBlockMap& keyFieldBlocks,
		const unsigned int sqlDialect,
		const string& relationName
	)
	{
		string triggerSource =
			"AS\n"
			"BEGIN\n";

		for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
			const auto& procedureName = keyFieldBlock->getProcedureName();
			const string metaKeyFieldName = escapeMetaName(sqlDialect, keyFieldName);
			const string keycodeBlock =
				"  /* Block for key " + keyFieldName + " */\n"
				"  IF (" + keyFieldBlock->deletingCondition + ") THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', OLD." + metaKeyFieldName + ", 'D');\n";
			triggerSource += keycodeBlock;
		}

		triggerSource +=
			"END";
		return triggerSource;
	}

}