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

using namespace LuceneUDR;
using namespace Firebird;

namespace FTSMetadata
{

    FTSKeyFieldBlock::FTSKeyFieldBlock(const std::string& aKeyFieldName, FTSKeyType aKeyFieldType)
        : keyFieldName(aKeyFieldName)
        , keyFieldType(aKeyFieldType)
        , fieldNames()
        , insertingCondition{}
        , updatingCondition{}
        , deletingCondition{}
    {}

    std::string FTSKeyFieldBlock::makeInsertSQL(const std::string& relationName, char opType, unsigned int sqlDialect) const
    {
        std::string context;
        switch (opType) {
        case 'I':
            context = "NEW.";
            break;
        case 'U':
        case 'D':
            context = "OLD.";
            break;
        }

        switch (keyFieldType) {
        case FTSKeyType::DB_KEY:
        {
            return "INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$DB_KEY, FTS$CHANGE_TYPE) "
                "VALUES('" + relationName + "', " + context + escapeMetaName(sqlDialect, keyFieldName) + ", '" + opType + "')";
        }
        case FTSKeyType::INT_ID:
        {
            return "INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_ID, FTS$CHANGE_TYPE) "
                "VALUES('" + relationName + "', " + context + escapeMetaName(sqlDialect, keyFieldName) + ", '" + opType + "')";
        }
        case FTSKeyType::UUID:
        {
            return "INSERT INTO FTS$LOG(FTS$RELATION_NAME, FTS$REC_UUID, FTS$CHANGE_TYPE) "
                "VALUES('" + relationName + "', " + context + escapeMetaName(sqlDialect, keyFieldName) + ", '" + opType + "')";
        }
        default:
            return {};
        }
    }

    std::string FTSTrigger::getHeader(unsigned int sqlDialect) const
    {
        std::string triggerHeader =
            "CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
            "ACTIVE AFTER " + triggerEvents + "\n"
            "POSITION " + std::to_string(position) + "\n";
        return triggerHeader;
    }

    std::string FTSTrigger::getScript(unsigned int sqlDialect) const
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
    /// 
    FTSKeyFieldBlockMap FTSTriggerHelper::fillKeyFieldBlocks(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        const std::string& relationName
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

        AutoRelease<IResultSet> rs(att->openCursor(
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
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            nullptr,
            0
        ));


        FTSKeyFieldBlockMap keyFieldBlocks;
        while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
            const std::string keyFieldName(output->keyFieldName.str, output->keyFieldName.length);
            const std::string_view sKeyFieldType(output->keyFieldType.str, output->keyFieldType.length);
            const std::string fieldName(output->fieldName.str, output->fieldName.length);

            FTSKeyType keyFieldType = FTSKeyTypeFromString(sKeyFieldType);

            auto [it, result] = keyFieldBlocks.try_emplace(
                keyFieldName, 
                keyFieldName,
                keyFieldType
            );
            auto&& block = it->second;
            block.fieldNames.insert(fieldName);

        }
        rs->close(status);
        rs.release();

        return keyFieldBlocks;
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
    FTSTriggerList FTSTriggerHelper::makeTriggerSourceByRelation(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        const std::string& relationName,
        bool multiAction,
        short position
    )
    {
        FTSKeyFieldBlockMap keyFieldBlocks = fillKeyFieldBlocks(status, att, tra, sqlDialect, relationName);
        for (auto&& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
            if (keyFieldBlock.fieldNames.empty()) {
                continue;
            }

            for (const auto& fieldName : keyFieldBlock.fieldNames) {
                const std::string metaFieldName = escapeMetaName(sqlDialect, fieldName);

                if (!keyFieldBlock.insertingCondition.empty()) {
                    keyFieldBlock.insertingCondition += "\n      OR ";
                }
                keyFieldBlock.insertingCondition += "NEW." + metaFieldName + " IS NOT NULL";

                if (!keyFieldBlock.updatingCondition.empty()) {
                    keyFieldBlock.updatingCondition += "\n      OR ";
                }
                keyFieldBlock.updatingCondition += "NEW." + metaFieldName + " IS DISTINCT FROM " + "OLD." + metaFieldName;

                if (!keyFieldBlock.deletingCondition.empty()) {
                    keyFieldBlock.deletingCondition += "\n      OR ";
                }
                keyFieldBlock.deletingCondition += "OLD." + metaFieldName + " IS NOT NULL";
            }
        }

        FTSTriggerList triggers;
        if (multiAction) {
            const std::string triggerName = "FTS$" + relationName + "_AIUD";
            const std::string source = makeTriggerSourceByRelationMulti(keyFieldBlocks, sqlDialect, relationName);

            triggers.emplace_back(
                triggerName,
                relationName,
                "INSERT OR UPDATE OR DELETE",
                position,
                source
            );
        }
        else {
            {
                // INSERT
                const std::string triggerName = "FTS$" + relationName + "_AI";
                const std::string source = makeTriggerSourceByRelationInsert(keyFieldBlocks, sqlDialect, relationName);
                triggers.emplace_back(
                    triggerName,
                    relationName,
                    "INSERT",
                    position,
                    source
                );
            }
            {
                // UPDATE
                const std::string triggerName = "FTS$" + relationName + "_AU";
                const std::string source = makeTriggerSourceByRelationUpdate(keyFieldBlocks, sqlDialect, relationName);
                triggers.emplace_back(
                    triggerName,
                    relationName,
                    "UPDATE",
                    position,
                    source
                );
            }
            {
                // DELETE
                const std::string triggerName = "FTS$" + relationName + "_AD";
                const std::string source = makeTriggerSourceByRelationDelete(keyFieldBlocks, sqlDialect, relationName);
                triggers.emplace_back(
                    triggerName,
                    relationName,
                    "DELETE",
                    position,
                    source
                );
            }
        }
        return triggers;
    }

    std::string FTSTriggerHelper::makeTriggerSourceByRelationMulti(
        const FTSKeyFieldBlockMap& keyFieldBlocks,
        unsigned int sqlDialect,
        const std::string& relationName
    )
    {
        std::string triggerSource =
            "AS\n"
            "BEGIN\n";

        for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
            std::string keycodeBlock =
                "  /* Block for key " + keyFieldName + " */\n";
            keycodeBlock +=
                "  IF (INSERTING AND (" + keyFieldBlock.insertingCondition + ")) THEN\n"
                "    " + keyFieldBlock.makeInsertSQL(relationName, 'I', sqlDialect) + ";\n";
            keycodeBlock +=
                "  IF (UPDATING AND (" + keyFieldBlock.updatingCondition + ")) THEN\n"
                "    " + keyFieldBlock.makeInsertSQL(relationName, 'U', sqlDialect) + ";\n";
            keycodeBlock +=
                "  IF (DELETING AND (" + keyFieldBlock.deletingCondition + ")) THEN\n"
                "    " + keyFieldBlock.makeInsertSQL(relationName, 'D', sqlDialect) + ";\n";
            triggerSource += keycodeBlock;
        }

        triggerSource +=
            "END";
        return triggerSource;
    }

    std::string FTSTriggerHelper::makeTriggerSourceByRelationInsert(
        const FTSKeyFieldBlockMap& keyFieldBlocks,
        unsigned int sqlDialect,
        const std::string& relationName
    )
    {
        std::string triggerSource =
            "AS\n"
            "BEGIN\n";

        for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
            const std::string keycodeBlock =
                "  /* Block for key " + keyFieldName + " */\n"
                "  IF (" + keyFieldBlock.insertingCondition + ") THEN\n"
                "    " + keyFieldBlock.makeInsertSQL(relationName, 'I', sqlDialect) + ";\n";
            triggerSource += keycodeBlock;
        }

        triggerSource +=
            "END";
        return triggerSource;
    }

    std::string FTSTriggerHelper::makeTriggerSourceByRelationUpdate(
        const FTSKeyFieldBlockMap& keyFieldBlocks,
        unsigned int sqlDialect,
        const std::string& relationName
    )
    {
        std::string triggerSource =
            "AS\n"
            "BEGIN\n";

        for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
            const std::string keycodeBlock =
                "  /* Block for key " + keyFieldName + " */\n"
                "  IF (" + keyFieldBlock.updatingCondition + ") THEN\n"
                "    " + keyFieldBlock.makeInsertSQL(relationName, 'U', sqlDialect) + ";\n";
            triggerSource += keycodeBlock;
        }

        triggerSource +=
            "END";
        return triggerSource;
    }

    std::string FTSTriggerHelper::makeTriggerSourceByRelationDelete(
        const FTSKeyFieldBlockMap& keyFieldBlocks,
        unsigned int sqlDialect,
        const std::string& relationName
    )
    {
        std::string triggerSource =
            "AS\n"
            "BEGIN\n";

        for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
            const std::string keycodeBlock =
                "  /* Block for key " + keyFieldName + " */\n"
                "  IF (" + keyFieldBlock.deletingCondition + ") THEN\n"
                "    " + keyFieldBlock.makeInsertSQL(relationName, 'D', sqlDialect) + ";\n";
            triggerSource += keycodeBlock;
        }

        triggerSource +=
            "END";
        return triggerSource;
    }

}
