/**
 *  Firebird Relation Helper.
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

#include "Relations.h"

#include "FBUtils.h"

using namespace Firebird;
using namespace LuceneUDR;

namespace 
{

    // SQL texts
    constexpr char SQL_RELATION_INFO[] = R"SQL(
SELECT
  TRIM(R.RDB$RELATION_NAME) AS RDB$RELATION_NAME,
  CASE
    WHEN R.RDB$RELATION_TYPE IS NOT NULL THEN R.RDB$RELATION_TYPE
    ELSE IIF(R.RDB$VIEW_BLR IS NULL, 0, 1)
  END AS RDB$RELATION_TYPE,
  COALESCE(R.RDB$SYSTEM_FLAG, 0) AS RDB$SYSTEM_FLAG
FROM RDB$RELATIONS R
WHERE R.RDB$RELATION_NAME = ?
)SQL";

    constexpr char SQL_RELATION_EXISTS[] = R"SQL(
SELECT COUNT(*) AS CNT
FROM RDB$RELATIONS
WHERE RDB$RELATION_NAME = ?
)SQL";

    constexpr char SQL_RELATION_FIELDS[] = R"SQL(
SELECT
    TRIM(RF.RDB$RELATION_NAME) AS RDB$RELATION_NAME
  , TRIM(RF.RDB$FIELD_NAME) AS RDB$FIELD_NAME
  , F.RDB$FIELD_TYPE
  , F.RDB$FIELD_LENGTH
  , F.RDB$CHARACTER_LENGTH
  , F.RDB$CHARACTER_SET_ID
  , F.RDB$FIELD_SUB_TYPE
  , F.RDB$FIELD_PRECISION
  , F.RDB$FIELD_SCALE
FROM RDB$RELATION_FIELDS RF
JOIN RDB$FIELDS F
  ON F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE
WHERE RF.RDB$RELATION_NAME = ?
)SQL";

    constexpr char SQL_RELATION_KEY_FIELDS[] = R"SQL(
SELECT
    TRIM(RF.RDB$RELATION_NAME) AS RDB$RELATION_NAME
  , TRIM(RF.RDB$FIELD_NAME) AS RDB$FIELD_NAME
  , F.RDB$FIELD_TYPE
  , F.RDB$FIELD_LENGTH
  , F.RDB$CHARACTER_LENGTH
  , F.RDB$CHARACTER_SET_ID
  , F.RDB$FIELD_SUB_TYPE
  , F.RDB$FIELD_PRECISION
  , F.RDB$FIELD_SCALE
FROM RDB$RELATION_CONSTRAINTS RC
JOIN RDB$INDEX_SEGMENTS INDS
  ON INDS.RDB$INDEX_NAME = RC.RDB$INDEX_NAME
JOIN RDB$RELATION_FIELDS RF
  ON RF.RDB$RELATION_NAME = RC.RDB$RELATION_NAME
 AND RF.RDB$FIELD_NAME = INDS.RDB$FIELD_NAME
JOIN RDB$FIELDS F
  ON F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE
WHERE RC.RDB$RELATION_NAME = ?
  AND RC.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
)SQL";

    constexpr char SQL_RELATION_FIELD[] = R"SQL(
SELECT
    TRIM(RF.RDB$RELATION_NAME) AS RDB$RELATION_NAME
  , TRIM(RF.RDB$FIELD_NAME) AS RDB$FIELD_NAME
  , F.RDB$FIELD_TYPE
  , F.RDB$FIELD_LENGTH
  , F.RDB$CHARACTER_LENGTH
  , F.RDB$CHARACTER_SET_ID
  , F.RDB$FIELD_SUB_TYPE
  , F.RDB$FIELD_PRECISION
  , F.RDB$FIELD_SCALE
FROM RDB$RELATION_FIELDS RF
JOIN RDB$FIELDS F
  ON F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE
WHERE RF.RDB$RELATION_NAME = ? AND RF.RDB$FIELD_NAME = ?
)SQL";

    constexpr char SQL_RELATION_FIELD_EXISTS[] = R"SQL(
SELECT COUNT(*) AS CNT
FROM RDB$RELATION_FIELDS
WHERE RDB$RELATION_NAME = ? AND RDB$FIELD_NAME = ?
)SQL";

}

namespace FTSMetadata
{

    RelationFieldInfo::RelationFieldInfo(
        std::string_view relationName_,
        std::string_view fieldName_,
        short fieldType_,
        short fieldLength_,
        short charLength_,
        short charsetId_,
        short fieldSubType_,
        short fieldPrecision_,
        short fieldScale_
    )
        : relationName(relationName_)
        , fieldName(fieldName_)
        , fieldType(fieldType_)
        , fieldLength(fieldLength_)
        , charLength(charLength_)
        , charsetId(charsetId_)
        , fieldSubType(fieldSubType_)
        , fieldPrecision(fieldPrecision_)
        , fieldScale(fieldScale_)
        , dbKeyFlag(fieldName_ == "RDB$DB_KEY")
    {}


    RelationHelper::RelationHelper(IMaster* master)
        : m_master(master)
    {}

    RelationHelper::~RelationHelper() = default;

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
    RelationInfo RelationHelper::getRelationInfo(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view relationName)
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

        if (!m_stmt_get_relation.hasData()) {
            m_stmt_get_relation.reset(att->prepare(
                status,
                tra,
                0,
                SQL_RELATION_INFO,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_get_relation->openCursor(
            status,
            tra,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            0
        ));

        int result = rs->fetchNext(status, output.getData());
        rs->close(status);
        rs.release();

        if (result == IStatus::RESULT_NO_DATA) {
            std::string sRelationName(relationName);
            throwException(status, R"(Relation "%s" not exists)", sRelationName.c_str());
        }
        
        
        std::string_view swRelationName(output->relationName.str, output->relationName.length);

        return RelationInfo(
            swRelationName, 
            static_cast<RelationType>(output->relationType), 
            static_cast<bool>(output->systemFlag)
        );
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
        unsigned int sqlDialect,
        std::string_view relationName)
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

        if (!m_stmt_exists_relation.hasData()) {
            m_stmt_exists_relation.reset(att->prepare(
                status,
                tra,
                0,
                SQL_RELATION_EXISTS,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_exists_relation->openCursor(
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
        rs.release();

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
    RelationFieldList RelationHelper::fillRelationFields(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view relationName
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

        if (!m_stmt_relation_fields.hasData()) {
            m_stmt_relation_fields.reset(att->prepare(
                status,
                tra,
                0,
                SQL_RELATION_FIELDS,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }
    
        RelationFieldList fields;

        AutoRelease<IResultSet> rs(m_stmt_relation_fields->openCursor(
            status,
            tra,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            0
        ));

        while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
            fields.emplace_back(
                std::string_view(output->relationName.str, output->relationName.length),
                std::string_view(output->fieldName.str, output->fieldName.length),
                output->fieldType,
                output->fieldLength,
                output->charLength,
                output->charsetId,
                output->fieldSubType,
                output->fieldPrecision,
                output->fieldScale
            );
        }

        rs->close(status);
        rs.release();

        return fields;
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
    RelationFieldList RelationHelper::fillPrimaryKeyFields(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view relationName
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

        if (!m_stmt_pk_fields.hasData()) {
            m_stmt_pk_fields.reset(att->prepare(
                status,
                tra,
                0,
                SQL_RELATION_KEY_FIELDS,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }

        RelationFieldList keyFields;

        AutoRelease<IResultSet> rs(m_stmt_pk_fields->openCursor(
            status,
            tra,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            0
        ));

        while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
            keyFields.emplace_back(
                std::string_view(output->relationName.str, output->relationName.length),
                std::string_view(output->fieldName.str, output->fieldName.length),
                output->fieldType,
                output->fieldLength,
                output->charLength,
                output->charsetId,
                output->fieldSubType,
                output->fieldPrecision,
                output->fieldScale
            );
        }

        rs->close(status);
        rs.release();

        return keyFields;
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
    RelationFieldInfo RelationHelper::getField(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view relationName,
        std::string_view fieldName)
    {

        if (fieldName == "RDB$DB_KEY") {
            // special case
            return RelationFieldInfo(
                relationName,
                "RDB$DB_KEY",
                14,
                8,
                8,
                1,
                0,
                0,
                0
            );
        }

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

        if (!m_stmt_get_field.hasData()) {
            m_stmt_get_field.reset(att->prepare(
                status,
                tra,
                0,
                SQL_RELATION_FIELD,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_get_field->openCursor(
            status,
            tra,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            0
        ));

        int result = rs->fetchNext(status, output.getData());
        rs->close(status);
        rs.release();


        if (result == IStatus::RESULT_NO_DATA) {
            std::string sFieldName(fieldName);
            std::string sRelationName(relationName);
            throwException(status, R"(Field "%s" not found in relation "%s".)", sFieldName.c_str(), sRelationName.c_str());
        }

        return RelationFieldInfo(
            std::string_view(output->relationName.str, output->relationName.length),
            std::string_view(output->fieldName.str, output->fieldName.length),
            output->fieldType,
            output->fieldLength,
            output->charLength,
            output->charsetId,
            output->fieldSubType,
            output->fieldPrecision,
            output->fieldScale
        );
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
        unsigned int sqlDialect,
        std::string_view relationName,
        std::string_view fieldName)
    {

        if (fieldName == "RDB$DB_KEY")
            return true;

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

        if (!m_stmt_exists_field.hasData()) {
            m_stmt_exists_field.reset(att->prepare(
                status,
                tra,
                0,
                SQL_RELATION_FIELD_EXISTS,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_exists_field->openCursor(
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
        rs.release();

        return foundFlag;
    }

}
