#ifndef FTS_RELATIONS_H
#define FTS_RELATIONS_H

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

#include <list>
#include <memory>
#include <string>

#include "LuceneUdr.h"

namespace FTSMetadata
{
    enum class RelationType {
       RT_REGULAR,
       RT_VIEW,
       RT_EXTERNAL,
       RT_VIRTUAL,
       RT_GTT_PRESERVE_ROWS,
       RT_GTT_DELETE_ROWS
    };

    struct RelationInfo final
    {
        std::string relationName;
        RelationType relationType{ RelationType::RT_REGULAR };
        bool systemFlag = false;
    
        RelationInfo() = default;
        RelationInfo(std::string_view a_RelationName, RelationType a_RelationType, bool a_SystemFlag)
            : relationName(a_RelationName)
            , relationType(a_RelationType)
            , systemFlag(a_SystemFlag)
        {
        }

        bool findKeyFieldSupported() const {
            return (relationType == RelationType::RT_REGULAR || relationType == RelationType::RT_GTT_PRESERVE_ROWS || relationType == RelationType::RT_GTT_DELETE_ROWS);
        }
    };

    class RelationFieldInfo final
    {
    public:
        std::string relationName;
        std::string fieldName;
        short fieldType = 0;
        short fieldLength = 0;
        short charLength = 0;
        short charsetId = 0;
        short fieldSubType = 0;
        short fieldPrecision = 0;
        short fieldScale = 0;
        bool dbKeyFlag = false;
    public:
        RelationFieldInfo() = default;
        RelationFieldInfo(
            std::string_view relationName_,
            std::string_view fieldName_,
            short fieldType_,
            short fieldLength_,
            short charLength_,
            short charsetId_,
            short fieldSubType_,
            short fieldPrecision_,
            short fieldScale_
       );

        RelationFieldInfo(const RelationFieldInfo&) = delete;
        RelationFieldInfo(RelationFieldInfo&&) noexcept = default;

        RelationFieldInfo& operator=(const RelationFieldInfo&) = delete;
        RelationFieldInfo& operator=(RelationFieldInfo&&) noexcept = default;

        bool isInt() const {
            return (fieldScale == 0) && (fieldType == 7 || fieldType == 8 || fieldType == 16 || fieldType == 26);
        }

        bool isFixedChar() const {
            return (fieldType == 14);
        }

        bool isVarChar() const {
            return (fieldType == 37);
        }

        bool isBlob() const {
            return (fieldType == 261);
        }

        bool isBinary() const {
            return (isBlob() && fieldSubType == 0) || ((isFixedChar() || isVarChar()) && charsetId == 1);
        }

        bool isDbKey() const {
            return dbKeyFlag;
        }

        bool ftsKeySupported() const {
            // Supported types SMALLINT, INTEGER, BIGINT, CHAR(16) CHARACTER SET OCTETS, BINARY(16)
            return isInt() || (isFixedChar() && isBinary() && fieldLength == 16);
        }
    };

    using RelationFieldList = std::list<RelationFieldInfo>;

    class RelationHelper final
    {
    private:
        Firebird::IMaster* m_master{ nullptr };
        // prepared statements
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_get_relation;
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_exists_relation;
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_relation_fields;
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_pk_fields;
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_get_field;
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_exists_field;

    public:
        RelationHelper() = delete;

        explicit RelationHelper(Firebird::IMaster* master);

        ~RelationHelper();

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
        RelationInfo getRelationInfo(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view relationName
        );

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
        bool relationExists(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view relationName);

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
        RelationFieldList fillRelationFields(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view relationName
        );

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
        RelationFieldList fillPrimaryKeyFields(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view relationName
        );

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
        RelationFieldInfo getField(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view relationName,
            std::string_view fieldName
        );

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
        bool fieldExists(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view relationName,
            std::string_view fieldName);
    };

    using RelationHelperPtr = std::unique_ptr<RelationHelper>;
}

#endif // FTS_RELATIONS_H
