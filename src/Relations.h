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

#include "LuceneUdr.h"
#include <string>
#include <list>
#include <memory>

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

    class RelationInfo final
    {
    public:
        std::string relationName{ "" };
        RelationType relationType{ RelationType::RT_REGULAR };
        bool systemFlag = false;
    public:
        RelationInfo() = default;

        bool findKeyFieldSupported() const {
            return (relationType == RelationType::RT_REGULAR || relationType == RelationType::RT_GTT_PRESERVE_ROWS || relationType == RelationType::RT_GTT_DELETE_ROWS);
        }
    };

    class RelationFieldInfo final
    {
    public:
        std::string relationName{ "" };
        std::string fieldName{ "" };
        short  fieldType = 0;
        short fieldLength = 0;
        short charLength = 0;
        short charsetId = 0;
        short fieldSubType = 0;
        short fieldPrecision = 0;
        short fieldScale = 0;
    public:
        RelationFieldInfo() = default;

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

        void initDB_KEYField(const std::string& aRelationName) {
            relationName = aRelationName;
            fieldName = "RDB$DB_KEY";
            fieldType = 14;
            fieldLength = 8;
            charLength = 8;
            charsetId = 1;
            fieldSubType = 0;
            fieldPrecision = 0;
            fieldScale = 0;
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
        /// <param name="relationInfo">Information about the relation</param>
        /// <param name="relationName">Relation name</param>
        /// 
        void getRelationInfo(
            Firebird::ThrowStatusWrapper* const status,
            Firebird::IAttachment* const att,
            Firebird::ITransaction* const tra,
            unsigned int sqlDialect,
            RelationInfo& relationInfo,
            const std::string& relationName
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
            Firebird::ThrowStatusWrapper* const status,
            Firebird::IAttachment* const att,
            Firebird::ITransaction* const tra,
            unsigned int sqlDialect,
            const std::string &relationName);

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
        void fillRelationFields(
            Firebird::ThrowStatusWrapper* const status,
            Firebird::IAttachment* const att,
            Firebird::ITransaction* const tra,
            unsigned int sqlDialect,
            const std::string& relationName,
            RelationFieldList& fields
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
        /// <param name="keyFields">List of relations primary key fields</param>
        /// 
        void fillPrimaryKeyFields(
            Firebird::ThrowStatusWrapper* const status,
            Firebird::IAttachment* const att,
            Firebird::ITransaction* const tra,
            unsigned int sqlDialect,
            const std::string& relationName,
            RelationFieldList& keyFields
        );

        /// <summary>
        /// Returns information about the field.
        /// </summary>
        /// 
        /// <param name="status">Firebird status</param>
        /// <param name="att">Firebird attachment</param>
        /// <param name="tra">Firebird transaction</param>
        /// <param name="sqlDialect">SQL dialect</param>
        /// <param name="fieldInfo">Information about the field</param>
        /// <param name="relationName">Relation name</param>
        /// <param name="fieldName">Field name</param>
        /// 
        void getField(
            Firebird::ThrowStatusWrapper* const status,
            Firebird::IAttachment* const att,
            Firebird::ITransaction* const tra,
            unsigned int sqlDialect,
            RelationFieldInfo& fieldInfo,
            const std::string& relationName,
            const std::string& fieldName
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
            Firebird::ThrowStatusWrapper* const status,
            Firebird::IAttachment* const att,
            Firebird::ITransaction* const tra,
            unsigned int sqlDialect,
            const std::string &relationName,
            const std::string &fieldName);
    };

    using RelationHelperPtr = std::unique_ptr<RelationHelper>;
}

#endif	// FTS_RELATIONS_H
