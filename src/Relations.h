#ifndef FTS_RELATIONS_H
#define FTS_RELATIONS_H

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

#include "LuceneUdr.h"
#include "FBUtils.h"
#include <string>
#include <sstream>
#include <list>
#include <memory>

using namespace Firebird;
using namespace std;

namespace LuceneUDR
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
		string relationName{ "" };
		RelationType relationType{ RelationType::RT_REGULAR };
		bool systemFlag = false;
	public:
		RelationInfo() = default;

		bool findKeyFieldSupported() {
			return (relationType == RelationType::RT_REGULAR || relationType == RelationType::RT_GTT_PRESERVE_ROWS || relationType == RelationType::RT_GTT_DELETE_ROWS);
		}
	};
	using RelationInfoPtr = unique_ptr<RelationInfo>;

	class RelationFieldInfo final
	{
	public:
		string relationName{ "" };
		string fieldName{ "" };
		short  fieldType = 0;
		short fieldLength = 0;
		short charLength = 0;
		short charsetId = 0;
		short fieldSubType = 0;
		short fieldPrecision = 0;
		short fieldScale = 0;
	public:
		RelationFieldInfo() = default;

		bool isInt() {
			return (fieldScale == 0) && (fieldType == 7 || fieldType == 8 || fieldType == 16 || fieldType == 26);
		}

		bool isFixedChar() {
			return (fieldType == 14);
		}

		bool isVarChar() {
			return (fieldType == 37);
		}

		bool isBlob() {
			return (fieldType == 261);
		}

		bool isBinary() {
			return (isBlob() && fieldSubType == 0) || ((isFixedChar() || isVarChar()) && charsetId == 1);
		}

		void initDB_KEYField(const string& aRelationName) {
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

	using RelationFieldInfoPtr = unique_ptr<RelationFieldInfo>;
	using RelationFieldList = list<RelationFieldInfoPtr>;

	class RelationHelper final
	{
	private:
		IMaster* m_master = nullptr;
		// prepared statements
		AutoRelease<IStatement> m_stmt_get_relation{ nullptr };
		AutoRelease<IStatement> m_stmt_exists_relation{ nullptr };
		AutoRelease<IStatement> m_stmt_relation_fields{ nullptr };
		AutoRelease<IStatement> m_stmt_pk_fields{ nullptr };
		AutoRelease<IStatement> m_stmt_get_field{ nullptr };
		AutoRelease<IStatement> m_stmt_exists_field{ nullptr };

		// SQL texts
		const char* SQL_RELATION_INFO = 
R"SQL(
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

		const char* SQL_RELATION_EXISTS =
R"SQL(
SELECT COUNT(*) AS CNT
FROM RDB$RELATIONS
WHERE RDB$RELATION_NAME = ?
)SQL";

		const char* SQL_RELATION_FIELDS =
R"SQL(
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

		const char* SQL_RELATION_KEY_FIELDS =
R"SQL(
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

		const char* SQL_RELATION_FIELD =
R"SQL(
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

		const char* SQL_RELATION_FIELD_EXISTS = 
R"SQL(
SELECT COUNT(*) AS CNT
FROM RDB$RELATION_FIELDS
WHERE RDB$RELATION_NAME = ? AND RDB$FIELD_NAME = ?
)SQL";
	public:
		RelationHelper() = delete;

		explicit RelationHelper(IMaster* master)
			: m_master(master)
		{}

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
		RelationInfoPtr getRelationInfo(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& relationName
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
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string &relationName);

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
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& relationName,
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
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& relationName,
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
		/// <param name="relationName">Relation name</param>
		/// <param name="fieldName">Field name</param>
		/// 
		/// <returns>Returns information about the field.</returns>
		RelationFieldInfoPtr getField(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& relationName,
			const string& fieldName
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
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string &relationName,
			const string &fieldName);
	};

	using RelationHelperPtr = unique_ptr<RelationHelper>;
}

#endif	// FTS_RELATIONS_H
