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

	struct RelationInfo
	{
		string relationName;
		RelationType relationType;
		bool systemFlag;

		bool findKeyFieldSupported() {
			return (relationType == RelationType::RT_REGULAR || relationType == RelationType::RT_GTT_PRESERVE_ROWS || relationType == RelationType::RT_GTT_DELETE_ROWS);
		}
	};

	struct RelationFieldInfo
	{
		string relationName;
		string fieldName;
		short  fieldType;
		short fieldLength;
		short charLength;
		short charsetId;
		short fieldSubType;
		short fieldPrecision;
		short fieldScale;

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

	using RelationFieldList = list<RelationFieldInfo>;

	class RelationHelper final
	{
	private:
		IMaster* m_master;
		// prepared statements
		AutoRelease<IStatement> stmt_get_relation;
		AutoRelease<IStatement> stmt_exists_relation;
		AutoRelease<IStatement> stmt_relation_fields;
		AutoRelease<IStatement> stmt_pk_fields;
		AutoRelease<IStatement> stmt_get_field;
		AutoRelease<IStatement> stmt_exists_field;
	public:
		RelationHelper()
			: RelationHelper(nullptr)
		{}

		RelationHelper(IMaster* master)
			: m_master(master)
			, stmt_get_relation(nullptr)
			, stmt_exists_relation(nullptr)
			, stmt_relation_fields(nullptr)
			, stmt_pk_fields(nullptr)
			, stmt_get_field(nullptr)
			, stmt_exists_field(nullptr)
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
		RelationInfo getRelationInfo(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
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
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
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
		/// 
		/// <returns>Returns a list of relations fields.</returns>
		RelationFieldList getFields(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string& relationName
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
		/// <returns>Returns a list of relations primary key fields.</returns>
		RelationFieldList getPrimaryKeyFields(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string& relationName
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
		RelationFieldInfo getField(
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
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
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &relationName,
			const string &fieldName);

		/// <summary>
		/// Builds an SQL query to retrieve field values. 
		/// </summary>
		/// 
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="fieldNames">List of field names </param>
		/// <param name="keyFieldName">Key field name</param>
		/// <param name="whereKey">If true, then a filtering condition by key field is added, 
		/// otherwise an SQL query will be built without filtering, that is, returning all records.</param>
		/// 
		/// <returns>Returns the text of the SQL query.</returns>
		static inline string buildSqlSelectFieldValues(
			const unsigned int sqlDialect, 
			const string& relationName, 
			list<string> fieldNames, 
			const string& keyFieldName,
			const bool whereKey = false)
		{
			std::stringstream ss;
			ss << "SELECT\n";
			int field_cnt = 0;
			for (const auto fieldName : fieldNames) {
				if (field_cnt == 0) {
					ss << "  " << escapeMetaName(sqlDialect, fieldName);
				}
				else {
					ss << ",\n  " << escapeMetaName(sqlDialect, fieldName);
				}
				field_cnt++;
			}
			ss << "\nFROM " << escapeMetaName(sqlDialect, relationName);
			ss << "\nWHERE ";
			if (whereKey) {
				ss << escapeMetaName(sqlDialect, keyFieldName) << " = ?";
			}
			else {
				ss << escapeMetaName(sqlDialect, keyFieldName) << " IS NOT NULL";
				string where;
				for (const auto fieldName : fieldNames) {
					if (fieldName == keyFieldName) continue;
					if (where.empty())
						where += escapeMetaName(sqlDialect, fieldName) + " IS NOT NULL";
					else
						where += " OR " + escapeMetaName(sqlDialect, fieldName) + " IS NOT NULL";
				}
				if (!where.empty())
				   ss << "\nAND (" << where << ")";
			}
			return ss.str();
		}
	};
}

#endif	// FTS_RELATIONS_H
