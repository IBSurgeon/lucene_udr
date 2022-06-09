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
		string relationName;
		RelationType relationType;
		bool systemFlag;
	public:
		RelationInfo()
			: relationName()
			, relationType(RelationType::RT_REGULAR)
			, systemFlag(false)
		{}

		bool findKeyFieldSupported() {
			return (relationType == RelationType::RT_REGULAR || relationType == RelationType::RT_GTT_PRESERVE_ROWS || relationType == RelationType::RT_GTT_DELETE_ROWS);
		}
	};
	using RelationInfoPtr = unique_ptr<RelationInfo>;

	class RelationFieldInfo final
	{
	public:
		string relationName;
		string fieldName;
		short  fieldType;
		short fieldLength;
		short charLength;
		short charsetId;
		short fieldSubType;
		short fieldPrecision;
		short fieldScale;
	public:
		RelationFieldInfo()
			: relationName()
			, fieldName()
			, fieldType(0)
			, fieldLength(0)
			, charLength(0)
			, charsetId(0)
			, fieldSubType(0)
			, fieldPrecision(0)
			, fieldScale(0)
		{
		}

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
}

#endif	// FTS_RELATIONS_H
