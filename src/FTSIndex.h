#ifndef FTS_INDEX_H
#define FTS_INDEX_H

/**
 *  Utilities for getting and managing metadata for full-text indexes.
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
#include <unordered_set>
#include <map>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace std;

namespace FTSMetadata
{

	enum class FTSKeyType {NONE, DB_KEY, INT_ID, UUID};

	class FTSIndexSegment;

	using FTSIndexSegmentPtr = unique_ptr<FTSIndexSegment>;
	using FTSIndexSegmentList = list<FTSIndexSegmentPtr>;
	using FTSIndexSegmentsMap = map<string, FTSIndexSegmentList>;


	/// <summary>
	/// Full-text index metadata.
	/// </summary>
	class FTSIndex final
	{
	public:
		string indexName{ "" };
		string relationName{ "" };
		string analyzer{ "" };
		string description{ "" };
		string status{ "" }; // N - new index, I - inactive, U - need rebuild, C - complete

		FTSIndexSegmentList segments;

		FTSKeyType keyFieldType{ FTSKeyType::NONE };
		wstring unicodeKeyFieldName{ L"" };
		wstring unicodeIndexDir{ L"" };
	private:
		AutoRelease<IStatement> m_stmtExtractRecord{ nullptr };
		AutoRelease<IMessageMetadata> m_inMetaExtractRecord{ nullptr };
		AutoRelease<IMessageMetadata> m_outMetaExtractRecord{ nullptr };
	public: 

		FTSIndex() = default;

		bool inline isActive() {
			return (status == "C") || (status == "U");
		}

		FTSIndexSegmentList::const_iterator findSegment(const string& fieldName);

		FTSIndexSegmentList::const_iterator findKey();

		bool checkAllFieldsExists();

		string buildSqlSelectFieldValues(
			ThrowStatusWrapper* const status,
			const unsigned int sqlDialect,
			const bool whereKey = false);

		void prepareExtractRecordStmt(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect
		);

		IStatement* const getPreparedExtractRecordStmt()
		{
			return m_stmtExtractRecord;
		}

		IMessageMetadata* const getOutExtractRecordMetadata()
		{
			return m_outMetaExtractRecord;
		}

		IMessageMetadata* const getInExtractRecordMetadata()
		{
			return m_inMetaExtractRecord;
		}
	};


	using FTSIndexPtr = unique_ptr<FTSIndex>;
	using FTSIndexList = list<FTSIndexPtr>;
	using FTSIndexMap = map<string, FTSIndexPtr>;

	/// <summary>
	/// Metadata for a full-text index segment.
	/// </summary>
	class FTSIndexSegment final
	{
	public:
		string indexName{""};
		string fieldName{""};
		bool key = false;
		double boost = 1.0;
		bool boostNull = true;
		bool fieldExists = false;
	public:
		FTSIndexSegment() = default;

		bool inline compareFieldName(const string& aFieldName) {
			return (fieldName == aFieldName) || (fieldName == "RDB$DB_KEY" && aFieldName == "DB_KEY");
		}
	};


	class RelationHelper;
	class AnalyzerRepository;


	/// <summary>
	/// Repository of full-text indexes. 
	/// 
	/// Allows you to retrieve and manage full-text index metadata.
	/// </summary>
	class FTSIndexRepository final
	{

	private:
		IMaster* m_master = nullptr;	
		AnalyzerRepository* const m_analyzerRepository{ nullptr };
		RelationHelper* const m_relationHelper{nullptr};
		// prepared statements
		AutoRelease<IStatement> m_stmt_exists_index{ nullptr };
		AutoRelease<IStatement> m_stmt_get_index{ nullptr };
		AutoRelease<IStatement> m_stmt_index_fields{ nullptr };
		AutoRelease<IStatement> m_stmt_index_key_field{ nullptr };
		AutoRelease<IStatement> m_stmt_active_indexes_by_analyzer{ nullptr };

		const char* SQL_CREATE_FTS_INDEX = R"SQL(
INSERT INTO FTS$INDICES (
  FTS$INDEX_NAME, 
  FTS$RELATION_NAME, 
  FTS$ANALYZER, 
  FTS$DESCRIPTION, 
  FTS$INDEX_STATUS)
VALUES(?, ?, ?, ?, ?)
)SQL";

		const char* SQL_DROP_FTS_INDEX = R"SQL(
DELETE FROM FTS$INDICES WHERE FTS$INDEX_NAME = ?
)SQL";

		const char* SQL_FTS_INDEX_EXISTS = R"SQL(
SELECT COUNT(*) AS CNT
FROM FTS$INDICES
WHERE FTS$INDEX_NAME = ?
)SQL";

		const char* SQL_SET_FTS_INDEX_STATUS = R"SQL(
UPDATE FTS$INDICES SET FTS$INDEX_STATUS = ? WHERE FTS$INDEX_NAME = ?
)SQL";

		const char* SQL_GET_FTS_INDEX = R"SQL(
SELECT 
  FTS$INDEX_NAME, 
  FTS$RELATION_NAME, 
  FTS$ANALYZER, 
  FTS$DESCRIPTION, 
  FTS$INDEX_STATUS
FROM FTS$INDICES
WHERE FTS$INDEX_NAME = ?
)SQL";

		const char* SQL_ALL_FTS_INDECES = R"SQL(
SELECT 
  FTS$INDEX_NAME, 
  FTS$RELATION_NAME, 
  FTS$ANALYZER, 
  FTS$DESCRIPTION, 
  FTS$INDEX_STATUS
FROM FTS$INDICES
ORDER BY FTS$INDEX_NAME
)SQL";

		const char* SQL_ALL_FTS_INDECES_AND_SEGMENTS = R"SQL(
SELECT
  FTS$INDICES.FTS$INDEX_NAME,
  FTS$INDICES.FTS$RELATION_NAME,
  FTS$INDICES.FTS$ANALYZER,
  FTS$INDICES.FTS$INDEX_STATUS,
  FTS$INDEX_SEGMENTS.FTS$FIELD_NAME,
  FTS$INDEX_SEGMENTS.FTS$KEY,
  FTS$INDEX_SEGMENTS.FTS$BOOST,
  (RF.RDB$FIELD_NAME IS NOT NULL) AS FIELD_EXISTS
FROM FTS$INDICES
LEFT JOIN FTS$INDEX_SEGMENTS ON FTS$INDEX_SEGMENTS.FTS$INDEX_NAME = FTS$INDICES.FTS$INDEX_NAME
LEFT JOIN RDB$RELATION_FIELDS RF
    ON RF.RDB$RELATION_NAME = FTS$INDICES.FTS$RELATION_NAME
   AND RF.RDB$FIELD_NAME = FTS$INDEX_SEGMENTS.FTS$FIELD_NAME
ORDER BY FTS$INDICES.FTS$INDEX_NAME
)SQL";

		const char* SQL_FTS_INDEX_SEGMENTS = R"SQL(
SELECT
  FTS$INDEX_SEGMENTS.FTS$INDEX_NAME,
  FTS$INDEX_SEGMENTS.FTS$FIELD_NAME,
  FTS$INDEX_SEGMENTS.FTS$KEY,
  FTS$INDEX_SEGMENTS.FTS$BOOST,
  (RF.RDB$FIELD_NAME IS NOT NULL) AS FIELD_EXISTS
FROM FTS$INDICES
JOIN FTS$INDEX_SEGMENTS
    ON FTS$INDEX_SEGMENTS.FTS$INDEX_NAME = FTS$INDICES.FTS$INDEX_NAME
LEFT JOIN RDB$RELATION_FIELDS RF
    ON RF.RDB$RELATION_NAME = FTS$INDICES.FTS$RELATION_NAME
   AND RF.RDB$FIELD_NAME = FTS$INDEX_SEGMENTS.FTS$FIELD_NAME
WHERE FTS$INDICES.FTS$INDEX_NAME = ?
)SQL";

		const char* SQL_FTS_KEY_INDEX_FIELD_EXISTS = R"SQL(
SELECT COUNT(*) AS CNT
FROM FTS$INDEX_SEGMENTS
WHERE FTS$INDEX_NAME = ? AND FTS$KEY IS TRUE
)SQL";

		const char* SQL_GET_FTS_KEY_INDEX_FIELD = R"SQL(
SELECT FTS$INDEX_NAME, FTS$FIELD_NAME
FROM FTS$INDEX_SEGMENTS
WHERE FTS$INDEX_NAME = ? AND FTS$KEY IS TRUE
)SQL";

		const char* SQL_FTS_ADD_INDEX_FIELD = R"SQL(
INSERT INTO FTS$INDEX_SEGMENTS (
  FTS$INDEX_NAME, 
  FTS$FIELD_NAME, 
  FTS$KEY, 
  FTS$BOOST
)
VALUES(?, ?, ?, ?)
)SQL";

		const char* SQL_FTS_DROP_INDEX_FIELD = R"SQL(
DELETE FROM FTS$INDEX_SEGMENTS
WHERE FTS$INDEX_NAME = ? AND FTS$FIELD_NAME = ?
)SQL";

		const char* SQL_FTS_SET_INDEX_FIELD_BOOST = R"SQL(
UPDATE FTS$INDEX_SEGMENTS
SET FTS$BOOST = ?
WHERE FTS$INDEX_NAME = ? AND FTS$FIELD_NAME = ?
)SQL";

		const char* SQL_HAS_INDEX_BY_ANALYZER = R"SQL(
SELECT COUNT(*) AS CNT
FROM FTS$INDICES
WHERE FTS$ANALYZER = ?
)SQL";

		const char* SQL_ACTIVE_INDEXES_BY_ANALYZER = R"SQL(
SELECT FTS$INDEX_NAME
FROM FTS$INDICES
WHERE FTS$ANALYZER = ? AND FTS$INDEX_STATUS = 'C'
)SQL";
	public:

		FTSIndexRepository() = delete;


		explicit FTSIndexRepository(IMaster* master);

		~FTSIndexRepository();

		RelationHelper* const getRelationHelper()
		{
			return m_relationHelper;
		}

		AnalyzerRepository* const getAnalyzerRepository()
		{
			return m_analyzerRepository;
		}

		/// <summary>
		/// Create a new full-text index. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="analyzerName">Analyzer name</param>
		/// <param name="description">Custom index description</param>
		void createIndex (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& indexName,
			const string& relationName,
			const string& analyzerName,
			const string& description);

		/// <summary>
		/// Remove a full-text index. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		void dropIndex (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& indexName);

		/// <summary>
		/// Set the index status.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="indexStatus">Index Status</param>
		void setIndexStatus (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& indexName,
			const string& indexStatus);

		/// <summary>
		/// Checks if an index with the given name exists.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// 
		/// <returns>Returns true if the index exists, false otherwise</returns>
		bool hasIndex (
			ThrowStatusWrapper* const status, 
			IAttachment* const att, 
			ITransaction* const tra, 
			const unsigned int sqlDialect, 
			const string& indexName);

		/// <summary>
		/// Returns index metadata by index name.
		/// 
		/// Throws an exception if the index does not exist. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="ftsIndex">Index metadata</param>
		/// <param name="indexName">Index name</param>
		/// <param name="withSegments">Fill segments list</param>
		/// 
		void getIndex (
			ThrowStatusWrapper* const status, 
			IAttachment* const att, 
			ITransaction* const tra, 
			const unsigned int sqlDialect, 
			const FTSIndexPtr& ftsIndex,
			const string& indexName,
			const bool withSegments = false);

		/// <summary>
		/// Returns a list of indexes. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexes">List of indexes</param>
		/// 
		void fillAllIndexes (
			ThrowStatusWrapper* const status, 
			IAttachment* const att, 
			ITransaction* const tra, 
			const unsigned int sqlDialect,
			FTSIndexList& indexes);

		/// <summary>
		/// Returns a list of indexes with fields (segments). 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexes">Map indexes of name with fields (segments)</param>
		/// 
		void fillAllIndexesWithFields(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			FTSIndexMap& indexes);

		/// <summary>
		/// Returns a list of index fields with the given name.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="segments">Segments list</param>
		/// 
		/// <returns>List of index fields (segments)</returns>
		void fillIndexFields(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& indexName,
			FTSIndexSegmentList& segments);



		/// <summary>
		/// Checks if an index key field exists for given relation.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// 
		/// <returns>Returns true if the index field exists, false otherwise</returns>
		bool hasKeyIndexField(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& indexName
		);

		/// <summary>
		/// Returns segment with key field.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="ftsIndexSegment">Key index field</param>
		/// <param name="indexName">Index name</param>
		/// 
		void getKeyIndexField (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const FTSIndexSegmentPtr& keyIndexSegment,
			const string& indexName
		);

		/// <summary>
		/// Adds a new field (segment) to the full-text index.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="fieldName">Field name</param>
		/// <param name="key">Field is key</param>
		/// <param name="boost">Significance multiplier</param>
		/// <param name="boostNull">Boost null flag</param>
		void addIndexField (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& indexName,
			const string& fieldName,
			const bool key,
			const double boost = 1.0,
			const bool boostNull = true);

		/// <summary>
		/// Removes a field (segment) from the full-text index.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="fieldName">Field name</param>
		void dropIndexField (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& indexName,
			const string& fieldName);

		/// <summary>
		/// Sets the significance multiplier for the index field.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="fieldName">Field name</param>
		/// <param name="boost">Significance multiplier</param>
		/// <param name="boostNull">Boost null flag</param>
		void setIndexFieldBoost(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& indexName,
			const string& fieldName,
			const double boost,
			const bool boostNull = false);


		/// <summary>
		/// Checks for the existence of a field (segment) in a full-text index. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="fieldName">Field name</param>
		/// <returns>Returns true if the field (segment) exists in the index, false otherwise</returns>
		bool hasIndexField (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& indexName,
			const string& fieldName);

		bool hasIndexByAnalyzer(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& analyzerName
		);

		unordered_set<string> getActiveIndexByAnalyzer(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& analyzerName
		);
	};
	using FTSIndexRepositoryPtr = unique_ptr<FTSIndexRepository>;

}
#endif	// FTS_INDEX_H
