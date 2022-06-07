#ifndef FTS_INDEX_H
#define FTS_INDEX_H

/**
 *  Utilities for getting and managing metadata for full-text indexes.
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
#include "Relations.h"
#include <string>
#include <list>
#include <map>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace std;

namespace LuceneUDR
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
	private:
		AutoRelease<IStatement> stmtExtractRecord;
		AutoRelease<IMessageMetadata> outMetaExtractRecord;
		AutoRelease<IMessageMetadata> inMetaExtractRecord;
	public: 

		FTSIndex()
			: indexName()
			, relationName()
			, analyzer()
			, description()
			, status()
			, keyFieldType(FTSKeyType::NONE)
			, unicodeKeyFieldName()
			, segments()
			, stmtExtractRecord(nullptr)
			, inMetaExtractRecord(nullptr)
			, outMetaExtractRecord(nullptr)
			, unicodeIndexDir()
		{}

		string indexName;
		string relationName;
		string analyzer;		
		string description;
		string status; // N - new index, I - inactive, U - need rebuild, C - complete

		FTSKeyType keyFieldType;
		wstring unicodeKeyFieldName;

		FTSIndexSegmentList segments;
		
		wstring unicodeIndexDir;

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
			return stmtExtractRecord;
		}

		IMessageMetadata* const getOutExtractRecordMetadata()
		{
			return outMetaExtractRecord;
		}

		IMessageMetadata* const getInExtractRecordMetadata()
		{
			return inMetaExtractRecord;
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
		string indexName;
		string fieldName;
		bool key;
		double boost;
		bool boostNull;
		bool fieldExists;

		FTSIndexSegment()
			: indexName()
			, fieldName()
			, key(false)
			, boost(1.0)
			, boostNull(true)
			, fieldExists(false)
		{}

		bool inline compareFieldName(const string& aFieldName) {
			return (fieldName == aFieldName) || (fieldName == "RDB$DB_KEY" && aFieldName == "DB_KEY");
		}
	};

	class FTSKeyFieldBlock final
	{
	public:
		FTSKeyFieldBlock()
			: keyFieldName()
			, keyFieldType(FTSKeyType::NONE)
			, fieldNames()
			, insertingCondition()
			, updatingCondition()
			, deletingCondition()
		{}

		string keyFieldName;
		FTSKeyType keyFieldType;
		list<string> fieldNames;

		string insertingCondition;
		string updatingCondition;
		string deletingCondition;

		const string getProcedureName()
		{
			switch (keyFieldType) {
			case FTSKeyType::DB_KEY:
				return "FTS$LOG_BY_DBKEY";
			case FTSKeyType::INT_ID:
				return "FTS$LOG_BY_ID";
			case FTSKeyType::UUID:
				return "FTS$LOG_BY_UUID";
			default:
				return "";
			}
		}
	};

	using FTSKeyFieldBlockPtr = unique_ptr<FTSKeyFieldBlock>;
	using FTSKeyFieldBlockMap = map<string, FTSKeyFieldBlockPtr>;

	class FTSTrigger final 
	{
	public:
		string triggerName;
		string relationName;
		string triggerEvents;
		unsigned int position;
		string triggerSource;		

		FTSTrigger()
			: triggerName()
			, relationName()
			, triggerEvents()
			, position(0)
			, triggerSource()
		{}

		FTSTrigger(const string& aTriggerName, const string& aRelationName, const string& aTriggerEvents, const unsigned int aPosition, const string& aTriggerSource)
			: triggerName(aTriggerName)
			, relationName(aRelationName)
			, triggerEvents(aTriggerEvents)
			, position(aPosition)
			, triggerSource(aTriggerSource)
		{}

		const string getHeader(unsigned int sqlDialect)
		{
			string triggerHeader =
				"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
				"ACTIVE AFTER " + triggerEvents + "\n"
				"POSITION " + std::to_string(position) + "\n";
			return triggerHeader;
		}

		const string getScript(unsigned int sqlDialect)
		{
			return getHeader(sqlDialect) + triggerSource;
		}
	};

	using FTSTriggerPtr = unique_ptr<FTSTrigger>;
	using FTSTriggerList = list<FTSTriggerPtr>;


	/// <summary>
	/// Repository of full-text indexes. 
	/// 
	/// Allows you to retrieve and manage full-text index metadata.
	/// </summary>
	class FTSIndexRepository final
	{

	private:
		IMaster* m_master;	
		// prepared statements
		AutoRelease<IStatement> stmt_exists_index;
		AutoRelease<IStatement> stmt_get_index;
		AutoRelease<IStatement> stmt_index_segments;
		AutoRelease<IStatement> stmt_rel_segments;
		AutoRelease<IStatement> stmt_key_segment;
	public:
		RelationHelper relationHelper;

		FTSIndexRepository()
			: FTSIndexRepository(nullptr)
		{}

		FTSIndexRepository(IMaster* master)
			: m_master(master)
			, relationHelper(master)
			, stmt_exists_index(nullptr)
			, stmt_get_index(nullptr)
			, stmt_index_segments(nullptr)
			, stmt_rel_segments(nullptr)
			, stmt_key_segment(nullptr)
		{
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
		/// <param name="indexName">Index name</param>
		/// <param name="withSegments">Fill segments list</param>
		/// 
		/// <returns>Index metadata</returns>
		FTSIndexPtr getIndex (
			ThrowStatusWrapper* const status, 
			IAttachment* const att, 
			ITransaction* const tra, 
			const unsigned int sqlDialect, 
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
		/// Returns a list of indexes with segments. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexes">Map indexes of name with segments</param>
		/// 
		void fillAllIndexesWithSegments(
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			FTSIndexMap& indexes);

		/// <summary>
		/// Returns a list of index segments with the given name.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="segments">Segments list</param>
		/// 
		/// <returns>List of index segments</returns>
		void fillIndexSegments(
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
		/// <param name="indexName">Index name</param>
		/// 
		/// <returns>Returns segment with key field.</returns>
		FTSIndexSegmentPtr getKeyIndexField (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
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
		bool hasIndexSegment (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& indexName,
			const string& fieldName);


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
		void makeTriggerSourceByRelation (
			ThrowStatusWrapper* const status,
			IAttachment* const att,
			ITransaction* const tra,
			const unsigned int sqlDialect,
			const string& relationName,
			const bool multiAction,
			const unsigned short position,
			FTSTriggerList& triggers);

		private:
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
			void fillKeyFieldBlocks(
				ThrowStatusWrapper* const status,
				IAttachment* const att,
				ITransaction* const tra,
				const unsigned int sqlDialect,
				const string& relationName,
				FTSKeyFieldBlockMap& keyFieldBlocks
			);

			const string makeTriggerSourceByRelationMulti(
				const FTSKeyFieldBlockMap& keyFieldBlocks,
				const unsigned int sqlDialect,
				const string& relationName
			);

			const string makeTriggerSourceByRelationInsert (
				const FTSKeyFieldBlockMap& keyFieldBlocks,
				const unsigned int sqlDialect,
				const string& relationName
			);

			const string makeTriggerSourceByRelationUpdate(
				const FTSKeyFieldBlockMap& keyFieldBlocks,
				const unsigned int sqlDialect,
				const string& relationName
			);

			const string makeTriggerSourceByRelationDelete(
				const FTSKeyFieldBlockMap& keyFieldBlocks,
				const unsigned int sqlDialect,
				const string& relationName
			);
	};
}
#endif	// FTS_INDEX_H
