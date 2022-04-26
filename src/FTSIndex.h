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
#include "LuceneHeaders.h"
#include "FileUtils.h"
#include <string>
#include <list>
#include <map>

using namespace Firebird;
using namespace std;
using namespace Lucene;

namespace LuceneUDR
{
	/// <summary>
	/// Full-text index metadata.
	/// </summary>
	struct FTSIndex
	{
		string indexName;
		string analyzer;
		string description;
		string status; // N - new index, I - inactive, U - need rebuild, C - complete

		bool isActive() {
			return (status == "C") || (status == "U");
		}
	};

	/// <summary>
	/// Metadata for a full-text index segment.
	/// </summary>
	struct FTSIndexSegment
	{
		string indexName;
		string relationName;
		string fieldName;
		bool storeData;
		double boost = 1.0;

		FTSIndex index;

		string getFullFieldName() {
			return relationName + "." + fieldName;
		}
	};

	/// <summary>
	/// Returns the directory where full-text indexes are located.
	/// </summary>
	/// 
	/// <param name="context">The context of the external routine.</param>
	/// 
	/// <returns>Full path to full-text index directory</returns>
	string getFtsDirectory(IExternalContext* context);

	inline bool createIndexDirectory(const string &indexDir)
	{
		auto indexDirUnicode = StringUtils::toUnicode(indexDir);
		if (!FileUtils::isDirectory(indexDirUnicode)) {
			return FileUtils::createDirectory(indexDirUnicode);
		}
		return true;
	}

	inline bool createIndexDirectory(const String &indexDir)
	{
		if (!FileUtils::isDirectory(indexDir)) {
			return FileUtils::createDirectory(indexDir);
		}
		return true;
	}

	inline bool removeIndexDirectory(const string &indexDir)
	{
		auto indexDirUnicode = StringUtils::toUnicode(indexDir);
		if (FileUtils::isDirectory(indexDirUnicode)) {
			return FileUtils::removeDirectory(indexDirUnicode);
		}
		return true;
	}

	inline bool removeIndexDirectory(const String &indexDir)
	{
		if (FileUtils::isDirectory(indexDir)) {
			return FileUtils::removeDirectory(indexDir);
		}
		return true;
	}

	using FTSIndexList = list<FTSIndex>;
	using FTSIndexMap = map<string, FTSIndex>;
	using FTSIndexSegmentList = list<FTSIndexSegment>;
	using FTSIndexSegmentsMap = map<string, FTSIndexSegmentList>;

	/// <summary>
	/// Full-text index relation.
	/// </summary>
	class FTSRelation final {
	private:
		string _relationName;            // Relation name

		FTSIndexMap _indexes;  // Index map by index name
		FTSIndexSegmentsMap _segments;   // Map of index segments by index name 
		map<string, string> _sqls;       // Map of SQL query texts by index name 

		/// <summary>
		/// Creates a list of segments for the index with the given name, 
		/// if the list has not already been created.
		/// </summary>
		/// 
		/// <param name="indexName">Index name</param>
		void createRelationSegmentsList(const string &indexName)
		{
			auto r = _segments.find(indexName);
			if (r == _segments.end()) {
				_segments[indexName] = list<FTSIndexSegment>();
			}
		}
	public:
		FTSRelation(string relationName)
			: _relationName(relationName),
			_indexes(),
			_segments(),
			_sqls()
		{
		}

		/// <summary>
		/// Adds an index if no index of the same name exists.
		/// </summary>
		/// 
		/// <param name="index">A structure that describes the metadata of a full-text index</param>
		void addIndex(FTSIndex index)
		{
			auto r = _indexes.find(index.indexName);
			if (r == _indexes.end()) {
				_indexes[index.indexName] = index;
			}
		}

		/// <summary>
		/// Updates an index.
		/// </summary>
		/// 
		/// <param name="index">Structure describing the index </param>
		void updateIndex(FTSIndex index)
		{
			_indexes[index.indexName] = index;
		}

		/// <summary>
		/// Returns a map of indexes by index name.
		/// </summary>
		/// 
		/// <returns>Map of indexes by index name</returns>
		FTSIndexMap getIndexes()
		{
			return _indexes;
		}

		/// <summary>
		/// Sets the SQL query for the given index name.
		/// </summary>
		/// 
		/// <param name="indexName">Index name</param>
		/// <param name="sql">SQL query text</param>
		void setSql(const string indexName, const string sql)
		{
			_sqls[indexName] = sql;
		}

		/// <summary>
		/// Returns the text of the SQL query for the index with the given name.
		/// </summary>
		/// 
		/// <param name="indexName">Index name</param>
		/// 
		/// <returns>SQL query text</returns>
		const string getSql(const string &indexName) {
			return _sqls[indexName];
		}

		/// <summary>
		/// Adds a segment to the segment map by index name.
		/// </summary>
		/// 
		/// <param name="segment">A structure that describes the metadata of a full-text index segment</param>
		void addSegment(FTSIndexSegment segment)
		{
			createRelationSegmentsList(segment.indexName);
			_segments[segment.indexName].push_back(segment);
		}

		/// <summary>
		/// Returns segments for the given index name. 
		/// </summary>
		/// 
		/// <param name="indexName">Index name</param>
		/// 
		/// <returns>List of full-text index segments</returns>
		FTSIndexSegmentList getSegmentsByIndexName(const string &indexName)
		{
			return _segments[indexName];
		}
	};

	/// <summary>
	/// Repository of full-text indexes. 
	/// 
	/// Allows you to retrieve and manage full-text index metadata.
	/// </summary>
	class FTSIndexRepository final
	{

	private:
		IMaster* m_master;
		RelationHelper relationHelper;
		// prepared statements
		AutoRelease<IStatement> stmt_exists_index;
		AutoRelease<IStatement> stmt_get_index;
		AutoRelease<IStatement> stmt_index_segments;
		AutoRelease<IStatement> stmt_rel_segments;
	public:
		FTSIndexRepository()
			: FTSIndexRepository(nullptr)
		{}

		FTSIndexRepository(IMaster* master)
			: m_master(master),
			relationHelper(master),
			stmt_exists_index(nullptr),
			stmt_get_index(nullptr),
			stmt_index_segments(nullptr),
			stmt_rel_segments(nullptr)
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
		/// <param name="analyzerName">Analyzer name</param>
		/// <param name="description">Custom index description</param>
		void createIndex (
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &indexName,
			const string &analyzerName,
			const string &description);

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
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &indexName);

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
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &indexName,
			const string &indexStatus);

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
			ThrowStatusWrapper* status, 
			IAttachment* att, 
			ITransaction* tra, 
			const unsigned int sqlDialect, 
			const string &indexName);

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
		/// 
		/// <returns>Index metadata</returns>
		FTSIndex getIndex (
			ThrowStatusWrapper* status, 
			IAttachment* att, 
			ITransaction* tra, 
			const unsigned int sqlDialect, 
			const string &indexName);

		/// <summary>
		/// Returns a list of indexes. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// 
		/// <returns>List of indexes</returns>
		FTSIndexList getAllIndexes (
			ThrowStatusWrapper* status, 
			IAttachment* att, 
			ITransaction* tra, 
			const unsigned int sqlDialect);

		/// <summary>
		/// Returns a list of index segments with the given name.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// 
		/// <returns>List of index segments</returns>
		FTSIndexSegmentList getIndexSegments (
			ThrowStatusWrapper* status, 
			IAttachment* att, 
			ITransaction* tra, 
			const unsigned int sqlDialect, 
			const string &indexName);

		/// <summary>
		/// Returns all segments of all indexes, ordered by index name. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// 
		/// <returns>List of index segments</returns>
		FTSIndexSegmentList getAllIndexSegments (
			ThrowStatusWrapper* status, 
			IAttachment* att, 
			ITransaction* tra, 
			const unsigned int sqlDialect);

		/// <summary>
		/// Returns index segments by relation name.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// 
		/// <returns>List of index segments</returns>
		FTSIndexSegmentList getIndexSegmentsByRelation (
			ThrowStatusWrapper* status, 
			IAttachment* att, 
			ITransaction* tra, 
			const unsigned int sqlDialect, 
			const string &relationName);


		/// <summary>
		/// Adds a new field (segment) to the full-text index.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="fieldName">Field name</param>
		/// <param name="boost">Significance multiplier</param>
		void addIndexField (
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &indexName,
			const string &relationName,
			const string &fieldName,
			const double boost);

		/// <summary>
		/// Removes a field (segment) from the full-text index.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="fieldName">Field name</param>
		void dropIndexField (
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &indexName,
			const string &relationName,
			const string &fieldName);


		/// <summary>
		/// Checks for the existence of a field (segment) in a full-text index. 
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="indexName">Index name</param>
		/// <param name="relationName">Relation name</param>
		/// <param name="fieldName">Field name</param>
		/// <returns>Returns true if the field (segment) exists in the index, false otherwise</returns>
		bool hasIndexSegment (
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &indexName,
			const string &relationName,
			const string &fieldName);

		/// <summary>
		/// Groups index segments by relation names.
		/// </summary>
		/// 
		/// <param name="segments">List of index segments</param>
		/// 
		/// <returns>Map of index segments by relation names</returns>
		static inline FTSIndexSegmentsMap groupIndexSegmentsByRelation(FTSIndexSegmentList segments)
		{
			FTSIndexSegmentsMap segmentsByRelation;
			for (const auto& segment : segments) {
				auto r = segmentsByRelation.find(segment.relationName);
				if (r != segmentsByRelation.end()) {
					auto relSegments = r->second;
					relSegments.push_back(segment);
					segmentsByRelation[segment.relationName] = relSegments;
				}
				else {
					list<FTSIndexSegment> relSegments;
					relSegments.push_back(segment);
					segmentsByRelation[segment.relationName] = relSegments;
				}
			}
			return segmentsByRelation;
		}

		/// <summary>
		/// Groups index segments by index names.
		/// </summary>
		/// 
		/// <param name="segments">List of index segments</param>
		/// 
		/// <returns>Map of index segments by index names</returns>
		static inline FTSIndexSegmentsMap groupSegmentsByIndex(FTSIndexSegmentList segments)
		{
			FTSIndexSegmentsMap segmentsByIndex;
			for (const auto& segment : segments) {
				auto r = segmentsByIndex.find(segment.indexName);
				if (r != segmentsByIndex.end()) {
					auto idxSegments = r->second;
					idxSegments.push_back(segment);
					segmentsByIndex[segment.indexName] = idxSegments;
				}
				else {
					FTSIndexSegmentList idxSegments;
					idxSegments.push_back(segment);
					segmentsByIndex[segment.indexName] = idxSegments;
				}
			}
			return segmentsByIndex;
		}

		/// <summary>
		/// Returns a list of full-text index field names given the relation name.
		/// </summary>
		/// 
		/// <param name="status">Firebird status</param>
		/// <param name="att">Firebird attachment</param>
		/// <param name="tra">Firebird transaction</param>
		/// <param name="sqlDialect">SQL dialect</param>
		/// <param name="relationName">Relation name</param>
		/// 
		/// <returns>List of full-text index field names</returns>
		list<string> getFieldsByRelation (
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &relationName);


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
		/// 
		/// <returns>Trigger source code list</returns>
		list<string> makeTriggerSourceByRelation (
			ThrowStatusWrapper* status,
			IAttachment* att,
			ITransaction* tra,
			const unsigned int sqlDialect,
			const string &relationName,
			const bool multiAction);
	};
}
#endif	// FTS_INDEX_H
