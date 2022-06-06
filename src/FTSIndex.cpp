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

#include "FTSIndex.h"
#include "FBUtils.h"
#include "LuceneAnalyzerFactory.h"

using namespace Firebird;
using namespace std;

namespace LuceneUDR
{

	FTSIndexSegmentList::const_iterator FTSIndex::findSegment(const string& fieldName) {
		return std::find_if(
			segments.cbegin(),
			segments.cend(),
			[&fieldName](const auto& segment) { return segment->compareFieldName(fieldName); }
		);
	}

	FTSIndexSegmentList::const_iterator FTSIndex::findKey() {
		return std::find_if(
			segments.cbegin(),
			segments.cend(),
			[](const auto& segment) { return segment->key; }
		);
	}

	bool FTSIndex::checkAllFieldsExists()
	{
		bool existsFlag = true;
		for (const auto& segment : segments) {
			existsFlag = existsFlag && segment->fieldExists;
		}
		return existsFlag;
	}

	void FTSIndex::prepareExtractRecordStmt(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect
	)
	{
		const auto sql = buildSqlSelectFieldValues(status, sqlDialect, true);
		stmtExtractRecord.reset(att->prepare(
			status,
			tra,
			static_cast<unsigned int>(sql.length()),
			sql.c_str(),
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));
		// get a description of the fields				
		AutoRelease<IMessageMetadata> outputMetadata(stmtExtractRecord->getOutputMetadata(status));
		const unsigned colCount = outputMetadata->getCount(status);
		// make all fields of string type except BLOB
		outMetaExtractRecord.reset(prepareTextMetaData(status, outputMetadata));
		inMetaExtractRecord.reset(stmtExtractRecord->getInputMetadata(status));
	}

	string FTSIndex::buildSqlSelectFieldValues(
		ThrowStatusWrapper* const status,
		const unsigned int sqlDialect,
		const bool whereKey)
	{
		list<string> fieldNames;
		for (const auto& segment : segments) {
			fieldNames.push_back(segment->fieldName);
		}
		auto iKeySegment = findKey();
		if (iKeySegment == segments.end()) {
			const string error_message = string_format("Key field not exists in index \"%s\".", indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string keyFieldName = (*iKeySegment)->fieldName;

		std::stringstream ss;
		ss << "SELECT\n";
		int field_cnt = 0;
		for (const auto& fieldName : fieldNames) {
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
			for (const auto& fieldName : fieldNames) {
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
	void FTSIndexRepository::createIndex (
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& indexName,
		const string& relationName,
		const string& analyzerName,
		const string& description)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
			(FB_BLOB, description)
			(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
		) input(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		input->analyzer.length = static_cast<ISC_USHORT>(analyzerName.length());
		analyzerName.copy(input->analyzer.str, input->analyzer.length);

		if (!description.empty()) {
			AutoRelease<IBlob> blob(att->createBlob(status, tra, &input->description, 0, nullptr));
			blob_set_string(status, blob, description);
			blob->close(status);
		}
		else {
			input->descriptionNull = true;
		}

		const string indexStatus = "N";
		input->indexStatus.length = static_cast<ISC_USHORT>(indexStatus.length());
		indexStatus.copy(input->indexStatus.str, input->indexStatus.length);

		// check for index existence
		if (hasIndex(status, att, tra, sqlDialect, indexName)) {
			const string error_message = string_format("Index \"%s\" already exists", indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		// checking the existence of the analyzer
		LuceneAnalyzerFactory analyzerFactory;
		if (!analyzerFactory.hasAnalyzer(analyzerName)) {
			const string error_message = string_format("Analyzer \"%s\" not exists", analyzerName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		att->execute(
			status,
			tra,
			0,
			"INSERT INTO FTS$INDICES(FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$ANALYZER, FTS$DESCRIPTION, FTS$INDEX_STATUS)\n"
			"VALUES(?, ?, ?, ?, ?)",
			sqlDialect,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
	}

	/// <summary>
	/// Remove a full-text index. 
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="indexName">Index name</param>
	void FTSIndexRepository::dropIndex (
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& indexName)
	{

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		) input(status, m_master);

		input.clear();
	
		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		// check for index existence
		if (!hasIndex(status, att, tra, sqlDialect, indexName)) {
			const string error_message = string_format("Index \"%s\" not exists", indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		att->execute(
			status,
			tra,
			0,
			"DELETE FROM FTS$INDICES WHERE FTS$INDEX_NAME = ?",
			sqlDialect,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
	}

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
	void FTSIndexRepository::setIndexStatus (
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& indexName,
		const string& indexStatus)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		) input(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		input->indexStatus.length = static_cast<ISC_USHORT>(indexStatus.length());
		indexStatus.copy(input->indexStatus.str, input->indexStatus.length);

		att->execute(
			status,
			tra,
			0,
			"UPDATE FTS$INDICES SET FTS$INDEX_STATUS = ? WHERE FTS$INDEX_NAME = ?",
			sqlDialect,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
	}

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
	bool FTSIndexRepository::hasIndex (
		ThrowStatusWrapper* const status, 
		IAttachment* const att, 
		ITransaction* const tra, 
		const unsigned int sqlDialect, 
		const string& indexName)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTEGER, cnt)
		) output(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		if (!stmt_exists_index.hasData()) {
			stmt_exists_index.reset(att->prepare(
				status,
				tra,
				0,
				"SELECT COUNT(*) AS CNT\n"
				"FROM FTS$INDICES\n"
				"WHERE FTS$INDEX_NAME = ?",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}
		AutoRelease<IResultSet> rs(stmt_exists_index->openCursor(
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

		return foundFlag;
	}



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
	FTSIndexPtr FTSIndexRepository::getIndex (
		ThrowStatusWrapper* const status, 
		IAttachment* const att, 
		ITransaction* const tra, 
		const unsigned int sqlDialect, 
		const string& indexName,
		const bool withSegments)
	{	
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
			(FB_BLOB, description)
			(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
		) output(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		if (!stmt_get_index.hasData()) {
			stmt_get_index.reset(att->prepare(
				status,
				tra,
				0,
				"SELECT FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$ANALYZER, FTS$DESCRIPTION, FTS$INDEX_STATUS\n"
				"FROM FTS$INDICES\n"
				"WHERE FTS$INDEX_NAME = ?",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}
		AutoRelease<IResultSet> rs(stmt_get_index->openCursor(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			output.getMetadata(),
			0
		));

		auto ftsIndex = make_unique<FTSIndex>();
		bool foundFlag = false;
		if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			foundFlag = true;
			ftsIndex->indexName.assign(output->indexName.str, output->indexName.length);
			ftsIndex->relationName.assign(output->relationName.str, output->relationName.length);
			ftsIndex->analyzer.assign(output->analyzer.str, output->analyzer.length);
			if (!output->descriptionNull) {
				AutoRelease<IBlob> blob(att->openBlob(status, tra, &output->description, 0, nullptr));
				ftsIndex->description = blob_get_string(status, blob);
				blob->close(status);
			}
			ftsIndex->status.assign(output->indexStatus.str, output->indexStatus.length);
		}
		rs->close(status);
		if (!foundFlag) {
			const string error_message = string_format("Index \"%s\" not exists", indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		if (withSegments) {
			fillIndexSegments(status, att, tra, sqlDialect, indexName, ftsIndex->segments);
		}

		return std::move(ftsIndex);
	}

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
	FTSIndexList FTSIndexRepository::getAllIndexes (
		ThrowStatusWrapper* const status, 
		IAttachment* const att, 
		ITransaction* const tra, 
		const unsigned int sqlDialect)
	{

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
			(FB_BLOB, description)
			(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
		) output(status, m_master);


		AutoRelease<IStatement> stmt(att->prepare(
				status,
				tra,
				0,
				"SELECT FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$ANALYZER, FTS$DESCRIPTION, FTS$INDEX_STATUS\n"
				"FROM FTS$INDICES\n"
				"ORDER BY FTS$INDEX_NAME",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
	
		AutoRelease<IResultSet> rs(stmt->openCursor(
			status,
			tra,
			nullptr,
			nullptr,
			output.getMetadata(),
			0
		));

		FTSIndexList indexes;
		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			auto ftsIndex = make_unique<FTSIndex>();
			ftsIndex->indexName.assign(output->indexName.str, output->indexName.length);
			ftsIndex->relationName.assign(output->relationName.str, output->relationName.length);
			ftsIndex->analyzer.assign(output->analyzer.str, output->analyzer.length);

			if (!output->descriptionNull) {
				AutoRelease<IBlob> blob(att->openBlob(status, tra, &output->description, 0, nullptr));
				ftsIndex->description = blob_get_string(status, blob);
				blob->close(status);
			}

			ftsIndex->status.assign(output->indexStatus.str, output->indexStatus.length);

			indexes.push_back(std::move(ftsIndex));
		}
		rs->close(status);

		return indexes;
	}

	/// <summary>
	/// Returns a list of indexes with segments. 
	/// </summary>
	/// 
	/// <param name="status">Firebird status</param>
	/// <param name="att">Firebird attachment</param>
	/// <param name="tra">Firebird transaction</param>
	/// <param name="sqlDialect">SQL dialect</param>
	/// 
	/// <returns>List of indexes with segments</returns>
	FTSIndexMap FTSIndexRepository::getAllIndexesWithSegments(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect)
	{
		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
			(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
			(FB_BOOLEAN, key)
			(FB_DOUBLE, boost)
			(FB_BOOLEAN, fieldExists)
		) output(status, m_master);


		AutoRelease<IStatement> stmt(att->prepare(
			status,
			tra,
			0,
			"SELECT\n"
			"  FTS$INDICES.FTS$INDEX_NAME,\n"
			"  FTS$INDICES.FTS$RELATION_NAME,\n"
			"  FTS$INDICES.FTS$ANALYZER,\n"
			"  FTS$INDICES.FTS$INDEX_STATUS,\n"
			"  FTS$INDEX_SEGMENTS.FTS$FIELD_NAME,\n"
			"  FTS$INDEX_SEGMENTS.FTS$KEY,\n"
			"  FTS$INDEX_SEGMENTS.FTS$BOOST,\n"
			"  (RF.RDB$FIELD_NAME IS NOT NULL) AS FIELD_EXISTS\n"
			"FROM FTS$INDICES\n"
			"LEFT JOIN FTS$INDEX_SEGMENTS ON FTS$INDEX_SEGMENTS.FTS$INDEX_NAME = FTS$INDICES.FTS$INDEX_NAME\n"
			"LEFT JOIN RDB$RELATION_FIELDS RF\n"
			"    ON RF.RDB$RELATION_NAME = FTS$INDICES.FTS$RELATION_NAME\n"
			"   AND RF.RDB$FIELD_NAME = FTS$INDEX_SEGMENTS.FTS$FIELD_NAME\n"
			"ORDER BY FTS$INDICES.FTS$INDEX_NAME",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));

		AutoRelease<IResultSet> rs(stmt->openCursor(
			status,
			tra,
			nullptr,
			nullptr,
			output.getMetadata(),
			0
		));

		FTSIndexMap indexes;
		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			const string indexName(output->indexName.str, output->indexName.length);

			auto [it, result] = indexes.try_emplace(indexName, make_unique<FTSIndex>());
			auto& index = it->second;
			if (result) {
				index->indexName.assign(output->indexName.str, output->indexName.length);
				index->relationName.assign(output->relationName.str, output->relationName.length);
				index->analyzer.assign(output->analyzerName.str, output->analyzerName.length);
				index->status.assign(output->indexStatus.str, output->indexStatus.length);
			}

			auto indexSegment = make_unique<FTSIndexSegment>();
			indexSegment->indexName.assign(output->indexName.str, output->indexName.length);
			indexSegment->fieldName.assign(output->fieldName.str, output->fieldName.length);
			indexSegment->key = output->key;
			indexSegment->boost = output->boost;
			indexSegment->boostNull = output->boostNull;
			if (indexSegment->fieldName == "RDB$DB_KEY") {
				indexSegment->fieldExists = true;
			}
			else {
				indexSegment->fieldExists = output->fieldExists;
			}

			index->segments.push_back(std::move(indexSegment));
		}
		rs->close(status);
		return indexes;
	}

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
	void FTSIndexRepository::fillIndexSegments(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& indexName,
		FTSIndexSegmentList& segments)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
			(FB_BOOLEAN, key)
			(FB_DOUBLE, boost)
			(FB_BOOLEAN, fieldExists)
		) output(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		if (!stmt_index_segments.hasData()) {
			stmt_index_segments.reset(att->prepare(
				status,
				tra,
				0,
				"SELECT"
				"  FTS$INDEX_SEGMENTS.FTS$INDEX_NAME,\n"
				"  FTS$INDEX_SEGMENTS.FTS$FIELD_NAME,\n"
				"  FTS$INDEX_SEGMENTS.FTS$KEY,\n"
				"  FTS$INDEX_SEGMENTS.FTS$BOOST,\n"
				"  (RF.RDB$FIELD_NAME IS NOT NULL) AS FIELD_EXISTS\n"
				"FROM FTS$INDICES\n"
				"JOIN FTS$INDEX_SEGMENTS\n"
				"    ON FTS$INDEX_SEGMENTS.FTS$INDEX_NAME = FTS$INDICES.FTS$INDEX_NAME\n"
				"LEFT JOIN RDB$RELATION_FIELDS RF\n"
				"    ON RF.RDB$RELATION_NAME = FTS$INDICES.FTS$RELATION_NAME\n"
				"   AND RF.RDB$FIELD_NAME = FTS$INDEX_SEGMENTS.FTS$FIELD_NAME\n"
				"WHERE FTS$INDICES.FTS$INDEX_NAME = ?",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}
		AutoRelease<IResultSet> rs(stmt_index_segments->openCursor(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			output.getMetadata(),
			0
		));
		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			auto indexSegment = make_unique<FTSIndexSegment>();
			indexSegment->indexName.assign(output->indexName.str, output->indexName.length);
			indexSegment->fieldName.assign(output->fieldName.str, output->fieldName.length);
			indexSegment->key = output->key;
			indexSegment->boost = output->boost;
			indexSegment->boostNull = output->boostNull;
			if (indexSegment->fieldName == "RDB$DB_KEY") {
				indexSegment->fieldExists = true;
			}
			else {
				indexSegment->fieldExists = output->fieldExists;
			}

			segments.push_back(std::move(indexSegment));
		}
		rs->close(status);
	}

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
	bool FTSIndexRepository::hasKeyIndexField(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& indexName
	)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTEGER, cnt)
		) output(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);


		AutoRelease<IStatement> stmt(att->prepare(
			status,
			tra,
			0,
			"SELECT COUNT(*) AS CNT\n"
			"FROM  FTS$INDEX_SEGMENTS\n"
			"WHERE FTS$INDEX_NAME = ? AND FTS$KEY IS TRUE",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));

		AutoRelease<IResultSet> rs(stmt->openCursor(
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

		return foundFlag;
	}

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
	FTSIndexSegmentPtr FTSIndexRepository::getKeyIndexField(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& indexName)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
			(FB_BOOLEAN, key)
			(FB_DOUBLE, boost)
		) output(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		if (!stmt_key_segment.hasData()) {
			stmt_key_segment.reset(att->prepare(
				status,
				tra,
				0,
				"SELECT FTS$INDEX_NAME, FTS$FIELD_NAME, FTS$KEY, FTS$BOOST\n"
				"FROM FTS$INDEX_SEGMENTS\n"
				"WHERE FTS$INDEX_NAME = ? AND FTS$KEY IS TRUE",
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}
		AutoRelease<IResultSet> rs(stmt_key_segment->openCursor(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			output.getMetadata(),
			0
		));
		bool foundFlag = false;
		auto keyIndexSegment = make_unique<FTSIndexSegment>();
		if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			keyIndexSegment->indexName.assign(output->indexName.str, output->indexName.length);
			keyIndexSegment->fieldName.assign(output->fieldName.str, output->fieldName.length);
			keyIndexSegment->key = true;
			keyIndexSegment->boost = 0;
			keyIndexSegment->boostNull = true;

			foundFlag = true;
		}
		rs->close(status);

		if (!foundFlag) {
			const string error_message = string_format("Key field not exists in index \"%s\".", indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		return std::move(keyIndexSegment);
	}

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
	/// <param name="boost">Significance multiplier</param>
	/// <param name="boostNull">Boost null flag</param>
	void FTSIndexRepository::addIndexField(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string &indexName,
		const string &fieldName,
		const bool key,
		const double boost,
		const bool boostNull)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
			(FB_BOOLEAN, key)
			(FB_DOUBLE, boost)
		) input(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		input->fieldName.length = static_cast<ISC_USHORT>(fieldName.length());
		fieldName.copy(input->fieldName.str, input->fieldName.length);

		input->key = key;

		input->boostNull = boostNull;
        input->boost = boost;


		auto index = getIndex(status, att, tra, sqlDialect, indexName);

		// Checking whether the key field exists in the index.
		if (key && hasKeyIndexField(status, att, tra, sqlDialect, indexName)) {
			const string error_message = string_format("The key field already exists in the \"%s\" index.", indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		// Checking whether the field exists in the index.
		if (hasIndexSegment(status, att, tra, sqlDialect, indexName, fieldName)) {			
			const string error_message = string_format("Field \"%s\" already exists in index \"%s\"", fieldName, indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		if (fieldName != "RDB$DB_KEY") {
			// Checking whether the field exists in relation.
			if (!relationHelper.fieldExists(status, att, tra, sqlDialect, index->relationName, fieldName)) {
				const string error_message = string_format("Field \"%s\" not exists in relation \"%s\".", fieldName, index->relationName);
				ISC_STATUS statusVector[] = {
				   isc_arg_gds, isc_random,
				   isc_arg_string, (ISC_STATUS)error_message.c_str(),
				   isc_arg_end
				};
				throw FbException(status, statusVector);
			}
		}

		att->execute(
			status,
			tra,
			0,
			"INSERT INTO FTS$INDEX_SEGMENTS(FTS$INDEX_NAME, FTS$FIELD_NAME, FTS$KEY, FTS$BOOST)\n"
			"VALUES(?, ?, ?, ?)",
			sqlDialect,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
		if (index->status != "N") {
			// set the status that the index metadata has been updated
			setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
		}
	}

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
	void FTSIndexRepository::dropIndexField(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string &indexName,
		const string &fieldName)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		) input(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		input->fieldName.length = static_cast<ISC_USHORT>(fieldName.length());
		fieldName.copy(input->fieldName.str, input->fieldName.length);

		// Checking whether the index exists.
		if (!hasIndex(status, att, tra, sqlDialect, indexName)) {
			const string error_message = string_format("Index \"%s\" not exists", indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		// Checking whether the field exists in the index.
		if (!hasIndexSegment(status, att, tra, sqlDialect, indexName, fieldName)) {
			const string error_message = string_format("Field \"%s\" not exists in index \"%s\"", fieldName, indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		att->execute(
			status,
			tra,
			0,
			"DELETE FROM FTS$INDEX_SEGMENTS\n"
			"WHERE FTS$INDEX_NAME = ? AND FTS$FIELD_NAME = ?",
			sqlDialect,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
		// set the status that the index metadata has been updated
		setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
	}

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
	void FTSIndexRepository::setIndexFieldBoost(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& indexName,
		const string& fieldName,
		const double boost,
		const bool boostNull)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_DOUBLE, boost)
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)			
		) input(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		input->fieldName.length = static_cast<ISC_USHORT>(fieldName.length());
		fieldName.copy(input->fieldName.str, input->fieldName.length);

		input->boost = boost;
		input->boostNull = boostNull;

		// Checking whether the index exists.
		if (!hasIndex(status, att, tra, sqlDialect, indexName)) {
			const string error_message = string_format("Index \"%s\" not exists", indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		// Checking whether the field exists in the index.
		if (!hasIndexSegment(status, att, tra, sqlDialect, indexName, fieldName)) {
			const string error_message = string_format("Field \"%s\" not exists in index \"%s\"", fieldName, indexName);
			ISC_STATUS statusVector[] = {
			   isc_arg_gds, isc_random,
			   isc_arg_string, (ISC_STATUS)error_message.c_str(),
			   isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		att->execute(
			status,
			tra,
			0,
			"UPDATE FTS$INDEX_SEGMENTS\n"
			"SET FTS$BOOST = ?"
			"WHERE FTS$INDEX_NAME = ? AND FTS$FIELD_NAME = ?",
			sqlDialect,
			input.getMetadata(),
			input.getData(),
			nullptr,
			nullptr
		);
		// set the status that the index metadata has been updated
		setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
	}

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
	bool FTSIndexRepository::hasIndexSegment(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string &indexName,
		const string &fieldName)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTEGER, cnt)
		) output(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		input->fieldName.length = static_cast<ISC_USHORT>(fieldName.length());
		fieldName.copy(input->fieldName.str, input->fieldName.length);

		AutoRelease<IStatement> stmt(att->prepare(
			status,
			tra,
			0,
			"SELECT COUNT(*) AS CNT\n"
			"FROM FTS$INDEX_SEGMENTS\n"
			"WHERE FTS$INDEX_NAME = ? AND FTS$FIELD_NAME = ?",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));

		AutoRelease<IResultSet> rs(stmt->openCursor(
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

		return foundFlag;
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
	list<string> FTSIndexRepository::getFieldsByRelation (
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string &relationName)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		) output(status, m_master);

		input.clear();

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		AutoRelease<IStatement> stmt(att->prepare(
			status,
			tra,
			0,
			"SELECT FTS$FIELD_NAME\n"
			"FROM FTS$INDEX_SEGMENTS\n"
			"WHERE FTS$RELATION_NAME = ?\n"
			"GROUP BY 1",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));

		AutoRelease<IResultSet> rs(stmt->openCursor(
			status,
			tra,
			input.getMetadata(),
			input.getData(),
			output.getMetadata(),
			0
		));

		list<string> fieldNames;
		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			string fieldName(output->fieldName.str, output->fieldName.length);
			fieldNames.push_back(fieldName);
		}
		rs->close(status);
		return fieldNames;
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
	/// 
	/// <returns>Trigger source code list</returns>
	list<string> FTSIndexRepository::makeTriggerSourceByRelation (
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string &relationName,
		const bool multiAction)
	{
		list<string> triggerSources;

		list<string> fieldNames = getFieldsByRelation(status, att, tra, sqlDialect, relationName);

		if (fieldNames.size() == 0)
			return triggerSources;

		string insertingCondition;
		string updatingCondition;
		string deletingCondition;
		for (auto fieldName : fieldNames) {
			const string metaFieldName = escapeMetaName(sqlDialect, fieldName);

			if (!insertingCondition.empty()) {
				insertingCondition += "\n      OR ";
			}
			insertingCondition += "NEW." + metaFieldName + " IS NOT NULL";

			if (!updatingCondition.empty()) {
				updatingCondition += "\n      OR ";
			}
			updatingCondition += "NEW." + metaFieldName + " IS DISTINCT FROM " + "OLD." + metaFieldName;

			if (!deletingCondition.empty()) {
				deletingCondition += "\n      OR ";
			}
			deletingCondition += "OLD." + metaFieldName + " IS NOT NULL";
		}

	
		if (multiAction) {
			string triggerName = "FTS$" + relationName + "_AIUD";
			string triggerSource =
				"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
				"ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 100\n"
				"AS\n"
				"BEGIN\n"
				"  IF (INSERTING AND (" + insertingCondition + ")) THEN\n"
				"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', NEW.RDB$DB_KEY, 'I');\n"
				"  IF (UPDATING AND (" + updatingCondition + ")) THEN\n"
				"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', OLD.RDB$DB_KEY, 'U');\n"
				"  IF (DELETING AND (" + deletingCondition + ")) THEN\n"
				"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', OLD.RDB$DB_KEY, 'D');\n"
				"END";
			triggerSources.push_back(triggerSource);
		}
		else {
			// INSERT
			string triggerName = "FTS$" + relationName + "_AI";
			string triggerSource =
				"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
				"ACTIVE AFTER INSERT POSITION 100\n"
				"AS\n"
				"BEGIN\n"
				"  IF (" + insertingCondition + ") THEN\n"
				"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', NEW.RDB$DB_KEY, 'I');\n"
				"END";
			triggerSources.push_back(triggerSource);
			// UPDATE
			triggerName = "FTS$" + relationName + "_AU";
			triggerSource =
				"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
				"ACTIVE AFTER UPDATE POSITION 100\n"
				"AS\n"
				"BEGIN\n"
				"  IF (" + updatingCondition + ") THEN\n"
				"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', OLD.RDB$DB_KEY, 'U');\n"
				"END";
			triggerSources.push_back(triggerSource);
			// DELETE
			triggerName = "FTS$" + relationName + "_AD";
			triggerSource =
				"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
				"ACTIVE AFTER DELETE POSITION 100\n"
				"AS\n"
				"BEGIN\n"
				"  IF (" + deletingCondition + ") THEN\n"
				"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', OLD.RDB$DB_KEY, 'D');\n"
				"END";
			triggerSources.push_back(triggerSource);
		}
		return triggerSources;
	}

}
