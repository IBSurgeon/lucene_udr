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
#include "LazyFactory.h"
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
		m_stmtExtractRecord.reset(att->prepare(
			status,
			tra,
			static_cast<unsigned int>(sql.length()),
			sql.c_str(),
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));
		// get a description of the fields				
		AutoRelease<IMessageMetadata> outputMetadata(m_stmtExtractRecord->getOutputMetadata(status));
		// make all fields of string type except BLOB
		m_outMetaExtractRecord.reset(prepareTextMetaData(status, outputMetadata));
		m_inMetaExtractRecord.reset(m_stmtExtractRecord->getInputMetadata(status));
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
			throwException(status, R"(Key field not exists in index "%s".)", indexName.c_str());
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

	const string FTSTrigger::getHeader(unsigned int sqlDialect)
	{
		string triggerHeader =
			"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
			"ACTIVE AFTER " + triggerEvents + "\n"
			"POSITION " + std::to_string(position) + "\n";
		return triggerHeader;
	}

	const string FTSTrigger::getScript(unsigned int sqlDialect)
	{
		return getHeader(sqlDialect) + triggerSource;
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
			BlobUtils::setString(status, blob, description);
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
			throwException(status, R"(Index "%s" already exists)", indexName.c_str());
		}

		// checking the existence of the analyzer
		LuceneAnalyzerFactory analyzerFactory;
		if (!analyzerFactory.hasAnalyzer(analyzerName)) {
			throwException(status, R"(Analyzer "%s" not exists)", analyzerName.c_str());
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		att->execute(
			status,
			tra,
			0,
			SQL_CREATE_FTS_INDEX,
			sqlDialect,
			inputMetadata,
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
			throwException(status, R"(Index "%s" not exists)", indexName.c_str());
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		att->execute(
			status,
			tra,
			0,
			SQL_DROP_FTS_INDEX,
			sqlDialect,
			inputMetadata,
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

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		att->execute(
			status,
			tra,
			0,
			SQL_SET_FTS_INDEX_STATUS,
			sqlDialect,
			inputMetadata,
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

		if (!m_stmt_exists_index.hasData()) {
			m_stmt_exists_index.reset(att->prepare(
				status,
				tra,
				0,
				SQL_FTS_INDEX_EXISTS,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(m_stmt_exists_index->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
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
	/// <param name="ftsIndex">Index metadata</param>	
	/// <param name="indexName">Index name</param>
	/// <param name="withSegments">Fill segments list</param>
	/// 
	void FTSIndexRepository::getIndex (
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const FTSIndexPtr& ftsIndex,
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

		if (!m_stmt_get_index.hasData()) {
			m_stmt_get_index.reset(att->prepare(
				status,
				tra,
				0,
				SQL_GET_FTS_INDEX,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(m_stmt_get_index->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
			0
		));

		int result = rs->fetchNext(status, output.getData());
		if (result == IStatus::RESULT_NO_DATA) {
			throwException(status, R"(Index "%s" not exists)", indexName.c_str());
		}
		rs->close(status);

		// index found
		if (result == IStatus::RESULT_OK) {
			ftsIndex->indexName.assign(output->indexName.str, output->indexName.length);
			ftsIndex->relationName.assign(output->relationName.str, output->relationName.length);
			ftsIndex->analyzer.assign(output->analyzer.str, output->analyzer.length);
			if (!output->descriptionNull) {
				AutoRelease<IBlob> blob(att->openBlob(status, tra, &output->description, 0, nullptr));
				ftsIndex->description = BlobUtils::getString(status, blob);
				blob->close(status);
			}
			ftsIndex->status.assign(output->indexStatus.str, output->indexStatus.length);	

			if (withSegments) {
				fillIndexFields(status, att, tra, sqlDialect, indexName, ftsIndex->segments);
			}
		}				
	}

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
	void FTSIndexRepository::fillAllIndexes(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		FTSIndexList& indexes)
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
			    SQL_ALL_FTS_INDECES,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));

		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());
	
		AutoRelease<IResultSet> rs(stmt->openCursor(
			status,
			tra,
			nullptr,
			nullptr,
			outputMetadata,
			0
		));

		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			auto ftsIndex = make_unique<FTSIndex>();
			ftsIndex->indexName.assign(output->indexName.str, output->indexName.length);
			ftsIndex->relationName.assign(output->relationName.str, output->relationName.length);
			ftsIndex->analyzer.assign(output->analyzer.str, output->analyzer.length);

			if (!output->descriptionNull) {
				AutoRelease<IBlob> blob(att->openBlob(status, tra, &output->description, 0, nullptr));
				ftsIndex->description = BlobUtils::getString(status, blob);
				blob->close(status);
			}

			ftsIndex->status.assign(output->indexStatus.str, output->indexStatus.length);

			indexes.push_back(std::move(ftsIndex));
		}
		rs->close(status);

	}

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
	void FTSIndexRepository::fillAllIndexesWithFields(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		FTSIndexMap& indexes)
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
			SQL_ALL_FTS_INDECES_AND_SEGMENTS,
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));

		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(stmt->openCursor(
			status,
			tra,
			nullptr,
			nullptr,
			outputMetadata,
			0
		));

		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			const string indexName(output->indexName.str, output->indexName.length);

			const auto& [it, result] = indexes.try_emplace(indexName, lazy_convert_construct([] { return std::make_unique<FTSIndex>(); }));
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
	void FTSIndexRepository::fillIndexFields(
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

		if (!m_stmt_index_fields.hasData()) {
			m_stmt_index_fields.reset(att->prepare(
				status,
				tra,
				0,
				SQL_FTS_INDEX_SEGMENTS,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}
		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(m_stmt_index_fields->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
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
			SQL_FTS_KEY_INDEX_FIELD_EXISTS,
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(stmt->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
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
	/// <param name="keyIndexSegment">Key index field</param>
	/// <param name="indexName">Index name</param>
	/// 
	void FTSIndexRepository::getKeyIndexField(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const FTSIndexSegmentPtr& keyIndexSegment,
		const string& indexName)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		) output(status, m_master);

		input.clear();

		input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
		indexName.copy(input->indexName.str, input->indexName.length);

		if (!m_stmt_index_key_field.hasData()) {
			m_stmt_index_key_field.reset(att->prepare(
				status,
				tra,
				0,
				SQL_GET_FTS_KEY_INDEX_FIELD,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(m_stmt_index_key_field->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
			0
		));
		int result = rs->fetchNext(status, output.getData());
		if (result == IStatus::RESULT_NO_DATA) {
			throwException(status, R"(Key field not exists in index "%s".)", indexName.c_str());
		}
		rs->close(status);

		if (result == IStatus::RESULT_OK) {
			keyIndexSegment->indexName.assign(output->indexName.str, output->indexName.length);
			keyIndexSegment->fieldName.assign(output->fieldName.str, output->fieldName.length);
			keyIndexSegment->key = true;
			keyIndexSegment->boost = 0;
			keyIndexSegment->boostNull = true;
		}	
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

		auto ftsIndex = make_unique<FTSIndex>();
		getIndex(status, att, tra, sqlDialect, ftsIndex, indexName);

		// Checking whether the key field exists in the index.
		if (key && hasKeyIndexField(status, att, tra, sqlDialect, indexName)) {
			throwException(status, R"(The key field already exists in the "%s" index.)", indexName.c_str());
		}

		// Checking whether the field exists in the index.
		if (hasIndexField(status, att, tra, sqlDialect, indexName, fieldName)) {			
			throwException(status, R"(Field "%s" already exists in index "%s")", fieldName.c_str(), indexName.c_str());
		}

		if (fieldName != "RDB$DB_KEY") {
			// Checking whether the field exists in relation.
			if (!m_relationHelper->fieldExists(status, att, tra, sqlDialect, ftsIndex->relationName, fieldName)) {
				throwException(status, R"(Field "%s" not exists in relation "%s".)", fieldName.c_str(), ftsIndex->relationName.c_str());
			}
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		att->execute(
			status,
			tra,
			0,
			SQL_FTS_ADD_INDEX_FIELD,
			sqlDialect,
			inputMetadata,
			input.getData(),
			nullptr,
			nullptr
		);
		if (ftsIndex->status != "N") {
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
			throwException(status, R"(Index "%s" not exists)", indexName.c_str());
		}

		// Checking whether the field exists in the index.
		if (!hasIndexField(status, att, tra, sqlDialect, indexName, fieldName)) {
			throwException(status, R"(Field "%s" not exists in index "%s")", fieldName.c_str(), indexName.c_str());
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		att->execute(
			status,
			tra,
			0,
			SQL_FTS_DROP_INDEX_FIELD,
			sqlDialect,
			inputMetadata,
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
			throwException(status, R"(Index "%s" not exists)", indexName.c_str());
		}

		// Checking whether the field exists in the index.
		if (!hasIndexField(status, att, tra, sqlDialect, indexName, fieldName)) {
			throwException(status, R"(Field "%s" not exists in index "%s")", fieldName.c_str(), indexName.c_str());
		}

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		att->execute(
			status,
			tra,
			0,
			SQL_FTS_SET_INDEX_FIELD_BOOST,
			sqlDialect,
			inputMetadata,
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
	bool FTSIndexRepository::hasIndexField(
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

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(stmt->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
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
	void FTSIndexRepository::fillKeyFieldBlocks(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& relationName,
		FTSKeyFieldBlockMap& keyFieldBlocks
	)
	{
		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		) input(status, m_master);

		FB_MESSAGE(Output, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), keyFieldName)
			(FB_INTL_VARCHAR(24, CS_UTF8), keyFieldType)
			(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		) output(status, m_master);

		input.clear();

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		AutoRelease<IStatement> stmt(att->prepare(
			status,
			tra,
			0,
			R"SQL(
WITH T AS (
SELECT
    I.FTS$INDEX_NAME,
    MAX(IIF(SEG.FTS$KEY IS TRUE, SEG.FTS$FIELD_NAME, NULL)) OVER(PARTITION BY SEG.FTS$INDEX_NAME) AS FTS$KEY_FIELD_NAME,
    MAX(
    CASE
      WHEN SEG.FTS$KEY IS TRUE AND F.RDB$FIELD_TYPE = 14 AND F.RDB$CHARACTER_SET_ID = 1 AND F.RDB$FIELD_LENGTH = 16 THEN 'UUID'
      WHEN SEG.FTS$KEY IS TRUE AND SEG.FTS$FIELD_NAME = 'RDB$DB_KEY' THEN 'DBKEY'
      WHEN SEG.FTS$KEY IS TRUE AND F.RDB$FIELD_TYPE IN (7, 8, 16) AND F.RDB$FIELD_SCALE = 0 THEN 'INT_ID'
    END) OVER(PARTITION BY SEG.FTS$INDEX_NAME) AS FTS$KEY_FIELD_TYPE,
    SEG.FTS$FIELD_NAME
FROM FTS$INDICES I
    JOIN FTS$INDEX_SEGMENTS SEG ON
          SEG.FTS$INDEX_NAME = I.FTS$INDEX_NAME
    LEFT JOIN RDB$RELATION_FIELDS RF ON
          RF.RDB$RELATION_NAME = I.FTS$RELATION_NAME AND
          RF.RDB$FIELD_NAME = SEG.FTS$FIELD_NAME
    LEFT JOIN RDB$FIELDS F ON
          F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE
WHERE I.FTS$RELATION_NAME = ? AND
      I.FTS$INDEX_STATUS = 'C' AND
      (RF.RDB$FIELD_NAME IS NOT NULL OR SEG.FTS$FIELD_NAME = 'RDB$DB_KEY')
)
SELECT DISTINCT
    FTS$KEY_FIELD_NAME,
    TRIM(FTS$KEY_FIELD_TYPE) AS FTS$KEY_FIELD_TYPE,
    FTS$FIELD_NAME
FROM T
WHERE FTS$KEY_FIELD_NAME <> FTS$FIELD_NAME
ORDER BY FTS$KEY_FIELD_NAME
)SQL",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());
		AutoRelease<IMessageMetadata> outputMetadata(output.getMetadata());

		AutoRelease<IResultSet> rs(stmt->openCursor(
			status,
			tra,
			inputMetadata,
			input.getData(),
			outputMetadata,
			0
		));

		while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
			const string keyFieldName(output->keyFieldName.str, output->keyFieldName.length);
			const string keyFieldType(output->keyFieldType.str, output->keyFieldType.length);
			const string fieldName(output->fieldName.str, output->fieldName.length);

			auto [it, result] = keyFieldBlocks.try_emplace(keyFieldName, lazy_convert_construct([] { return std::make_unique<FTSKeyFieldBlock>(); }));
			const auto& block = it->second;
			if (result) {
				block->keyFieldName = keyFieldName;
				if (keyFieldType == "UUID") {
					block->keyFieldType = FTSKeyType::UUID;
				}
				if (keyFieldType == "DBKEY") {
					block->keyFieldType = FTSKeyType::DB_KEY;
				}
				if (keyFieldType == "INT_ID") {
					block->keyFieldType = FTSKeyType::INT_ID;
				}
			}
			block->fieldNames.push_back(fieldName);

		}
		rs->close(status);
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
	/// <param name="position">Trigger position</param>
	/// <param name="triggers">Triggers list</param>
	/// 
	void FTSIndexRepository::makeTriggerSourceByRelation(
		ThrowStatusWrapper* const status,
		IAttachment* const att,
		ITransaction* const tra,
		const unsigned int sqlDialect,
		const string& relationName,
		const bool multiAction,
		const unsigned short position,
		FTSTriggerList& triggers)
	{

		FTSKeyFieldBlockMap keyFieldBlocks;

		fillKeyFieldBlocks(status, att, tra, sqlDialect, relationName, keyFieldBlocks);
		for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
			if (keyFieldBlock->fieldNames.empty()) {
				continue;
			}
		
			for (const auto& fieldName : keyFieldBlock->fieldNames) {
				const string metaFieldName = escapeMetaName(sqlDialect, fieldName);

				if (!keyFieldBlock->insertingCondition.empty()) {
					keyFieldBlock->insertingCondition += "\n      OR ";
				}
				keyFieldBlock->insertingCondition += "NEW." + metaFieldName + " IS NOT NULL";

				if (!keyFieldBlock->updatingCondition.empty()) {
					keyFieldBlock->updatingCondition += "\n      OR ";
				}
				keyFieldBlock->updatingCondition += "NEW." + metaFieldName + " IS DISTINCT FROM " + "OLD." + metaFieldName;

				if (!keyFieldBlock->deletingCondition.empty()) {
					keyFieldBlock->deletingCondition += "\n      OR ";
				}
				keyFieldBlock->deletingCondition += "OLD." + metaFieldName + " IS NOT NULL";
			}
		}

	
		if (multiAction) {
			const string triggerName = "FTS$" + relationName + "_AIUD";
			const auto& source = makeTriggerSourceByRelationMulti(keyFieldBlocks, sqlDialect, relationName);
			auto trigger = make_unique<FTSTrigger>(
				triggerName,
				relationName,
				"INSERT OR UPDATE OR DELETE",
				position,
				source
				);
			triggers.push_back(std::move(trigger));
		}
		else {
			{
				// INSERT
				const string triggerName = "FTS$" + relationName + "_AI";
				const auto& source = makeTriggerSourceByRelationInsert(keyFieldBlocks, sqlDialect, relationName);
				auto trigger = make_unique<FTSTrigger>(
					triggerName,
					relationName,
					"INSERT",
					position,
					source
					);
				triggers.push_back(std::move(trigger));
			}
			{
				// UPDATE
				const string triggerName = "FTS$" + relationName + "_AU";
				const auto& source = makeTriggerSourceByRelationUpdate(keyFieldBlocks, sqlDialect, relationName);
				auto trigger = make_unique<FTSTrigger>(
					triggerName,
					relationName,
					"UPDATE",
					position,
					source
					);
				triggers.push_back(std::move(trigger));
			}
			{
				// DELETE
				const string triggerName = "FTS$" + relationName + "_AD";
				const auto& source = makeTriggerSourceByRelationDelete(keyFieldBlocks, sqlDialect, relationName);
				auto trigger = make_unique<FTSTrigger>(
					triggerName,
					relationName,
					"DELETE",
					position,
					source
					);
				triggers.push_back(std::move(trigger));
			}
		}
	}

	const string FTSIndexRepository::makeTriggerSourceByRelationMulti(
		const FTSKeyFieldBlockMap& keyFieldBlocks,
		const unsigned int sqlDialect,
		const string& relationName
	)
	{
		string triggerSource =
			"AS\n"
			"BEGIN\n";

		for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
			const auto& procedureName = keyFieldBlock->getProcedureName();
			const string metaKeyFieldName = escapeMetaName(sqlDialect, keyFieldName);
			const string keycodeBlock =
				"  /* Block for key " + keyFieldName + " */\n"
				"  IF (INSERTING AND (" + keyFieldBlock->insertingCondition + ")) THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', NEW." + metaKeyFieldName + ", 'I');\n"
				"  IF (UPDATING AND (" + keyFieldBlock->updatingCondition + ")) THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', OLD." + metaKeyFieldName + ", 'U');\n"
				"  IF (DELETING AND (" + keyFieldBlock->deletingCondition + ")) THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', OLD." + metaKeyFieldName + ", 'D');\n";
			triggerSource += keycodeBlock;
		}

		triggerSource += 
			"END";
		return triggerSource;
	}

	const string FTSIndexRepository::makeTriggerSourceByRelationInsert(
		const FTSKeyFieldBlockMap& keyFieldBlocks,
		const unsigned int sqlDialect,
		const string& relationName
	)
	{
		string triggerSource =
			"AS\n"
			"BEGIN\n";

		for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
			const auto& procedureName = keyFieldBlock->getProcedureName();
			const string metaKeyFieldName = escapeMetaName(sqlDialect, keyFieldName);
			const string keycodeBlock =
				"  /* Block for key " + keyFieldName + " */\n"
				"  IF (" + keyFieldBlock->insertingCondition + ") THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', NEW." + metaKeyFieldName + ", 'I');\n";
			triggerSource += keycodeBlock;
		}

		triggerSource += 
			"END";
		return triggerSource;
	}

	const string FTSIndexRepository::makeTriggerSourceByRelationUpdate(
		const FTSKeyFieldBlockMap& keyFieldBlocks,
		const unsigned int sqlDialect,
		const string& relationName
	)
	{
		string triggerSource =
			"AS\n"
			"BEGIN\n";

		for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
			const auto& procedureName = keyFieldBlock->getProcedureName();
			const string metaKeyFieldName = escapeMetaName(sqlDialect, keyFieldName);
			const string keycodeBlock =
				"  /* Block for key " + keyFieldName + " */\n"
				"  IF (" + keyFieldBlock->updatingCondition + ") THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', OLD." + metaKeyFieldName + ", 'U');\n";
			triggerSource += keycodeBlock;
		}

		triggerSource += 
			"END";
		return triggerSource;
	}

	const string FTSIndexRepository::makeTriggerSourceByRelationDelete(
		const FTSKeyFieldBlockMap& keyFieldBlocks,
		const unsigned int sqlDialect,
		const string& relationName
	)
	{
		string triggerSource =
			"AS\n"
			"BEGIN\n";

		for (const auto& [keyFieldName, keyFieldBlock] : keyFieldBlocks) {
			const auto& procedureName = keyFieldBlock->getProcedureName();
			const string metaKeyFieldName = escapeMetaName(sqlDialect, keyFieldName);
			const string keycodeBlock =
				"  /* Block for key " + keyFieldName + " */\n"
				"  IF (" + keyFieldBlock->deletingCondition + ") THEN\n"
				"    EXECUTE PROCEDURE " + procedureName + "('" + relationName + "', OLD." + metaKeyFieldName + ", 'D');\n";
			triggerSource += keycodeBlock;
		}

		triggerSource += 
			"END";
		return triggerSource;
	}

}
