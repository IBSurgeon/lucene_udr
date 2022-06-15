/**
 *  Implementation of basic full-text search procedures.
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
#include "FTSIndex.h"
#include "FTSUtils.h"
#include "FTSLog.h"
#include "Relations.h"
#include "FBUtils.h"
#include "FBFieldInfo.h"
#include "EncodeUtils.h"
#include "LuceneHeaders.h"
#include "FileUtils.h"
#include "QueryScorer.h"
#include "LuceneAnalyzerFactory.h"
#include <sstream>
#include <vector>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;

/***
PROCEDURE FTS$SEARCH (
	FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
	FTS$LIMIT INT NOT NULL DEFAULT 1000,
	FTS$EXPLAIN BOOLEAN DEFAULT FALSE
)
RETURNS (
	FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
	FTS$KEY_FIELD_NAME VARCHAR(63) CHARACTER SET UTF8,
	FTS$DB_KEY CHAR(8) CHARACTER SET OCTETS,
	FTS$ID BIGINT,
	FTS$UUID CHAR(16) CHARACTER SET OCTETS,
	FTS$SCORE DOUBLE PRECISION,
	FTS$EXPLANATION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!ftsSearch'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsSearch)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(32765, CS_UTF8), query)
		(FB_INTEGER, limit)
		(FB_BOOLEAN, explain)
	);

	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), keyFieldName)
		(FB_INTL_VARCHAR(8, CS_BINARY), dbKey)
		(FB_BIGINT, id)
		(FB_INTL_VARCHAR(16, CS_BINARY), uuid)
		(FB_DOUBLE, score)
		(FB_BLOB, explanation)
	);

	FB_UDR_CONSTRUCTOR
	, indexRepository(context->getMaster())
	, analyzerFactory()
	{
	}

	FTSIndexRepository indexRepository;
	LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->indexNameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Index name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string indexName(in->indexName.str, in->indexName.length);

		string queryStr;
		if (!in->queryNull) {
			queryStr.assign(in->query.str, in->query.length);
		}

		const auto limit = static_cast<int32_t>(in->limit);

		if (!in->explainNull) {
			explainFlag = in->explain;
		}

		const auto& ftsDirectoryPath = getFtsDirectory(context);


		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		const auto& ftsIndex = procedure->indexRepository.getIndex(status, att, tra, sqlDialect, indexName, true);

		// check if directory exists for index
		const auto& indexDirectoryPath = ftsDirectoryPath / indexName;
		if (ftsIndex->status == "N" || !fs::is_directory(indexDirectoryPath)) {
			string error_message = string_format("Index \"%s\" exists, but is not build. Please rebuild index.", indexName);
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		try {
			const auto& ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
			if (!IndexReader::indexExists(ftsIndexDir)) {
				const string error_message = string_format("Index \"%s\" exists, but is not build. Please rebuild index.", indexName);
				ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS)error_message.c_str(),
					isc_arg_end
				};
				throw FbException(status, statusVector);
			}

			IndexReaderPtr reader = IndexReader::open(ftsIndexDir, true);
			searcher = newLucene<IndexSearcher>(reader);
			AnalyzerPtr analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex->analyzer);

			string keyFieldName;
			auto fields = Collection<String>::newInstance();
			for (const auto& segment : ftsIndex->segments) {
				if (!segment->key) {
					fields.add(StringUtils::toUnicode(segment->fieldName));
				}
				else {
					keyFieldName = segment->fieldName;
					unicodeKeyFieldName = StringUtils::toUnicode(keyFieldName);
				}
			}

			if (keyFieldName != "RDB$DB_KEY") {
				keyFieldInfo = procedure->indexRepository.relationHelper.getField(status, att, tra, sqlDialect, ftsIndex->relationName, keyFieldName);
			}
			else {
				keyFieldInfo = make_unique<RelationFieldInfo>();
				keyFieldInfo->initDB_KEYField(ftsIndex->relationName);
			}

			MultiFieldQueryParserPtr parser = newLucene<MultiFieldQueryParser>(LuceneVersion::LUCENE_CURRENT, fields, analyzer);
			parser->setDefaultOperator(QueryParser::OR_OPERATOR);
			query = parser->parse(StringUtils::toUnicode(queryStr));
			TopDocsPtr docs = searcher->search(query, limit);

			scoreDocs = docs->scoreDocs;

			it = scoreDocs.begin();

			out->relationNameNull = false;
			out->relationName.length = static_cast<ISC_USHORT>(ftsIndex->relationName.length());
			ftsIndex->relationName.copy(out->relationName.str, out->relationName.length);

			out->keyFieldNameNull = false;
			out->keyFieldName.length = static_cast<ISC_USHORT>(keyFieldName.length());
			keyFieldName.copy(out->keyFieldName.str, out->keyFieldName.length);


			out->dbKeyNull = true;
			out->uuidNull = true;
			out->idNull = true;
			out->scoreNull = true;
		}
		catch (LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	}

	bool explainFlag = false;
	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	RelationFieldInfoPtr keyFieldInfo;
	String unicodeKeyFieldName;
	QueryPtr query;
	SearcherPtr searcher;
	Collection<ScoreDocPtr> scoreDocs;
	Collection<ScoreDocPtr>::iterator it;

	FB_UDR_FETCH_PROCEDURE
	{
		if (it == scoreDocs.end()) {
			return false;
		}
		ScoreDocPtr scoreDoc = *it;
		DocumentPtr doc = searcher->doc(scoreDoc->doc);

		try {
			const string keyValue = StringUtils::toUTF8(doc->get(unicodeKeyFieldName));
			if (unicodeKeyFieldName == L"RDB$DB_KEY") {
				// In the Lucene index, the string is stored in hexadecimal form, so let's convert it back to binary format.
				const string dbKey = hex_to_string(keyValue);
				out->dbKeyNull = false;
				out->dbKey.length = static_cast<ISC_USHORT>(dbKey.length());
				dbKey.copy(out->dbKey.str, out->dbKey.length);
			}
			else if (keyFieldInfo->isBinary()) {
				// In the Lucene index, the string is stored in hexadecimal form, so let's convert it back to binary format.
				const string uuid = hex_to_string(keyValue);
				out->uuidNull = false;
				out->uuid.length = static_cast<ISC_USHORT>(uuid.length());
				uuid.copy(out->uuid.str, out->uuid.length);
			}
			else if (keyFieldInfo->isInt()) {
				out->idNull = false;
				out->id = std::stoll(keyValue);
			}
		}
		catch (invalid_argument& e) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)e.what(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}


		out->scoreNull = false;
		out->score = scoreDoc->score;

		if (explainFlag) {
			out->explanationNull = false;
			const auto explanation = searcher->explain(query, scoreDoc->doc);
			const string explanationStr = StringUtils::toUTF8(explanation->toString());
			AutoRelease<IBlob> blob(att->createBlob(status, tra, &out->explanation, 0, nullptr));
			blob_set_string(status, blob, explanationStr);
			blob->close(status);
		}
		else {
			out->explanationNull = true;
		}

		++it;
		return true;
	}
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$LOG_BY_DBKEY (
    FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$DBKEY          CHAR(8) CHARACTER SET OCTETS NOT NULL,
	FTS$CHANGE_TYPE    FTS$D_CHANGE_TYPE NOT NULL
)
EXTERNAL NAME 'luceneudr!ftsLogByDdKey'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsLogByDdKey)
    FB_UDR_MESSAGE(InMessage,
	    (FB_INTL_VARCHAR(252, CS_UTF8), relationName)
	    (FB_INTL_VARCHAR(8, CS_BINARY), dbKey)
		(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
    );

	FB_UDR_CONSTRUCTOR
		, logRepository(context->getMaster())
	{
	}

	FTSLogRepository logRepository;

    FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->relationNameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$RELATION_NAME can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	    const string relationName(in->relationName.str, in->relationName.length);

		if (in->dbKeyNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$DBKEY can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string dbKey(in->dbKey.str, in->dbKey.length);

		if (in->changeTypeNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$CHANGE_TYPE can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string changeType(in->changeType.str, in->changeType.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		procedure->logRepository.appendLogByDbKey(status, att, tra, sqlDialect, relationName, dbKey, changeType);
	}

    FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$LOG_BY_ID (
	FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$ID             BIGINT NOT NULL,
	FTS$CHANGE_TYPE    FTS$D_CHANGE_TYPE NOT NULL
)
EXTERNAL NAME 'luceneudr!ftsLogById'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsLogById)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_BIGINT, id)
		(FB_INTL_VARCHAR(4, CS_UTF8), change_type)
	);

	FB_UDR_CONSTRUCTOR
		, logRepository(context->getMaster())
	{
	}

	FTSLogRepository logRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->relation_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$RELATION_NAME can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string relationName(in->relation_name.str, in->relation_name.length);

		if (in->idNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$ID can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const ISC_INT64 id = in->id;

		if (in->change_typeNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$CHANGE_TYPE can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string changeType(in->change_type.str, in->change_type.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		procedure->logRepository.appendLogById(status, att, tra, sqlDialect, relationName, id, changeType);
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$LOG_BY_UUID (
	FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$UUID           CHAR(16) CHARACTER SET OCTETS NOT NULL,
	FTS$CHANGE_TYPE    FTS$D_CHANGE_TYPE NOT NULL
)
EXTERNAL NAME 'luceneudr!ftsLogByUuid'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsLogByUuid)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(16, CS_BINARY), uuid)
		(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
	);

	FB_UDR_CONSTRUCTOR
		, logRepository(context->getMaster())
	{
	}

	FTSLogRepository logRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->relationNameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$RELATION_NAME can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string relationName(in->relationName.str, in->relationName.length);

		if (in->uuidNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$UUID can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string uuid(in->uuid.str, in->uuid.length);

		if (in->changeTypeNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$CHANGE_TYPE can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string changeType(in->changeType.str, in->changeType.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		procedure->logRepository.appendLogByUuid(status, att, tra, sqlDialect, relationName, uuid, changeType);
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$CLEAR_LOG
EXTERNAL NAME 'luceneudr!ftsClearLog'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsClearLog)

	FB_UDR_EXECUTE_PROCEDURE
	{

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		att->execute(
			status,
			tra,
			0,
			"DELETE FROM FTS$LOG",
			sqlDialect,
			nullptr,
			nullptr,
			nullptr,
			nullptr
		);
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$UPDATE_INDEXES 
EXTERNAL NAME 'luceneudr!updateFtsIndexes'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(updateFtsIndexes)

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
		, logRepository(context->getMaster())
		, analyzerFactory()
	{
	}


	FTSIndexRepository indexRepository;
	FTSLogRepository logRepository;
	LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_PROCEDURE
	{
		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		const auto& ftsDirectoryPath = getFtsDirectory(context);

		const char* fbCharset = context->getClientCharSet();
		
		// get all indexes with segments
		FTSIndexMap indexes;
		procedure->indexRepository.fillAllIndexesWithSegments(status, att, tra, sqlDialect, indexes);
		// fill map indexes of relationName
		map<string, list<string>> indexesByRelation;
		for (const auto& [indexName, ftsIndex] : indexes) {	
			if (!ftsIndex->isActive()) {
				continue;
			}
			const auto& indexDirectoryPath = ftsDirectoryPath / indexName;
			if (!ftsIndex->checkAllFieldsExists() || !fs::is_directory(indexDirectoryPath)) {
				// index need to rebuild
				setIndexToRebuild(status, att, sqlDialect, ftsIndex);
				// go to next index
				continue;
			}
			ftsIndex->unicodeIndexDir = indexDirectoryPath.wstring();

			// collect and save the SQL query to extract the record by key
			ftsIndex->prepareExtractRecordStmt(status, att, tra, sqlDialect);

			const auto& inMetadata = ftsIndex->getInExtractRecordMetadata();
			if (inMetadata->getCount(status) != 1) {
				// The number of input parameters must be equal to 1.
				// index need to rebuild
				setIndexToRebuild(status, att, sqlDialect, ftsIndex);
				// go to next index
				continue;
			}
			const auto& params = make_unique<FbFieldsInfo>(status, inMetadata);
			const auto& keyParam = params->at(0);
			if (keyParam->isBinary()) {
				switch (keyParam->length) {
				case 8:
					ftsIndex->keyFieldType = FTSKeyType::DB_KEY;
					break;
				case 16:
					ftsIndex->keyFieldType = FTSKeyType::UUID;
					break;
				default:
					// index need to rebuild
					setIndexToRebuild(status, att, sqlDialect, ftsIndex);
					// go to next index
					continue;
				}
			}
			else if (keyParam->isInt()) {
				ftsIndex->keyFieldType = FTSKeyType::INT_ID;
			}
			else {
				// index need to rebuild
				setIndexToRebuild(status, att, sqlDialect, ftsIndex);
				// go to next index
				continue;
			}


			const auto& outMetadata =  ftsIndex->getOutExtractRecordMetadata();
			auto fields = make_unique<FbFieldsInfo>(status, outMetadata);
			// initial specific FTS property for fields
			for (int i = 0; i < fields->size(); i++) {
				const auto& field = fields->at(i);
				auto iSegment = ftsIndex->findSegment(field->fieldName);
				if (iSegment == ftsIndex->segments.end()) {
					// index need to rebuild
					setIndexToRebuild(status, att, sqlDialect, ftsIndex);
					// go to next index
					continue;
				}
				auto const& segment = *iSegment;
				field->ftsFieldName = StringUtils::toUnicode(segment->fieldName);
				field->ftsKey = segment->key;
				field->ftsBoost = segment->boost;
				field->ftsBoostNull = segment->boostNull;
				if (field->ftsKey) {
					ftsIndex->unicodeKeyFieldName = field->ftsFieldName;
				}
			}
			fieldsInfoMap[indexName] = std::move(fields);

            // put indexName to map
			auto it = indexesByRelation.find(ftsIndex->relationName);
			if (it == indexesByRelation.end()) {
				list<string> indexNames;
				indexNames.push_back(ftsIndex->indexName);
				indexesByRelation[ftsIndex->relationName] = indexNames;
			}
			else {
				it->second.push_back(ftsIndex->indexName);
			}
		}

		try 
		{
			DBKEYInput dbKeyInput(status, context->getMaster());
			UUIDInput uuidInput(status, context->getMaster());
			IDInput idInput(status, context->getMaster());

			FBStringEncoder fbStringEncoder(fbCharset);

			// prepare statement for delete record from FTS log
			AutoRelease<IStatement> logDeleteStmt(att->prepare(
				status,
				tra,
				0,
				SQL_DELETE_FTS_LOG,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));

			LogDelInput logDelInput(status, context->getMaster());

			// prepare statement for retrieval record from FTS log 
			AutoRelease<IStatement> logStmt(att->prepare(
				status,
				tra,
				0,
				SQL_SELECT_FTS_LOG,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));

			LogOutput logOutput(status, context->getMaster());

			AutoRelease<IResultSet> logRs(logStmt->openCursor(
				status,
				tra,
				nullptr,
				nullptr,
				logOutput.getMetadata(),
				0
			));


			while (logRs->fetchNext(status, logOutput.getData()) == IStatus::RESULT_OK) {
				const ISC_INT64 logId = logOutput->id;
				const string relationName(logOutput->relationName.str, logOutput->relationName.length);
				const string changeType(logOutput->changeType.str, logOutput->changeType.length);


				const auto itIndexNames = indexesByRelation.find(relationName);
				if (itIndexNames == indexesByRelation.end()) {
					continue;
				}
				const auto& indexNames = itIndexNames->second;

				// for all indexes for relationName
				for (const auto& indexName : indexNames) {
					const auto& ftsIndex = indexes[indexName];
					const auto& indexWriter = getIndexWriter(status, ftsIndex);

					Lucene::String unicodeKeyValue;
					switch (ftsIndex->keyFieldType) {
					case FTSKeyType::DB_KEY:
						if (!logOutput->dbKeyNull) {
							string dbKey(logOutput->dbKey.str, logOutput->dbKey.length);
							unicodeKeyValue = StringUtils::toUnicode(string_to_hex(dbKey));
						}
						break;
					case FTSKeyType::UUID:
						if (!logOutput->uuidNull) {
							string uuid(logOutput->uuid.str, logOutput->uuid.length);
							unicodeKeyValue = StringUtils::toUnicode(string_to_hex(uuid));
						}
						break;
					case FTSKeyType::INT_ID:
						if (!logOutput->recIdNull) {
							string id = std::to_string(logOutput->recId);
							unicodeKeyValue = StringUtils::toUnicode(id);
						}
						break;
					default:
						continue;
					}

					if (unicodeKeyValue.empty()) {
						continue;
					}

					if (changeType == "D") {						
						TermPtr term = newLucene<Term>(ftsIndex->unicodeKeyFieldName, unicodeKeyValue);
						indexWriter->deleteDocuments(term);		
						continue;
					}

					const auto& stmt = ftsIndex->getPreparedExtractRecordStmt();
					const auto& outMetadata = ftsIndex->getOutExtractRecordMetadata();

					const auto& fields = *fieldsInfoMap[indexName];

					AutoRelease<IResultSet> rs(nullptr);
					switch (ftsIndex->keyFieldType) {
					case FTSKeyType::DB_KEY:
						if (logOutput->dbKeyNull) continue;
						dbKeyInput->dbKeyNull = logOutput->dbKeyNull;
						dbKeyInput->dbKey.length = logOutput->dbKey.length;
						memcpy(dbKeyInput->dbKey.str, logOutput->dbKey.str, logOutput->dbKey.length);
						rs.reset(stmt->openCursor(
							status,
							tra,
							dbKeyInput.getMetadata(),
							dbKeyInput.getData(),
							outMetadata,
							0
						));
						break;
					case FTSKeyType::UUID:
						if (logOutput->uuidNull) continue;
						uuidInput->uuidNull = logOutput->uuidNull;
						uuidInput->uuid.length = logOutput->uuid.length;
						memcpy(uuidInput->uuid.str, logOutput->uuid.str, uuidInput->uuid.length);
						rs.reset(stmt->openCursor(
							status,
							tra,
							uuidInput.getMetadata(),
							uuidInput.getData(),
							outMetadata,
							0
						));
						break;
					case FTSKeyType::INT_ID:
						if (logOutput->recIdNull) continue;
						idInput->idNull = logOutput->recIdNull;
						idInput->id = logOutput->recId;
						rs.reset(stmt->openCursor(
							status,
							tra,
							idInput.getMetadata(),
							idInput.getData(),
							outMetadata,
							0
						));
						break;
					default:
						continue;
					}

					if (!rs.hasData()) {
						continue;
					}

					const unsigned colCount = outMetadata->getCount(status);
					const unsigned msgLength = outMetadata->getMessageLength(status);
					{
						// allocate output buffer
						auto b = make_unique<unsigned char[]>(msgLength);
						unsigned char* buffer = b.get();
						memset(buffer, 0, msgLength);
						while (rs->fetchNext(status, buffer) == IStatus::RESULT_OK) {
							bool emptyFlag = true;
							const auto& doc = newLucene<Document>();

							for (unsigned int i = 0; i < colCount; i++) {
								const auto& field = fields[i];

								Lucene::String unicodeValue;
								if (!field->isNull(buffer)) {
									const string value = field->getStringValue(status, att, tra, buffer);
									if (!value.empty()) {
										// re-encode content to Unicode only if the string is non-binary
										if (!field->isBinary()) {
											unicodeValue = fbStringEncoder.toUnicode(value);
										}
										else {
											// convert the binary string to a hexadecimal representation
											unicodeValue = StringUtils::toUnicode(string_to_hex(value));
										}
									}
								}
								// add field to document
								if (field->ftsKey) {
									const auto& luceneField = newLucene<Field>(field->ftsFieldName, unicodeValue, Field::STORE_YES, Field::INDEX_NOT_ANALYZED);
									doc->add(luceneField);
								}
								else {
									const auto& luceneField = newLucene<Field>(field->ftsFieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED);
									if (!field->ftsBoostNull) {
										luceneField->setBoost(field->ftsBoost);
									}
									doc->add(luceneField);
									emptyFlag = emptyFlag && unicodeValue.empty();
								}

							}
							if ((changeType == "I") && !emptyFlag) {
								indexWriter->addDocument(doc);
							}
							if (changeType == "U") {
								if (!ftsIndex->unicodeKeyFieldName.empty()) {
									TermPtr term = newLucene<Term>(ftsIndex->unicodeKeyFieldName, unicodeKeyValue);
									if (!emptyFlag) {
										indexWriter->updateDocument(term, doc);
									}
									else {
										indexWriter->deleteDocuments(term);
									}
								}
							}
							memset(buffer, 0, msgLength);
						}
						rs->close(status);
					}
				}
				// delete record from FTS log
				logDelInput->idNull = false;
				logDelInput->id = logId;
				logDeleteStmt->execute(
					status,
					tra,
					logDelInput.getMetadata(),
					logDelInput.getData(),
					nullptr,
					nullptr
				);
				
			}
			logRs->close(status);
			// commit changes for all indexes
			for (const auto& pIndexWriter : indexWriters) {
				const auto& indexWriter = pIndexWriter.second;
				indexWriter->commit();
				indexWriter->close();
			}
		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	}

	// Input message for extracting a record by RDB$DB_KEY
	FB_MESSAGE(DBKEYInput, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(8, CS_BINARY), dbKey)
	);

	// Input message for extracting a record by UUID
	FB_MESSAGE(UUIDInput, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(16, CS_BINARY), uuid)
	);

	// Input message for extracting a record by integer ID
	FB_MESSAGE(IDInput, ThrowStatusWrapper,
		(FB_BIGINT, id)
	);

	// Input message for the FTS log record delete statement
	FB_MESSAGE(LogDelInput, ThrowStatusWrapper,
		(FB_BIGINT, id)
	);

	const char* SQL_DELETE_FTS_LOG =
R"SQL(
DELETE FROM FTS$LOG
WHERE FTS$LOG_ID = ?
)SQL";

	// FTS log output message
	FB_MESSAGE(LogOutput, ThrowStatusWrapper,
		(FB_BIGINT, id)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_VARCHAR(8), dbKey)
		(FB_VARCHAR(16), uuid)
		(FB_BIGINT, recId)
		(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
	);

	const char* SQL_SELECT_FTS_LOG =
R"SQL(
SELECT
    FTS$LOG_ID
  , TRIM(FTS$RELATION_NAME) AS FTS$RELATION_NAME
  , FTS$DB_KEY
  , FTS$REC_UUID
  , FTS$REC_ID
  , FTS$CHANGE_TYPE
FROM FTS$LOG
ORDER BY FTS$LOG_ID
)SQL";

	map<string, FbFieldsInfoPtr> fieldsInfoMap;
	map<string, IndexWriterPtr> indexWriters;


	void setIndexToRebuild(ThrowStatusWrapper* const status, IAttachment* const att, const unsigned int sqlDialect, const FTSIndexPtr& ftsIndex)
	{
		ftsIndex->status = "U";
		// this is done in an autonomous transaction				
		AutoRelease<ITransaction> tra(att->startTransaction(status, 0, nullptr));
		procedure->indexRepository.setIndexStatus(status, att, tra, sqlDialect, ftsIndex->indexName, ftsIndex->status);
		tra->commit(status);
	}

	IndexWriterPtr const getIndexWriter(ThrowStatusWrapper* const status, const FTSIndexPtr& ftsIndex)
	{
		const auto it = indexWriters.find(ftsIndex->indexName);
		if (it == indexWriters.end()) {
			const auto& fsIndexDir = FSDirectory::open(ftsIndex->unicodeIndexDir);
			const auto& analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex->analyzer);
			const auto& indexWriter = newLucene<IndexWriter>(fsIndexDir, analyzer, IndexWriter::MaxFieldLengthLIMITED);
			indexWriters[ftsIndex->indexName] = indexWriter;
			return indexWriter;
		}
		else {
			return it->second;
		}
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE

FB_UDR_IMPLEMENT_ENTRY_POINT
