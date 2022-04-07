#include "LuceneUdr.h"
#include "FTSIndex.h"
#include "FTSLog.h"
#include "Relations.h"
#include "FBUtils.h"
#include "FbFieldInfo.h"
#include "EncodeUtils.h"
#include "lucene++/LuceneHeaders.h"
#include "lucene++/FileUtils.h"
#include "lucene++/QueryScorer.h"
#include "LuceneAnalyzerFactory.h"
#include <sstream>
#include <vector>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;

/***
PROCEDURE FTS$LOG_CHANGE (
    FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$REC_ID         CHAR(8) CHARACTER SET OCTETS NOT NULL,
	FTS$CHANGE_TYPE    FTS$CHANGE_TYPE NOT NULL
)
EXTERNAL NAME 'luceneudr!ftsLogChange'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsLogChange)
    FB_UDR_MESSAGE(InMessage,
	    (FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
	    (FB_INTL_VARCHAR(8, CS_BINARY), db_key)
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

		if (in->db_keyNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$REC_ID can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string dbKey(in->db_key.str, in->db_key.length);

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

		procedure->logRepository.appendLog(status, att, tra, sqlDialect, relationName, dbKey, changeType);
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
	FB_UDR_CONSTRUCTOR
		, logRepository(context->getMaster())
	{
	}

	FTSLogRepository logRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		procedure->logRepository.clearLog(status, att, tra, sqlDialect);
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
		, relationHelper(context->getMaster())
		, logRepository(context->getMaster())
		, analyzerFactory()
		, prepareStmtMap()
	{
	}

	FB_UDR_DESTRUCTOR
	{
		clearPreparedStatements();
	}

	FTSIndexRepository indexRepository;
	RelationHelper relationHelper;
	FTSLogRepository logRepository;
	LuceneAnalyzerFactory analyzerFactory;
	map<string, IStatement*> prepareStmtMap;

	void clearPreparedStatements() {
		for (auto& pStmt : prepareStmtMap) {
			pStmt.second->release();
		}
		prepareStmtMap.clear();
	}

	FB_UDR_EXECUTE_PROCEDURE
	{
		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		const string ftsDirectory = getFtsDirectory(context);

		const char* fbCharset = context->getClientCharSet();
		const string icuCharset = getICICharset(fbCharset);

		map<string, FTSRelation> relationsByName;
		procedure->clearPreparedStatements();
		
		// get all indexes
		auto allIndexes = procedure->indexRepository.getAllIndexes(status, att, tra, sqlDialect);
		for (auto& ftsIndex : allIndexes) {
			// exclude inactive indexes
			if (!ftsIndex.isActive()) {
				continue;
			}
		    // get index segments
			auto segments = procedure->indexRepository.getIndexSegments(status, att, tra, sqlDialect, ftsIndex.indexName);
			for (auto& ftsSegment : segments) {
			    // looking for a table by name
				auto r = relationsByName.find(ftsSegment.relationName);
				
				if (r != relationsByName.end()) {
					// if the table is found, then add a new index and segment to it, and update the relations map.
					auto ftsRelation = r->second;
					ftsRelation.addIndex(ftsIndex);
					ftsRelation.addSegment(ftsSegment);
					relationsByName.insert_or_assign(ftsSegment.relationName, ftsRelation);
				}
				else {
					// if there is no such table yet, add it
					FTSRelation ftsRelation(ftsSegment.relationName);
					// add a new index and segment to it
					ftsRelation.addIndex(ftsIndex);
					ftsRelation.addSegment(ftsSegment);
					relationsByName.insert_or_assign(ftsSegment.relationName, ftsRelation);
				}
			}
		}
		
		// now it is necessary for each table for each index to build queries to extract records
		for (auto& p : relationsByName) {
			const string relationName = p.first;
			auto ftsRelation = p.second;
			auto ftsIndexes = ftsRelation.getIndexes();
			for (auto& pIndex : ftsIndexes) {
				auto ftsIndex = pIndex.second;
				// exclude inactive indexes
				if (!ftsIndex.isActive()) {
					continue;
				}
				auto segments = ftsRelation.getSegmentsByIndexName(ftsIndex.indexName);
				list<string> fieldNames;
				for (const auto& segment : segments) {
					if (procedure->relationHelper.fieldExists(status, att, tra, sqlDialect, segment.relationName, segment.fieldName)) {
						fieldNames.push_back(segment.fieldName);
					}
					else {
						// if the field does not exist, then you need to mark the index as requiring updating
						if (ftsIndex.status == "C") {
							ftsIndex.status = "U";
							// this is done in an autonomous transaction
							AutoRelease<ITransaction> aTra(att->startTransaction(status, 0, nullptr));
							procedure->indexRepository.setIndexStatus(status, att, aTra, sqlDialect, ftsIndex.indexName, ftsIndex.status);
							aTra->commit(status);
							ftsRelation.updateIndex(ftsIndex);
						}
					}
				}
				const string sql = RelationHelper::buildSqlSelectFieldValues(sqlDialect, relationName, fieldNames, true);
				ftsRelation.setSql(ftsIndex.indexName, sql);
			}
			relationsByName.insert_or_assign(relationName, ftsRelation);
		}
		
		FB_MESSAGE(ValInput, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(8, CS_BINARY), db_key)
		) selValInput(status, context->getMaster());
		

		map<string, IndexWriterPtr> indexWriters;

		
		// get the log of changes of records for the index
		AutoRelease<IStatement> logStmt(att->prepare(
			status,
			tra,
			0,
			"SELECT FTS$LOG_ID, FTS$RELATION_NAME, FTS$REC_ID, FTS$CHANGE_TYPE\n"
			"FROM FTS$LOG\n"
			"ORDER BY FTS$LOG_ID",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));

		FB_MESSAGE(LogOutput, ThrowStatusWrapper,
			(FB_BIGINT, id)
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_VARCHAR(8), dbKey)	
			(FB_INTL_VARCHAR(4, CS_UTF8), changeType)

		) logOutput(status, context->getMaster());
		logOutput.clear();

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
			const string dbKey(logOutput->dbKey.str, logOutput->dbKey.length);
			const string relationName(logOutput->relationName.str, logOutput->relationName.length);
			const string changeType(logOutput->changeType.str, logOutput->changeType.length);

			const string hexDbKey = string_to_hex(dbKey);

			// looking for a table in the list of indexed tables
			auto r = relationsByName.find(relationName);
			if (r != relationsByName.end()) {
				// for each table we get a list of indexes 
				FTSRelation ftsRelation = r->second;
				auto ftsIndexes = ftsRelation.getIndexes();
				
				for (auto& pIndex : ftsIndexes) {
					const string indexName = pIndex.first;
					auto ftsIndex = pIndex.second;
					// exclude inactive indexes
					if (!ftsIndex.isActive()) {
						continue;
					}
					auto ftsSegments = ftsRelation.getSegmentsByIndexName(indexName);
					// looking for an IndexWriter
					auto iWriter = indexWriters.find(ftsIndex.indexName);
					// if not found, then open this
					if (iWriter == indexWriters.end()) {
						// Check if the index directory exists, and if it doesn't exist, create it. 
						const auto unicodeIndexDir = FileUtils::joinPath(StringUtils::toUnicode(ftsDirectory), StringUtils::toUnicode(indexName));
						if (!FileUtils::isDirectory(unicodeIndexDir)) {
							if (ftsIndex.status == "C") {
								// If the index directory does not exist, then mark the index as requiring reindexing.
								AutoRelease<ITransaction> aTra(att->startTransaction(status, 0, nullptr));
								procedure->indexRepository.setIndexStatus(status, att, aTra, sqlDialect, ftsIndex.indexName, ftsIndex.status);
								aTra->commit(status);
							}
							// go to next index
							continue;
						}
						auto fsIndexDir = FSDirectory::open(unicodeIndexDir);
						auto analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex.analyzer);
						IndexWriterPtr writer = newLucene<IndexWriter>(fsIndexDir, analyzer, IndexWriter::MaxFieldLengthLIMITED);
						indexWriters[indexName] = writer;
					}
					IndexWriterPtr writer = indexWriters[indexName];
					if ((changeType == "I") || (changeType == "U")) {
						const string stmtName = relationName + "." + indexName;
						// looking for a prepared statement, and if it is not there, we prepare it
						auto iStmt = procedure->prepareStmtMap.find(stmtName);
						if (iStmt == procedure->prepareStmtMap.end()) {
							IStatement* stmt = att->prepare(
								status,
								tra,
								0,
								ftsRelation.getSql(indexName).c_str(),
								sqlDialect,
								IStatement::PREPARE_PREFETCH_METADATA
							);
							procedure->prepareStmtMap[stmtName] = stmt;
						}
						auto stmt = procedure->prepareStmtMap[stmtName];
						// get the desired field values
						AutoRelease<IMessageMetadata> outputMetadata(stmt->getOutputMetadata(status));
						const unsigned colCount = outputMetadata->getCount(status);
						// make all fields of string type except BLOB
						AutoRelease<IMessageMetadata> newMeta(prepareTextMetaData(status, outputMetadata));
						auto fields = FbFieldsInfo(status, newMeta);

						selValInput->db_keyNull = false;
						selValInput->db_key.length = static_cast<ISC_USHORT>(dbKey.length());
						dbKey.copy(selValInput->db_key.str, selValInput->db_key.length);

						AutoRelease<IResultSet> rs(stmt->openCursor(
							status,
							tra,
							selValInput.getMetadata(),
							selValInput.getData(),
							newMeta,
							0
						));

						const unsigned msgLength = newMeta->getMessageLength(status);
						{
							// allocate output buffer
							auto b = make_unique<unsigned char[]>(msgLength);
							unsigned char* buffer = b.get();
							while (rs->fetchNext(status, buffer) == IStatus::RESULT_OK) {
								bool emptyFlag = true;
								DocumentPtr doc = newLucene<Document>();
								doc->add(newLucene<Field>(L"RDB$DB_KEY", StringUtils::toUnicode(hexDbKey), Field::STORE_YES, Field::INDEX_NOT_ANALYZED));
								doc->add(newLucene<Field>(L"RDB$RELATION_NAME", StringUtils::toUnicode(relationName), Field::STORE_YES, Field::INDEX_NOT_ANALYZED));
								for (unsigned int i = 1; i < colCount; i++) {
									auto field = fields[i];
									bool nullFlag = field.isNull(buffer);
									string value;
									if (!nullFlag) {
										value = field.getStringValue(status, att, tra, buffer);
									}
									const auto fieldName = StringUtils::toUnicode(relationName + "." + field.fieldName);
									Lucene::String unicodeValue;
									if (!value.empty()) {
										// re-encode content to Unicode only if the string is non-empty
										unicodeValue = StringUtils::toUnicode(to_utf8(value, icuCharset));
									}
									auto luceneField = newLucene<Field>(fieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED);
									
									auto iSegment = std::find_if(
										ftsSegments.begin(), 
										ftsSegments.end(), 
										[&field](FTSIndexSegment ftsSegment) { return ftsSegment.fieldName == field.fieldName; }
									);
									if (iSegment != ftsSegments.end()) {
										luceneField->setBoost((*iSegment).boost);
									}
									
									doc->add(luceneField);
									emptyFlag = emptyFlag && value.empty();
								}
								if ((changeType == "I") && !emptyFlag) {
									writer->addDocument(doc);
								}
								if (changeType == "U") {
									TermPtr term = newLucene<Term>(L"RDB$DB_KEY", StringUtils::toUnicode(hexDbKey));
									if (!emptyFlag) {
										writer->updateDocument(term, doc);
									}
									else {
										writer->deleteDocuments(term);
									}
								}
							}
							rs->close(status);
						}
					}
					else if (changeType == "D") {
						TermPtr term = newLucene<Term>(L"RDB$DB_KEY", StringUtils::toUnicode(hexDbKey));
						writer->deleteDocuments(term);
					}
				}
			}
			procedure->logRepository.deleteLog(status, att, tra, sqlDialect, logId);
		}
		logRs->close(status);
		// commit changes for all indexes
		for (auto& pIndexWriter : indexWriters) {
			auto indexWriter = pIndexWriter.second;
			indexWriter->commit();
			indexWriter->close();
		}
		// clean up prepared statements
		procedure->clearPreparedStatements();
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$SEARCH (
	FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$SEARCH_RELATION VARCHAR(63) CHARACTER SET UTF8,
	FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
	FTS$LIMIT INT NOT NULL DEFAULT 1000,
	FTS$EXPLAIN BOOLEAN DEFAULT FALSE
)
RETURNS (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
	FTS$REC_ID CHAR(8) CHARACTER SET OCTETS,
	FTS$SCORE DOUBLE PRECISION,
	FTS$EXPLANATION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!ftsSearch'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsSearch)
    FB_UDR_MESSAGE(InMessage,
	    (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_INTL_VARCHAR(32765, CS_UTF8), query)
		(FB_INTEGER, limit)
		(FB_BOOLEAN, explain)
    );

	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_INTL_VARCHAR(8, CS_BINARY), rec_id)
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
	    if (in->index_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Index name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
	    }
	    const string indexName(in->index_name.str, in->index_name.length);

		string relationName;
		if (!in->relation_nameNull) {
			relationName.assign(in->relation_name.str, in->relation_name.length);
		}

		string queryStr;
		if (!in->queryNull) {
			queryStr.assign(in->query.str, in->query.length);
		}

		const auto limit = static_cast<int32_t>(in->limit);

		if (!in->explainNull) {
			explainFlag = in->explain;
		}

		const string ftsDirectory = getFtsDirectory(context);


		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		auto ftsIndex = procedure->indexRepository.getIndex(status, att, tra, sqlDialect, indexName);

		// check if directory exists for index
		const auto indexDir = FileUtils::joinPath(StringUtils::toUnicode(ftsDirectory), StringUtils::toUnicode(indexName));
		if (ftsIndex.status == "N" || !FileUtils::isDirectory(indexDir)) {			
			string error_message = string_format("Index \"%s\" exists, but is not build. Please rebuild index.", indexName);
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		try {
			const auto ftsIndexDir = FSDirectory::open(indexDir);
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
			AnalyzerPtr analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex.analyzer);
			auto segments = procedure->indexRepository.getIndexSegments(status, att, tra, sqlDialect, indexName);
			if (!relationName.empty()) {
				// if a table name is given, then select only segments with this table
				auto segmentsByRelation = FTSIndexRepository::groupIndexSegmentsByRelation(segments);
				auto el = segmentsByRelation.find(relationName);
				if (el == segmentsByRelation.end()) {
					const string error_message = string_format("Relation \"%s\" not exists in index \"%s\".", relationName, indexName);
					ISC_STATUS statusVector[] = {
						isc_arg_gds, isc_random,
						isc_arg_string, (ISC_STATUS)error_message.c_str(),
						isc_arg_end
					};
					throw FbException(status, statusVector);
				}
				segments = el->second;
			}
			
			auto fields = Collection<String>::newInstance();
			for (auto segment: segments) {
				fields.add(StringUtils::toUnicode(segment.getFullFieldName()));
			}

			MultiFieldQueryParserPtr parser = newLucene<MultiFieldQueryParser>(LuceneVersion::LUCENE_CURRENT, fields, analyzer);
			parser->setDefaultOperator(QueryParser::OR_OPERATOR);
			query = parser->parse(StringUtils::toUnicode(queryStr));
			TopDocsPtr docs = searcher->search(query, limit);

			scoreDocs = docs->scoreDocs;

			it = scoreDocs.begin();
			
			out->relation_nameNull = true;
			out->rec_idNull = true;
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
		const string relationName = StringUtils::toUTF8(doc->get(L"RDB$RELATION_NAME"));
		const string hexDbKey = StringUtils::toUTF8(doc->get(L"RDB$DB_KEY"));
		// In the Lucene index, the string is stored in hexadecimal form, so let's convert it back to binary format.
		const string dbKey = hex_to_string(hexDbKey);
		
        out->relation_nameNull = false;
		out->relation_name.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(out->relation_name.str, out->relation_name.length);
		
		out->rec_idNull = false;
		out->rec_id.length = static_cast<ISC_USHORT>(dbKey.length());
		dbKey.copy(out->rec_id.str, out->rec_id.length);

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


FB_UDR_IMPLEMENT_ENTRY_POINT