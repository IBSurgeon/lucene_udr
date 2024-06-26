/**
 *  Implementation of basic full-text search procedures.
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
#include "FTSIndex.h"
#include "FTSUtils.h"
#include "Relations.h"
#include "FBUtils.h"
#include "FBFieldInfo.h"
#include "LuceneHeaders.h"
#include "FileUtils.h"
#include "TermAttribute.h"
#include "Analyzers.h"
#include "LuceneAnalyzerFactory.h"
#include "Utils.h"
#include <string>
#include <sstream>
#include <memory>
#include <unordered_map>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace FTSMetadata;
using namespace LuceneUDR;

namespace {

    std::string queryEscape(std::string_view query)
    {
        std::string s;
        s.reserve(query.size());
        for (auto ch : query) {
            switch (ch) {
            case '+':
            case '-':
            case '!':
            case '^':
            case '"':
            case '~':
            case '*':
            case '?':
            case ':':
            case '\\':
            case '&':
            case '|':
            case '(':
            case ')':
            case '[':
            case ']':
            case '{':
            case '}':
                s+= '\\' + ch;
                break;
            default:
                s += ch;
            }
        }
        return s;
    }
}

/***
FUNCTION FTS$ESCAPE_QUERY (
    FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8
)
RETURNS VARCHAR(8191) CHARACTER SET UTF8
EXTERNAL NAME 'luceneudr!ftsEscapeQuery'
ENGINE UDR;
***/
FB_UDR_BEGIN_FUNCTION(ftsEscapeQuery)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(32765, CS_UTF8), query)
    );

    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(32765, CS_UTF8), query)
    );

    FB_UDR_EXECUTE_FUNCTION
    {
        if (!in->queryNull) {
            std::string_view queryStr(in->query.str, in->query.length);

            const std::string escapedQuery = queryEscape(queryStr);

            out->queryNull = false;
            out->query.length = static_cast<ISC_USHORT>(escapedQuery.length());
            escapedQuery.copy(out->query.str, out->query.length);
        }
        else {
            out->queryNull = true;
        }
    }
FB_UDR_END_FUNCTION

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
        , indexRepository(std::make_unique<FTSIndexRepository>(context->getMaster()))
    {
    }

    FTSIndexRepositoryPtr indexRepository{nullptr};

    
    void getCharSet([[maybe_unused]] ThrowStatusWrapper* status, [[maybe_unused]] IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }
    
    FB_UDR_EXECUTE_PROCEDURE
    {
        if (in->indexNameNull) {
            throwException(status, "Index name can not be NULL");
        }
        std::string_view indexName(in->indexName.str, in->indexName.length);
        
        std::string queryStr;
        if (!in->queryNull) {
            queryStr.assign(in->query.str, in->query.length);
        }

        const auto limit = static_cast<int32_t>(in->limit);

        if (!in->explainNull) {
            explainFlag = in->explain;
        }

        const auto ftsDirectoryPath = getFtsDirectory(status, context);

        att.reset(context->getAttachment(status));
        tra.reset(context->getTransaction(status));

        unsigned int sqlDialect = getSqlDialect(status, att);

        auto ftsIndex = procedure->indexRepository->getIndex(status, att, tra, sqlDialect, indexName, true);

        // check if directory exists for index
        const auto indexDirectoryPath = ftsDirectoryPath / indexName;
        if (ftsIndex.status == "N" || !fs::is_directory(indexDirectoryPath)) {
            std::string sIndexName(indexName);
            throwException(status, R"(Index "%s" exists, but is not build. Please rebuild index.)", sIndexName.c_str());
        }

        try {
            auto ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
            if (!IndexReader::indexExists(ftsIndexDir)) {
                std::string sIndexName(indexName);
                throwException(status, R"(Index "%s" exists, but is not build. Please rebuild index.)", sIndexName.c_str());
            }

            const auto analyzers = procedure->indexRepository->getAnalyzerRepository();
            AnalyzerPtr analyzer = analyzers->createAnalyzer(status, att, tra, sqlDialect, ftsIndex.analyzer);
            searcher = newLucene<IndexSearcher>(ftsIndexDir, true);
            
            std::string keyFieldName;
            auto fields = Collection<String>::newInstance();
            for (const auto& segment : ftsIndex.segments) {
                if (!segment.isKey()) {
                    fields.add(StringUtils::toUnicode(segment.fieldName()));
                }
                else {
                    keyFieldName = segment.fieldName();
                    unicodeKeyFieldName = StringUtils::toUnicode(keyFieldName);
                }
            }

            keyFieldInfo = procedure->indexRepository->getRelationHelper()->getField(status, att, tra, sqlDialect, ftsIndex.relationName, keyFieldName);

            if (fields.size() == 1) {
                QueryParserPtr parser = newLucene<QueryParser>(LuceneVersion::LUCENE_CURRENT, fields[0], analyzer);
                query = parser->parse(StringUtils::toUnicode(queryStr));
            }
            else {
                MultiFieldQueryParserPtr  parser = newLucene<MultiFieldQueryParser>(LuceneVersion::LUCENE_CURRENT, fields, analyzer);
                parser->setDefaultOperator(QueryParser::OR_OPERATOR);
                query = parser->parse(StringUtils::toUnicode(queryStr));
            }
            docs = searcher->search(query, limit);

            it = docs->scoreDocs.begin();

            out->relationNameNull = false;
            out->relationName.length = static_cast<ISC_USHORT>(ftsIndex.relationName.length());
            ftsIndex.relationName.copy(out->relationName.str, out->relationName.length);

            out->keyFieldNameNull = false;
            out->keyFieldName.length = static_cast<ISC_USHORT>(keyFieldName.length());
            keyFieldName.copy(out->keyFieldName.str, out->keyFieldName.length);

            out->dbKeyNull = true;
            out->uuidNull = true;
            out->idNull = true;
            out->scoreNull = true;
        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
    }

    bool explainFlag = false;
    AutoRelease<IAttachment> att{ nullptr };
    AutoRelease<ITransaction> tra{ nullptr };
    RelationFieldInfo keyFieldInfo;
    String unicodeKeyFieldName = L"";
    SearcherPtr searcher{ nullptr };
    QueryPtr query{ nullptr };	
    TopDocsPtr docs{ nullptr };
    Collection<ScoreDocPtr>::iterator it;

    FB_UDR_FETCH_PROCEDURE
    {
        try {
            if (it == docs->scoreDocs.end()) {
                return false;
            }
            ScoreDocPtr scoreDoc = *it;
            DocumentPtr doc = searcher->doc(scoreDoc->doc);

            try {
                const std::string keyValue = StringUtils::toUTF8(doc->get(unicodeKeyFieldName));
                if (unicodeKeyFieldName == L"RDB$DB_KEY") {
                    // In the Lucene index, the string is stored in hexadecimal form, so let's convert it back to binary format.
                    auto dbKey = hex_to_binary(keyValue);
                    auto dbKeyPtr = reinterpret_cast<char*>(dbKey.data());
                    out->dbKeyNull = false;
                    out->dbKey.length = static_cast<ISC_USHORT>(dbKey.size());
                    std::copy(dbKeyPtr, dbKeyPtr + out->dbKey.length, out->dbKey.str);
                }
                else if (keyFieldInfo.isBinary()) {
                    // In the Lucene index, the string is stored in hexadecimal form, so let's convert it back to binary format.
                    auto uuid = hex_to_binary(keyValue);
                    auto uuidPtr = reinterpret_cast<char*>(uuid.data());
                    out->uuidNull = false;
                    out->uuid.length = static_cast<ISC_USHORT>(uuid.size());
                    std::copy(uuidPtr, uuidPtr + out->uuid.length, out->uuid.str);
                }
                else if (keyFieldInfo.isInt()) {
                    out->idNull = false;
                    out->id = std::stoll(keyValue);
                }
            }
            catch (const std::invalid_argument& e) {
                throwException(status, e.what());
            }

            out->scoreNull = false;
            out->score = scoreDoc->score;

            if (explainFlag) {    
                auto explanation = searcher->explain(query, scoreDoc->doc);
                const std::string explanationStr = StringUtils::toUTF8(explanation->toString());
                out->explanationNull = false;
                writeStringToBlob(status, att, tra, &out->explanation, explanationStr);
            }
            else {
                out->explanationNull = true;
            }

            ++it;
        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
        return true;
    }
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$ANALYZE (
    FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
    FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD'
)
RETURNS (
    FTS$TERM VARCHAR(8191) CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!ftsAnalyze'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsAnalyze)
    FB_UDR_MESSAGE(InMessage,
        (FB_BLOB, text)
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
    );

    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(32765, CS_UTF8), term)
    );

    FB_UDR_CONSTRUCTOR
        , analyzers(std::make_unique<AnalyzerRepository>(context->getMaster()))
    {
    }

    std::unique_ptr<AnalyzerRepository> analyzers;

    void getCharSet([[maybe_unused]] ThrowStatusWrapper* status, [[maybe_unused]] IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {
        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        std::string analyzerName = DEFAULT_ANALYZER_NAME;
        if (!in->analyzerNameNull) {
            analyzerName.assign(in->analyzerName.str, in->analyzerName.length);
        }

        if (!in->textNull) {
            std::string text = readStringFromBlob(status, att, tra, &in->text);
            try {
                auto analyzer = procedure->analyzers->createAnalyzer(status, att, tra, sqlDialect, analyzerName);
                auto stringReader = newLucene<StringReader>(StringUtils::toUnicode(text));

                tokenStream = analyzer->tokenStream(L"", stringReader);
                termAttribute = tokenStream->addAttribute<TermAttribute>();
                tokenStream->reset();
            } catch (const LuceneException& e) {
                const std::string error_message = StringUtils::toUTF8(e.getError());
                throwException(status, error_message.c_str());
            }
        }
    }

    TokenStreamPtr tokenStream = nullptr;
    TermAttributePtr termAttribute = nullptr;

    FB_UDR_FETCH_PROCEDURE
    {
        if (!(tokenStream && tokenStream->incrementToken())) {
            return false;
        }
        const auto uTerm = termAttribute->term();

        if (uTerm.length() > 8191) {
            throwException(status, "Term size exceeds 8191 characters");
        }

        const std::string term = StringUtils::toUTF8(uTerm);

        out->termNull = false;
        out->term.length = static_cast<ISC_USHORT>(term.length());
        term.copy(out->term.str, out->term.length);

        return true;
    }

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$UPDATE_INDEXES 
EXTERNAL NAME 'luceneudr!updateFtsIndexes'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(updateFtsIndexes)

    FB_UDR_CONSTRUCTOR
        , indexRepository(std::make_unique<FTSIndexRepository>(context->getMaster()))
        , logDeleteStmt(nullptr)
        , logStmt(nullptr)
    {
    }


    FTSIndexRepositoryPtr indexRepository{nullptr};
    AutoRelease<IStatement> logDeleteStmt{nullptr};
    AutoRelease<IStatement> logStmt{nullptr};

    void getCharSet([[maybe_unused]] ThrowStatusWrapper* status, [[maybe_unused]] IExternalContext* context,
        char* name, unsigned nameSize) 
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {
        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        constexpr const char* SQL_DELETE_FTS_LOG = R"SQL(
DELETE FROM FTS$LOG
WHERE FTS$LOG_ID = ?
)SQL";

        constexpr const char* SQL_SELECT_FTS_LOG = R"SQL(
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

        const auto ftsDirectoryPath = getFtsDirectory(status, context);
        
        std::unordered_map<std::string, FTSPreparedIndexStmt> ftsPreparedIndexesStmt;
        // get all indexes with segments
        auto indexes = procedure->indexRepository->allIndexes(status, att, tra, sqlDialect, true);
        // fill map indexes of relationName
        std::unordered_map<std::string, std::list<std::string>> indexesByRelation;
        for (auto&& ftsIndex : indexes) {	
            if (!ftsIndex.isActive()) {
                continue;
            }
            const auto indexDirectoryPath = ftsDirectoryPath / ftsIndex.indexName;
            if (!ftsIndex.checkAllFieldsExists() || !fs::is_directory(indexDirectoryPath)) {
                // index need to rebuild
                setIndexToRebuild(status, att, sqlDialect, ftsIndex);
                // go to next index
                continue;
            }
            ftsIndex.unicodeIndexDir = indexDirectoryPath.wstring();

            // collect and save the SQL query to extract the record by key
            const std::string sql = ftsIndex.buildSqlSelectFieldValues(status, sqlDialect, true);
            auto&& [itIndexStmt, inserted] = ftsPreparedIndexesStmt.try_emplace(
                ftsIndex.indexName,
                status,
                att,
                tra,
                sqlDialect,
                sql
            );
            auto&& indexStmt = itIndexStmt->second;

            auto inMetadata = indexStmt.getInExtractRecordMetadata();
            if (inMetadata->getCount(status) != 1) {
                // The number of input parameters must be equal to 1.
                // index need to rebuild
                setIndexToRebuild(status, att, sqlDialect, ftsIndex);
                // go to next index
                continue;
            }
            auto params = makeFbFieldsInfo(status, inMetadata);
            const auto& keyParam = params.at(0);
            if (keyParam.isBinary()) {
                switch (keyParam.length) {
                case 8:
                    ftsIndex.keyFieldType = FTSKeyType::DB_KEY;
                    break;
                case 16:
                    ftsIndex.keyFieldType = FTSKeyType::UUID;
                    break;
                default:
                    // index need to rebuild
                    setIndexToRebuild(status, att, sqlDialect, ftsIndex);
                    // go to next index
                    continue;
                }
            }
            else if (keyParam.isInt()) {
                ftsIndex.keyFieldType = FTSKeyType::INT_ID;
            }
            else {
                // index need to rebuild
                setIndexToRebuild(status, att, sqlDialect, ftsIndex);
                // go to next index
                continue;
            }


            auto outMetadata = indexStmt.getOutExtractRecordMetadata();
            // add fields info for index
            auto fields = makeFbFieldsInfo(status, outMetadata);
            for (auto& field : fields) {
                auto iSegment = ftsIndex.findSegment(field.fieldName);
                if (iSegment == ftsIndex.segments.end()) {
                    // index need to rebuild
                    setIndexToRebuild(status, att, sqlDialect, ftsIndex);
                    // go to next index
                    continue;
                }
                auto const& segment = *iSegment;
                field.ftsFieldName = StringUtils::toUnicode(segment.fieldName());
                field.ftsKey = segment.isKey();
                field.ftsBoost = segment.boost();
                field.ftsBoostNull = segment.isBoostNull();
                if (field.ftsKey) {
                    ftsIndex.unicodeKeyFieldName = field.ftsFieldName;
                }
            }
            fieldsInfoMap.try_emplace(ftsIndex.indexName, std::move(fields));

            // Add FTS indexName for relation
            auto& indexNames = indexesByRelation[ftsIndex.relationName];
            indexNames.push_back(ftsIndex.indexName);
        }

        try 
        {
            DBKEYInput dbKeyInput(status, context->getMaster());
            UUIDInput uuidInput(status, context->getMaster());
            IDInput idInput(status, context->getMaster());


            // prepare statement for delete record from FTS log
            if (!procedure->logDeleteStmt.hasData()) {
                procedure->logDeleteStmt.reset(att->prepare(
                    status,
                    tra,
                    0,
                    SQL_DELETE_FTS_LOG,
                    sqlDialect,
                    IStatement::PREPARE_PREFETCH_METADATA
                ));
            }

            LogDelInput logDelInput(status, context->getMaster());

            // prepare statement for retrieval record from FTS log 
            if (!procedure->logStmt.hasData()) {
                procedure->logStmt.reset(att->prepare(
                    status,
                    tra,
                    0,
                    SQL_SELECT_FTS_LOG,
                    sqlDialect,
                    IStatement::PREPARE_PREFETCH_METADATA
                ));
            }

            LogOutput logOutput(status, context->getMaster());


            AutoRelease<IResultSet> logRs (procedure->logStmt->openCursor(
                status,
                tra,
                nullptr,
                nullptr,
                logOutput.getMetadata(),
                0
            ));


            while (logRs->fetchNext(status, logOutput.getData()) == IStatus::RESULT_OK) {
                const ISC_INT64 logId = logOutput->id;
                const std::string relationName(logOutput->relationName.str, logOutput->relationName.length);
                const std::string changeType(logOutput->changeType.str, logOutput->changeType.length);


                const auto itIndexNames = indexesByRelation.find(relationName);
                if (itIndexNames == indexesByRelation.end()) {
                    continue;
                }
                const auto& indexNames = itIndexNames->second;

                // for all indexes for relationName
                for (const auto& indexName : indexNames) {
                    const auto& ftsIndex = indexes[indexName];
                    auto indexWriter = getIndexWriter(status, att, tra, sqlDialect, ftsIndex);

                    Lucene::String unicodeKeyValue;
                    switch (ftsIndex->keyFieldType) {
                    case FTSKeyType::DB_KEY:
                        if (!logOutput->dbKeyNull) {
                            std::string dbKey = binary_to_hex(reinterpret_cast<unsigned char*>(logOutput->dbKey.str), logOutput->dbKey.length);
                            unicodeKeyValue = StringUtils::toUnicode(dbKey);
                        }
                        break;
                    case FTSKeyType::UUID:
                        if (!logOutput->uuidNull) {
                            std::string uuid = binary_to_hex(reinterpret_cast<unsigned char*>(logOutput->uuid.str), logOutput->uuid.length);
                            unicodeKeyValue = StringUtils::toUnicode(uuid);
                        }
                        break;
                    case FTSKeyType::INT_ID:
                        if (!logOutput->recIdNull) {
                            std::string id = std::to_string(logOutput->recId);
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

                    auto&& ftsIndexStmt = ftsPreparedIndexesStmt[indexName];

                    auto stmt = ftsIndexStmt.getPreparedExtractRecordStmt();
                    auto outMetadata = ftsIndexStmt.getOutExtractRecordMetadata();

                    const auto& fields = fieldsInfoMap[indexName];

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

                    if (!rs) {
                        continue;
                    }

                    //const unsigned colCount = outMetadata->getCount(status);
                    const unsigned msgLength = outMetadata->getMessageLength(status);
                    {
                        // allocate output buffer
                        std::vector<unsigned char> buffer(msgLength, 0);
                        while (rs->fetchNext(status, buffer.data()) == IStatus::RESULT_OK) {
                            bool emptyFlag = true;
                            auto doc = newLucene<Document>();

                            for (const auto& field : fields) {
                                const std::string value = field.getStringValue(status, att, tra, buffer.data());

                                Lucene::String unicodeValue = StringUtils::toUnicode(value);

                                // add field to document
                                if (field.ftsKey) {
                                    auto luceneField = newLucene<Field>(field.ftsFieldName, unicodeValue, Field::STORE_YES, Field::INDEX_NOT_ANALYZED);
                                    doc->add(luceneField);
                                }
                                else {
                                    auto luceneField = newLucene<Field>(field.ftsFieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED);
                                    if (!field.ftsBoostNull) {
                                        luceneField->setBoost(field.ftsBoost);
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
                            std::fill(buffer.begin(), buffer.end(), 0);
                        }
                        rs->close(status);
                        rs.release();
                    }
                }
                // delete record from FTS log
                logDelInput->idNull = false;
                logDelInput->id = logId;
                procedure->logDeleteStmt->execute(
                    status,
                    tra,
                    logDelInput.getMetadata(),
                    logDelInput.getData(),
                    nullptr,
                    nullptr
                );
                
            }
            logRs->close(status);
            logRs.release();
            // commit changes for all indexes
            for (auto&& pIndexWriter : indexWriters) {
                auto indexWriter = pIndexWriter.second;
                indexWriter->commit();
                indexWriter->close();
            }
        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
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

    // FTS log output message
    FB_MESSAGE(LogOutput, ThrowStatusWrapper,
        (FB_BIGINT, id)
        (FB_INTL_VARCHAR(252, CS_UTF8), relationName)
        (FB_VARCHAR(8), dbKey)
        (FB_VARCHAR(16), uuid)
        (FB_BIGINT, recId)
        (FB_INTL_VARCHAR(4, CS_UTF8), changeType)
    );

    std::unordered_map<std::string, FbFieldsInfo> fieldsInfoMap;
    std::map<std::string, IndexWriterPtr> indexWriters;

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }


    void setIndexToRebuild(ThrowStatusWrapper* status, IAttachment* att, unsigned int sqlDialect, FTSIndex& ftsIndex)
    {
        ftsIndex.status = "U";
        // this is done in an autonomous transaction				
        AutoRelease<ITransaction> tra(att->startTransaction(status, 0, nullptr));
        try {
            procedure->indexRepository->setIndexStatus(status, att, tra, sqlDialect, ftsIndex.indexName, "U");
            tra->commit(status);
            tra.release();
        }
        catch (...) {
            tra->rollback(status);
            tra.release();
        }
    }

    IndexWriterPtr getIndexWriter(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned int sqlDialect, const FTSIndex& ftsIndex)
    {
        const auto it = indexWriters.find(ftsIndex.indexName);
        if (it == indexWriters.end()) {
            auto fsIndexDir = FSDirectory::open(ftsIndex.unicodeIndexDir);
            const auto analyzers = procedure->indexRepository->getAnalyzerRepository();
            auto analyzer = analyzers->createAnalyzer(status, att, tra, sqlDialect, ftsIndex.analyzer);
            auto indexWriter = newLucene<IndexWriter>(fsIndexDir, analyzer, IndexWriter::MaxFieldLengthUNLIMITED);
            indexWriters[ftsIndex.indexName] = indexWriter;
            return indexWriter;
        }
        else {
            return it->second;
        }
    }

FB_UDR_END_PROCEDURE

FB_UDR_IMPLEMENT_ENTRY_POINT
