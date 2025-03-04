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

#include <memory>
#include <string>
#include <unordered_map>

#include "Analyzers.h"
#include "FBUtils.h"
#include "FTSHelper.h"
#include "FTSIndex.h"
#include "FTSUtils.h"
#include "LuceneAnalyzerFactory.h"
#include "LuceneUdr.h"
#include "LuceneHeaders.h"
#include "Relations.h"
#include "TermAttribute.h"



using namespace Firebird;
using namespace Lucene;
using namespace FTSMetadata;
using namespace LuceneUDR;

namespace {

    constexpr std::string_view SPECIAL_CHARS = "+-!^\"~*?:\\&|()[]{}";

    std::string queryEscape(std::string_view query)
    {
        std::string s;
        s.reserve(query.size());

        auto p = query.find_first_of(SPECIAL_CHARS);
        while (p != std::string::npos) {
            s += query.substr(0, p);
            s += '\\';
            s += query.substr(p, 1);
            query.remove_prefix(p + 1);

            p = query.find_first_of(SPECIAL_CHARS);
        }
        s += query;

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
        , analyzerRepository(std::make_unique<AnalyzerRepository>(context->getMaster()))
        , relationHelper(std::make_unique<RelationHelper>(context->getMaster()))
    {
    }

    FTSIndexRepositoryPtr indexRepository;
    std::unique_ptr<AnalyzerRepository> analyzerRepository;
    RelationHelperPtr relationHelper;

    
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

            AnalyzerPtr analyzer = procedure->analyzerRepository->createAnalyzer(status, att, tra, sqlDialect, ftsIndex.analyzer);
            searcher = newLucene<IndexSearcher>(ftsIndexDir, true);
            
            std::string keyFieldName;
            auto fields = Collection<String>::newInstance();
            for (const auto& segment : ftsIndex.segments) {
                if (!segment.isKey()) {
                    fields.add(StringUtils::toUnicode(segment.fieldName()));
                }
                else {
                    keyFieldName = segment.fieldName();
                }
            }
            if (keyFieldName.empty()) {
                std::string sIndexName(indexName);
                throwException(status, R"(Not found key field in FTS index "%s".)", sIndexName.c_str());
            }
            unicodeKeyFieldName = StringUtils::toUnicode(keyFieldName);

            keyFieldInfo = procedure->relationHelper->getField(status, att, tra, sqlDialect, ftsIndex.relationName, keyFieldName);

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
    AutoRelease<IAttachment> att;
    AutoRelease<ITransaction> tra;
    RelationFieldInfo keyFieldInfo;
    String unicodeKeyFieldName;
    SearcherPtr searcher;
    QueryPtr query;
    TopDocsPtr docs;
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
                if (keyFieldInfo.isDbKey()) {
                    // In the Lucene index, the string is stored in hexadecimal form, so let's convert it back to binary format.
                    auto dbKey = hex_to_binary(keyValue);
                    std::string_view svDbKey(reinterpret_cast<char*>(dbKey.data()), dbKey.size());
                    out->dbKeyNull = false;
                    out->dbKey.length = static_cast<ISC_USHORT>(svDbKey.size());
                    svDbKey.copy(out->dbKey.str, out->dbKey.length);
                }
                else if (keyFieldInfo.isBinary()) {
                    // In the Lucene index, the string is stored in hexadecimal form, so let's convert it back to binary format.
                    auto uuid = hex_to_binary(keyValue);
                    std::string_view svUuid(reinterpret_cast<char*>(uuid.data()), uuid.size());
                    out->uuidNull = false;
                    out->uuid.length = static_cast<ISC_USHORT>(svUuid.size());
                    svUuid.copy(out->uuid.str, out->uuid.length);
                }
                else if (keyFieldInfo.isInt()) {
                    out->idNull = false;
                    out->id = std::stoll(keyValue);
                }
                else {
                    std::string sMessage = "FTS index does not know the key type.";
                    throwException(status, sMessage.c_str());
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

    TokenStreamPtr tokenStream;
    TermAttributePtr termAttribute;

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
        
        // fill map indexes of relationName
        std::unordered_map<std::string, std::list<FTSPreparedIndex>> indexesByRelation;
        {
            // get all indexes with segments
            auto indexes = procedure->indexRepository->allIndexes(status, att, tra, sqlDialect, true);

            for (auto&& ftsIndex : indexes) {
                if (!ftsIndex.isActive()) {
                    continue;
                }
                const std::string indexName = ftsIndex.indexName;
                auto&& [it, insert] = indexesByRelation.try_emplace(ftsIndex.relationName);
                auto& list = it->second;
                try {
                    auto preparedIndex = prepareFtsIndex(
                        status,
                        context->getMaster(),
                        att,
                        tra,
                        sqlDialect,
                        std::move(ftsIndex),
                        ftsDirectoryPath,
                        true);
                    list.push_back(std::move(preparedIndex));
                } catch (const FbException&) {
                    // if prepared error - set index to rebuild
                    setIndexToRebuild(status, att, sqlDialect, indexName);
                }
            }
        }

        try 
        {
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
                const std::string_view changeType(logOutput->changeType.str, logOutput->changeType.length);


                const auto itIndexes = indexesByRelation.find(relationName);
                if (itIndexes == indexesByRelation.end()) {
                    continue;
                }
                auto& preparedIndexes = itIndexes->second;

                // for all indexes for relationName
                for (auto& preparedIndex : preparedIndexes) {
                    switch (preparedIndex.keyType()) {
                    case FTSKeyType::DB_KEY:
                        if (!logOutput->dbKeyNull) {
                            preparedIndex.updateIndexByDbkey(
                                status,
                                att,
                                tra,
                                reinterpret_cast<unsigned char*>(logOutput->dbKey.str),
                                logOutput->dbKey.length,
                                changeType
                            );                                
                        }
                        break;
                    case FTSKeyType::UUID:
                        if (!logOutput->uuidNull) {
                            preparedIndex.updateIndexByUuui(
                                status,
                                att,
                                tra,
                                reinterpret_cast<unsigned char*>(logOutput->uuid.str),
                                logOutput->uuid.length,
                                changeType
                            );                            
                        }
                        break;
                    case FTSKeyType::INT_ID:
                        if (!logOutput->recIdNull) {
                            preparedIndex.updateIndexById(
                                status,
                                att,
                                tra,
                                logOutput->recId,
                                changeType
                            );
                        }
                        break;
                    default:
                        continue;
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
            for (auto&& [relationName, preparedIndexes] : indexesByRelation) {
                for (auto& preparedIndex : preparedIndexes) {
                    preparedIndex.optimize(status);
                    preparedIndex.commit(status);
                    preparedIndex.close(status);
                }
            }
        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
    }

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


    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }


    void setIndexToRebuild(ThrowStatusWrapper* status, IAttachment* att, unsigned int sqlDialect, const std::string& indexName)
    {
        // this is done in an autonomous transaction				
        AutoRelease<ITransaction> tra(att->startTransaction(status, 0, nullptr));
        try {
            procedure->indexRepository->setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
            tra->commit(status);
            tra.release();
        }
        catch (...) {
            tra->rollback(status);
            tra.release();
        }
    }

FB_UDR_END_PROCEDURE
