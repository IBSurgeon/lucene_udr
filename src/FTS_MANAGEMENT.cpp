/**
 *  Implementation of procedures and functions of the FTS$MANAGEMENT package.
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
#include "FtsHelper.h"
#include "LuceneHeaders.h"
#include "FileUtils.h"
#include "Analyzers.h"
#include "LuceneAnalyzerFactory.h"
#include "Utils.h"
#include <sstream>
#include <memory>
#include <algorithm>
#include <filesystem> 

namespace fs = std::filesystem;

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;
using namespace FTSMetadata;

/***
FUNCTION FTS$GET_DIRECTORY ()
RETURNS VARCHAR(255) CHARACTER SET UTF8
DETERMINISTIC
EXTERNAL NAME 'luceneudr!getFTSDirectory'
ENGINE UDR;
***/
FB_UDR_BEGIN_FUNCTION(getFTSDirectory)
    FB_UDR_MESSAGE(OutMessage,
       (FB_INTL_VARCHAR(1020, CS_UTF8), directory)
    );

    void getCharSet([[maybe_unused]] ThrowStatusWrapper* status, [[maybe_unused]] IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_FUNCTION
    {
        const auto ftsDirectoryPath = getFtsDirectory(status, context);
        const std::string ftsDirectory = ftsDirectoryPath.u8string();

        out->directoryNull = false;
        out->directory.length = static_cast<ISC_USHORT>(ftsDirectory.length());
        ftsDirectory.copy(out->directory.str, out->directory.length);
    }
FB_UDR_END_FUNCTION

/***
PROCEDURE FTS$SYSTEM_ANALYZERS
RETURNS (
  FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
  FTS$STOP_WORDS_SUPPORTED BOOLEAN
)
EXTERNAL NAME 'luceneudr!systemAnalyzers'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(systemAnalyzers)

    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
        (FB_BOOLEAN, stopWordsSupported)
    );

    FB_UDR_CONSTRUCTOR
        , analyzers(std::make_unique<LuceneAnalyzerFactory>())
    {
    }

    std::unique_ptr<LuceneAnalyzerFactory> analyzers;

    void getCharSet([[maybe_unused]] ThrowStatusWrapper* status, [[maybe_unused]] IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {
        analyzerInfos = procedure->analyzers->getAnalyzerInfos();
        it = analyzerInfos.begin();
    }

    std::list<AnalyzerInfo> analyzerInfos;
    std::list<AnalyzerInfo>::const_iterator it;

    FB_UDR_FETCH_PROCEDURE
    {
        if (it == analyzerInfos.end()) {
            return false;
        }
        auto&& info = *it;

        out->analyzerNull = false;
        out->analyzer.length = static_cast<ISC_USHORT>(info.analyzerName.length());
        info.analyzerName.copy(out->analyzer.str, out->analyzer.length);

        out->stopWordsSupportedNull = false;
        out->stopWordsSupported = static_cast<FB_BOOLEAN>(info.stopWordsSupported);

        ++it;
        return true;
    }
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$GET_SYSTEM_ANALYZER (
  FTS$ANALYZER_NAME VARCHAR(63) CHARACTER SET UTF8
)
RETURNS (
  FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
  FTS$STOP_WORDS_SUPPORTED BOOLEAN
)
EXTERNAL NAME 'luceneudr!getSystemAnalyzer'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getSystemAnalyzer)

    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
    );

    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
        (FB_BOOLEAN, stopWordsSupported)
    );

    FB_UDR_CONSTRUCTOR
        , analyzers(std::make_unique<LuceneAnalyzerFactory>())
    {
    }

    std::unique_ptr<LuceneAnalyzerFactory> analyzers;

    void getCharSet([[maybe_unused]] ThrowStatusWrapper* status, [[maybe_unused]] IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {
        if (!in->analyzerNameNull) {
            std::string_view analyzerName(in->analyzerName.str, in->analyzerName.length);

            auto info = procedure->analyzers->getAnalyzerInfo(status, analyzerName);

            fetchFlag = true;

            out->analyzerNull = false;
            out->analyzer.length = static_cast<ISC_USHORT>(info.analyzerName.length());
            info.analyzerName.copy(out->analyzer.str, out->analyzer.length);

            out->stopWordsSupportedNull = false;
            out->stopWordsSupported = static_cast<FB_BOOLEAN>(info.stopWordsSupported);
        }
        else {
            fetchFlag = false;
            out->analyzerNull = true;
            out->stopWordsSupportedNull = true;
        }
    }

    bool fetchFlag = false;

    FB_UDR_FETCH_PROCEDURE
    {
        if (!fetchFlag) {
            return false;
        }
        fetchFlag = false;

        return true;
    }
FB_UDR_END_PROCEDURE

/***
FUNCTION FTS$HAS_SYSTEM_ANALYZER (
  FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8
)
RETURNS BOOLEAN
EXTERNAL NAME 'luceneudr!hasSystemAnalyzer'
ENGINE UDR;
***/
FB_UDR_BEGIN_FUNCTION(hasSystemAnalyzer)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
    );

    FB_UDR_MESSAGE(OutMessage,
        (FB_BOOLEAN, exists)
    );

    FB_UDR_CONSTRUCTOR
        , analyzers(std::make_unique<LuceneAnalyzerFactory>())
    {
    }

    std::unique_ptr<LuceneAnalyzerFactory> analyzers;

    void getCharSet([[maybe_unused]] ThrowStatusWrapper* status, [[maybe_unused]] IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_FUNCTION
    {
        out->existsNull = false;
        if (!in->analyzerNull) {
            std::string_view analyzerName(in->analyzer.str, in->analyzer.length);
            out->exists = analyzers->hasAnalyzer(analyzerName);
        } 
        else {
            out->exists = false;
        }
    }
FB_UDR_END_FUNCTION


/***
PROCEDURE FTS$ANALYZER_STOP_WORDS (
    FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
    FTS$WORD VARCHAR(63) CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!getAnalyzerStopWords' 
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getAnalyzerStopWords)

    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
    );

    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), word)
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
        std::string_view analyzerName(in->analyzer.str, in->analyzer.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        stopWords = procedure->analyzers->getStopWords(status, att, tra, sqlDialect, analyzerName);
        it = stopWords.begin();
    }

    HashSet<String> stopWords;
    HashSet<String>::const_iterator it;

    FB_UDR_FETCH_PROCEDURE
    {
        if (it == stopWords.end()) {
            return false;
        }
        const String uStopWord = *it;
        const std::string stopWord = StringUtils::toUTF8(uStopWord);

        out->wordNull = false;
        out->word.length = static_cast<ISC_USHORT>(stopWord.length());
        stopWord.copy(out->word.str, out->word.length);

        ++it;
        return true;
    }
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$ADD_STOP_WORD (
    FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$WORD VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!addStopWord'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(addStopWord)

    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
        (FB_INTL_VARCHAR(252, CS_UTF8), stopWord)
    );

    void getCharSet([[maybe_unused]] ThrowStatusWrapper* status, [[maybe_unused]] IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {
        std::string_view analyzerName(in->analyzerName.str, in->analyzerName.length);

        if (in->stopWord.length == 0) {
            in->stopWordNull = true;
        }

        if (in->stopWordNull) {
            throwException(status, "Stop word can not be NULL");
        }
        std::string_view stopWord(in->stopWord.str, in->stopWord.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        auto indexRepository = std::make_unique<FTSIndexRepository>(context->getMaster());

        auto analyzers = indexRepository->getAnalyzerRepository();

        stopWord = trim(stopWord);
        analyzers->addStopWord(status, att, tra, sqlDialect, analyzerName, stopWord);

        //set dependent active index as rebuild
        auto dependentActiveIndexes = indexRepository->getActiveIndexByAnalyzer(status, att, tra, sqlDialect, analyzerName);
        for (const auto& indexName : dependentActiveIndexes) {
            indexRepository->setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
        }
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$DROP_STOP_WORD (
    FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$WORD VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!dropStopWord'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(dropStopWord)

    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
        (FB_INTL_VARCHAR(252, CS_UTF8), stopWord)
    );

    void getCharSet([[maybe_unused]] ThrowStatusWrapper* status, [[maybe_unused]] IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {

        std::string_view analyzerName(in->analyzerName.str, in->analyzerName.length);

        if (in->stopWord.length == 0) {
            in->stopWordNull = true;
        }

        if (in->stopWordNull) {
            throwException(status, "Stop word can not be NULL");
        }
        std::string_view stopWord(in->stopWord.str, in->stopWord.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        auto indexRepository = std::make_unique<FTSIndexRepository>(context->getMaster());

        auto analyzers = indexRepository->getAnalyzerRepository();

        stopWord = trim(stopWord);
        analyzers->deleteStopWord(status, att, tra, sqlDialect, analyzerName, stopWord);

        //set dependent active index as rebuild
        auto dependentActiveIndexes = indexRepository->getActiveIndexByAnalyzer(status, att, tra, sqlDialect, analyzerName);
        for (const auto& indexName : dependentActiveIndexes) {
            indexRepository->setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
        }
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$CREATE_INDEX (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
     FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
     FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
     FTS$KEY_FIELD_NAME VARCHAR(63) CHARACTER SET UTF8,
     FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!createIndex'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(createIndex)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), indexName)
        (FB_INTL_VARCHAR(252, CS_UTF8), relationName)
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
        (FB_INTL_VARCHAR(252, CS_UTF8), keyFieldName)
        (FB_BLOB, description)
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
        std::string_view indexName(in->indexName.str, in->indexName.length);

        std::string_view relationName(in->relationName.str, in->relationName.length);

        std::string analyzerName;
        if (!in->analyzerNull) {
            analyzerName.assign(in->analyzer.str, in->analyzer.length);
        }
        if (analyzerName.empty()) {
            analyzerName = DEFAULT_ANALYZER_NAME;
        }
        else {
            std::transform(analyzerName.begin(), analyzerName.end(), analyzerName.begin(), ::toupper);
        }

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        auto relationHelper = procedure->indexRepository->getRelationHelper();
        auto relationInfo = relationHelper->getRelationInfo(status, att, tra, sqlDialect, relationName);


        procedure->indexRepository->createIndex(status, att, tra, sqlDialect, indexName, relationName, analyzerName, !in->descriptionNull ? &in->description : nullptr);

        std::string keyFieldName;
        if (in->keyFieldNameNull) {
            if (relationInfo.findKeyFieldSupported()) {
                auto keyFields = relationHelper->fillPrimaryKeyFields(status, att, tra, sqlDialect, relationName);
                if (keyFields.size() == 0) {
                   // There is no primary key constraint.
                    if (relationInfo.relationType == RelationType::RT_REGULAR) {
                        keyFieldName = "RDB$DB_KEY";
                    }
                    else {
                        throwException(status, "The key field is not specified.");
                    }
                }
                else if (keyFields.size() == 1) {
                    // OK
                    const auto& keyFieldInfo = *keyFields.cbegin();
                    keyFieldName = keyFieldInfo.fieldName;
                }
                else {
                    throwException(status, 
                        "The primary key of the relation is composite. The FTS index does not support composite keys. " 
                        "Please specify the key field explicitly.");				
                }
            }
            else {
                throwException(status, "It is not possible to automatically determine the key field for this type of relation. Please specify this explicitly.");
            }
        }
        else {
            keyFieldName.assign(in->keyFieldName.str, in->keyFieldName.length);
        }

        if (keyFieldName == "RDB$DB_KEY") {
            if (relationInfo.relationType != RelationType::RT_REGULAR) {
                throwException(status, R"(Using "RDB$DB_KEY" as a key is supported only for regular tables.)");
            }
        }
        else {
            const auto keyFieldInfo = relationHelper->getField(status, att, tra, sqlDialect, relationName, keyFieldName);
            // check field type
            if (!keyFieldInfo.ftsKeySupported()) {
                throwException(status, "Unsupported data type for the key field. Supported data types: SMALLINT, INTEGER, BIGINT, CHAR(16) CHARACTER SET OCTETS, BINARY(16).");
            }
        }

        // Add index key field
        procedure->indexRepository->addIndexField(status, att, tra, sqlDialect, indexName, keyFieldName, true);

    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$DROP_INDEX (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!dropIndex'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(dropIndex)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
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
        std::string_view indexName(in->index_name.str, in->index_name.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        procedure->indexRepository->dropIndex(status, att, tra, sqlDialect, indexName);

        const auto ftsDirectoryPath = getFtsDirectory(status, context);
        const auto indexDirectoryPath = ftsDirectoryPath / indexName;
        // If the directory exists, then delete it.
        if (!removeIndexDirectory(indexDirectoryPath)) {
            throwException(status, R"(Cannot delete index directory "%s".)", indexDirectoryPath.u8string().c_str());
        }
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$SET_INDEX_ACTIVE (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
     FTS$INDEX_ACTIVE BOOLEAN NOT NULL
)
EXTERNAL NAME 'luceneudr!setIndexActive'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(setIndexActive)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
        (FB_BOOLEAN, index_active)
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
        if (in->index_nameNull) {
            throwException(status, "Index name can not be NULL");
        }
        std::string_view indexName(in->index_name.str, in->index_name.length);
        bool indexActive = in->index_active;

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        auto ftsIndex =  procedure->indexRepository->getIndex(status, att, tra, sqlDialect, indexName);
        if (indexActive) {
            // index is inactive
            if (ftsIndex.status == "I") {
                // index is active but needs to be rebuilt
                procedure->indexRepository->setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
            }
        }
        else {
            // index is active
            if (ftsIndex.isActive()) {
                // make inactive
                procedure->indexRepository->setIndexStatus(status, att, tra, sqlDialect, indexName, "I");
            }
        }
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$ADD_INDEX_FIELD (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
     FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
     FTS$BOOST DOUBLE PRECISION DEFAULT NULL
)
EXTERNAL NAME 'luceneudr!addIndexField'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(addIndexField)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
        (FB_INTL_VARCHAR(252, CS_UTF8), field_name)
        (FB_DOUBLE, boost)
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
        std::string_view indexName(in->index_name.str, in->index_name.length);
        std::string_view fieldName(in->field_name.str, in->field_name.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        // Adding a field to the index.
        procedure->indexRepository->addIndexField(status, att, tra, sqlDialect, indexName, fieldName, false, in->boost, in->boostNull);
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$DROP_INDEX_FIELD (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
     FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!dropIndexField'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(dropIndexField)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
        (FB_INTL_VARCHAR(252, CS_UTF8), field_name)
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
        std::string_view indexName(in->index_name.str, in->index_name.length);
        std::string_view fieldName(in->field_name.str, in->field_name.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        // Deleting a field from the index.
        procedure->indexRepository->dropIndexField(status, att, tra, sqlDialect, indexName, fieldName);
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$SET_INDEX_FIELD_BOOST (
     FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
     FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
     FTS$BOOST DOUBLE PRECISION
)
EXTERNAL NAME 'luceneudr!setIndexFieldBoost'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(setIndexFieldBoost)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), indexName)
        (FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
        (FB_DOUBLE, boost)
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
        std::string_view indexName(in->indexName.str, in->indexName.length);
        std::string_view fieldName(in->fieldName.str, in->fieldName.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        procedure->indexRepository->setIndexFieldBoost(status, att, tra, sqlDialect, indexName, fieldName, in->boost, in->boostNull);
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$REBUILD_INDEX (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!rebuildIndex'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(rebuildIndex)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
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
        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const std::string indexName(in->index_name.str, in->index_name.length);

        const auto ftsDirectoryPath = getFtsDirectory(status, context);
        // check if there is a directory for full-text indexes
        if (!fs::is_directory(ftsDirectoryPath)) {
            throwException(status, R"(Fts directory "%s" not exists)", ftsDirectoryPath.u8string().c_str());
        }

        const unsigned int sqlDialect = getSqlDialect(status, att);

        try {
            // get FTS index metadata
            auto ftsIndex = procedure->indexRepository->getIndex(status, att, tra, sqlDialect, indexName, true);
            // prepare index to rebuild
            auto preparedIndex = prepareFtsIndex(
                status, context->getMaster(), att, tra, sqlDialect, 
                std::move(ftsIndex), ftsDirectoryPath);

            preparedIndex.deleteAll(status);
            preparedIndex.commit(status);

            preparedIndex.rebuild(status, att, tra);
            preparedIndex.commit(status);

            preparedIndex.optimize(status); 
            preparedIndex.commit(status);
            preparedIndex.close(status);

            // if the index building was successful, then set the indexing completion status
            procedure->indexRepository->setIndexStatus(status, att, tra, sqlDialect, indexName, "C");
        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$OPTIMIZE_INDEX (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!optimizeIndex'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(optimizeIndex)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
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
        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const std::string indexName(in->index_name.str, in->index_name.length);

        const auto ftsDirectoryPath = getFtsDirectory(status, context);	
        // check if there is a directory for full-text indexes
        if (!fs::is_directory(ftsDirectoryPath)) {
            throwException(status, R"(Fts directory "%s" not exists)", ftsDirectoryPath.u8string().c_str());
        }

        const unsigned int sqlDialect = getSqlDialect(status, att);

        try {
            // get FTS index metadata
            auto ftsIndex = procedure->indexRepository->getIndex(status, att, tra, sqlDialect, indexName);
            // Check if the index directory exists. 
            const auto& indexDirectoryPath = ftsDirectoryPath / indexName;
            if (!fs::is_directory(indexDirectoryPath)) {
                throwException(status, R"(Index directory "%s" not exists.)", indexDirectoryPath.u8string().c_str());
            }

            const auto analyzers = procedure->indexRepository->getAnalyzerRepository();

            auto fsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
            auto analyzer = analyzers->createAnalyzer(status, att, tra, sqlDialect, ftsIndex.analyzer);
            auto writer = newLucene<IndexWriter>(fsIndexDir, analyzer, false, IndexWriter::MaxFieldLengthUNLIMITED);

            // clean up index directory
            writer->optimize();
            writer->close();
            fsIndexDir->close();
        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE
