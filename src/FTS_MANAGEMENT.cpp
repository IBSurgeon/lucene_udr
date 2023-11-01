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
#include "EncodeUtils.h"
#include "FBFieldInfo.h"
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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_FUNCTION
    {
        const auto& ftsDirectoryPath = getFtsDirectory(status, context);
        const std::string ftsDirectory = ftsDirectoryPath.u8string();

        out->directoryNull = false;
        out->directory.length = static_cast<ISC_USHORT>(ftsDirectory.length());
        ftsDirectory.copy(out->directory.str, out->directory.length);
    }
FB_UDR_END_FUNCTION


/***
PROCEDURE FTS$ANALYZERS
RETURNS (
  FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
  FTS$BASE_ANALYZER VARCHAR(63) CHARACTER SET UTF8,
  FTS$STOP_WORDS_SUPPORTED BOOLEAN,
  FTS$SYSTEM_FLAG BOOLEAN
)
EXTERNAL NAME 'luceneudr!getAnalyzers' 
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getAnalyzers)

    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
        (FB_INTL_VARCHAR(252, CS_UTF8), baseAnalyzer)
        (FB_BOOLEAN, stopWordsSupported)
        (FB_BOOLEAN, systemFlag)
    );

    FB_UDR_CONSTRUCTOR
        , analyzers(std::make_unique<AnalyzerRepository>(context->getMaster()))
    {
    }

    std::unique_ptr<AnalyzerRepository> analyzers;

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
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

        analyzerInfos = procedure->analyzers->getAnalyzerInfos(status, att, tra, sqlDialect);
        it = analyzerInfos.begin();
    }

    std::list<AnalyzerInfo> analyzerInfos;
    std::list<AnalyzerInfo>::const_iterator it;

    FB_UDR_FETCH_PROCEDURE
    {
        if (it == analyzerInfos.end()) {
            return false;
        }
        const AnalyzerInfo info = *it;

        out->analyzerNull = false;
        out->analyzer.length = static_cast<ISC_USHORT>(info.analyzerName.length());
        info.analyzerName.copy(out->analyzer.str, out->analyzer.length);

        out->baseAnalyzerNull = (info.baseAnalyzer.length() == 0);
        out->baseAnalyzer.length = static_cast<ISC_USHORT>(info.baseAnalyzer.length());
        info.baseAnalyzer.copy(out->baseAnalyzer.str, out->baseAnalyzer.length);

        out->stopWordsSupportedNull = false;
        out->stopWordsSupported = static_cast<FB_BOOLEAN>(info.stopWordsSupported);

        out->systemFlagNull = false;
        out->systemFlag = static_cast<FB_BOOLEAN>(info.systemFlag);

        ++it;
        return true;
    }
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$CREATE_ANALYZER (
    FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$BASE_ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
    FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!createAnalyzer'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(createAnalyzer)

    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
        (FB_INTL_VARCHAR(252, CS_UTF8), baseAnalyzer)
        (FB_BLOB, description)
    );

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {

        if (in->analyzerNameNull) {
            throwException(status, "Analyzer name can not be NULL");
        }
        const std::string analyzerName(in->analyzerName.str, in->analyzerName.length);

        if (in->baseAnalyzerNull) {
            throwException(status, "Base analyzer name can not be NULL");
        }
        const std::string baseAnalyzer(in->baseAnalyzer.str, in->baseAnalyzer.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        const auto& analyzers = std::make_unique<AnalyzerRepository>(context->getMaster());

        std::string description;
        if (!in->descriptionNull) {
            AutoRelease<IBlob> blob(att->openBlob(status, tra, &in->description, 0, nullptr));
            description = BlobUtils::getString(status, blob);
            blob->close(status);
            blob.release();
        }

        analyzers->addAnalyzer(status, att, tra, sqlDialect, analyzerName, baseAnalyzer, description);
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$DROP_ANALYZER (
    FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!dropAnalyzer'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(dropAnalyzer)

    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
    );

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {

        if (in->analyzerNameNull) {
            throwException(status, "Analyzer name can not be NULL");
        }
        const std::string analyzerName(in->analyzerName.str, in->analyzerName.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        const auto indexRepository = std::make_unique<FTSIndexRepository>(context->getMaster());

        const auto& analyzers = indexRepository->getAnalyzerRepository();

        if (!indexRepository->hasIndexByAnalyzer(status, att, tra, sqlDialect, analyzerName)) {
            analyzers->deleteAnalyzer(status, att, tra, sqlDialect, analyzerName);
        }
        else {
            throwException(status, R"(Unable to drop analyzer, there are dependent indexes.)");
        }
    }

    FB_UDR_FETCH_PROCEDURE
    {
        return false;
    }

FB_UDR_END_PROCEDURE

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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {
        if (in->analyzerNull) {
            throwException(status, "Analyzer can not be NULL");
        }
        const std::string analyzerName(in->analyzer.str, in->analyzer.length);

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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {

        if (in->analyzerNameNull) {
            throwException(status, "Analyzer name can not be NULL");
        }
        const std::string analyzerName(in->analyzerName.str, in->analyzerName.length);

        if (in->stopWord.length == 0) {
            in->stopWordNull = true;
        }

        if (in->stopWordNull) {
            throwException(status, "Stop word can not be NULL");
        }
        const std::string stopWord(in->stopWord.str, in->stopWord.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        const auto indexRepository = std::make_unique<FTSIndexRepository>(context->getMaster());

        const auto& analyzers = indexRepository->getAnalyzerRepository();

        analyzers->addStopWord(status, att, tra, sqlDialect, analyzerName, trim(stopWord));

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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
        char* name, unsigned nameSize)
    {
        // Forced internal request encoding to UTF8
        memset(name, 0, nameSize);
        memcpy(name, INTERNAL_UDR_CHARSET, std::size(INTERNAL_UDR_CHARSET));
    }

    FB_UDR_EXECUTE_PROCEDURE
    {

        if (in->analyzerNameNull) {
            throwException(status, "Analyzer name can not be NULL");
        }
        const std::string analyzerName(in->analyzerName.str, in->analyzerName.length);

        if (in->stopWord.length == 0) {
            in->stopWordNull = true;
        }

        if (in->stopWordNull) {
            throwException(status, "Stop word can not be NULL");
        }
        const std::string stopWord(in->stopWord.str, in->stopWord.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        const auto indexRepository = std::make_unique<FTSIndexRepository>(context->getMaster());

        const auto& analyzers = indexRepository->getAnalyzerRepository();

        analyzers->deleteStopWord(status, att, tra, sqlDialect, analyzerName, trim(stopWord));

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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
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
        const std::string indexName(in->indexName.str, in->indexName.length);

        if (in->relationNameNull) {
            throwException(status, "Relation name can not be NULL");
        }
        const std::string relationName(in->relationName.str, in->relationName.length);

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


        std::string description;
        if (!in->descriptionNull) {
            AutoRelease<IBlob> blob(att->openBlob(status, tra, &in->description, 0, nullptr));
            description = BlobUtils::getString(status, blob);
            blob->close(status);
            blob.release();
        }

        const auto& relationHelper = procedure->indexRepository->getRelationHelper();
        auto relationInfo = std::make_unique<RelationInfo>();
        relationHelper->getRelationInfo(status, att, tra, sqlDialect, relationInfo, relationName);


        procedure->indexRepository->createIndex(status, att, tra, sqlDialect, indexName, relationName, analyzerName, description);

        std::string keyFieldName;
        if (in->keyFieldNameNull) {
            if (relationInfo->findKeyFieldSupported()) {
                RelationFieldList keyFields;
                relationHelper->fillPrimaryKeyFields(status, att, tra, sqlDialect, relationName, keyFields);
                if (keyFields.size() == 0) {
                   // There is no primary key constraint.
                    if (relationInfo->relationType == RelationType::RT_REGULAR) {
                        keyFieldName = "RDB$DB_KEY";
                    }
                    else {
                        throwException(status, "The key field is not specified.");
                    }
                }
                else if (keyFields.size() == 1) {
                    // OK
                    const auto& keyFieldInfo = *keyFields.cbegin();
                    keyFieldName = keyFieldInfo->fieldName;
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
            if (relationInfo->relationType != RelationType::RT_REGULAR) {
                throwException(status, R"(Using "RDB$DB_KEY" as a key is supported only for regular tables.)");
            }
        }
        else {
            auto keyFieldInfo = std::make_unique<RelationFieldInfo>();
            relationHelper->getField(status, att, tra, sqlDialect, keyFieldInfo, relationName, keyFieldName);
            // check field type
            // Supported types SMALLINT, INTEGER, BIGINT, CHAR(16) CHARACTER SET OCTETS, BINARY(16) 
            if (!(keyFieldInfo->isInt() || (keyFieldInfo->isFixedChar() && keyFieldInfo->isBinary() && keyFieldInfo->fieldLength == 16))) {
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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
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
        const std::string indexName(in->index_name.str, in->index_name.length);

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        procedure->indexRepository->dropIndex(status, att, tra, sqlDialect, indexName);

        const auto& ftsDirectoryPath = getFtsDirectory(status, context);
        const auto& indexDirectoryPath = ftsDirectoryPath / indexName;
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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
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
        const std::string indexName(in->index_name.str, in->index_name.length);
        const bool indexActive = in->index_active;

        AutoRelease<IAttachment> att(context->getAttachment(status));
        AutoRelease<ITransaction> tra(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        auto ftsIndex = std::make_unique<FTSIndex>();
        procedure->indexRepository->getIndex(status, att, tra, sqlDialect, ftsIndex, indexName);
        if (indexActive) {
            // index is inactive
            if (ftsIndex->status == "I") {
                // index is active but needs to be rebuilt
                procedure->indexRepository->setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
            }
        }
        else {
            // index is active
            if (ftsIndex->isActive()) {
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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
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
        const std::string indexName(in->index_name.str, in->index_name.length);


        if (in->field_nameNull) {
            throwException(status, "Field name can not be NULL");
        }
        const std::string fieldName(in->field_name.str, in->field_name.length);

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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
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
        const std::string indexName(in->index_name.str, in->index_name.length);

        if (in->field_nameNull) {
            throwException(status, "Field name can not be NULL");
        }
        const std::string fieldName(in->field_name.str, in->field_name.length);

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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
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
        const std::string indexName(in->indexName.str, in->indexName.length);


        if (in->fieldNameNull) {
            throwException(status, "Field name can not be NULL");
        }
        const std::string fieldName(in->fieldName.str, in->fieldName.length);

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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
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

        if (in->index_nameNull) {
            throwException(status, "Index name can not be NULL");
        }
        const std::string indexName(in->index_name.str, in->index_name.length);

        const auto& ftsDirectoryPath = getFtsDirectory(status, context);
        // check if there is a directory for full-text indexes
        if (!fs::is_directory(ftsDirectoryPath)) {
            throwException(status, R"(Fts directory "%s" not exists)", ftsDirectoryPath.u8string().c_str());
        }

        const unsigned int sqlDialect = getSqlDialect(status, att);

        try {
            // check for index existence
            auto ftsIndex = std::make_unique<FTSIndex>();
            procedure->indexRepository->getIndex(status, att, tra, sqlDialect, ftsIndex, indexName, true);
            // Check if the index directory exists, and if it doesn't exist, create it. 
            const auto indexDirectoryPath = ftsDirectoryPath / indexName;
            if (!createIndexDirectory(indexDirectoryPath)) {
                throwException(status, R"(Cannot create index directory "%s".)", indexDirectoryPath.u8string().c_str());
            }

            // check relation exists
            const auto& relationHelper = procedure->indexRepository->getRelationHelper();
            if (!relationHelper->relationExists(status, att, tra, sqlDialect, ftsIndex->relationName)) {
                throwException(status, R"(Cannot rebuild index "%s". Table "%s" not exists.)", indexName.c_str(), ftsIndex->relationName.c_str());
            }

            // check segments exists
            if (ftsIndex->segments.size() == 0) {
                throwException(status, R"(Cannot rebuild index "%s". The index does not contain fields.)", indexName.c_str());
            }

            const auto& analyzers = procedure->indexRepository->getAnalyzerRepository();

            auto fsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
            auto analyzer = analyzers->createAnalyzer(status, att, tra, sqlDialect, ftsIndex->analyzer);
            auto writer = newLucene<IndexWriter>(fsIndexDir, analyzer, true, IndexWriter::MaxFieldLengthLIMITED);

            // clean up index directory
            writer->deleteAll();
            writer->commit();

            for (const auto& segment : ftsIndex->segments) {
                if (segment->fieldName != "RDB$DB_KEY") {
                    if (!segment->fieldExists) {
                        throwException(status, R"(Cannot rebuild index "%s". Field "%s" not exists in relation "%s".)", indexName.c_str(), segment->fieldName.c_str(), ftsIndex->relationName.c_str());
                    }
                }
            }
                
            const std::string sql = ftsIndex->buildSqlSelectFieldValues(status, sqlDialect);

            AutoRelease<IStatement> stmt(att->prepare(
                status,
                tra,
                0,
                sql.c_str(),
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
            AutoRelease<IMessageMetadata> outputMetadata(stmt->getOutputMetadata(status));
            // make all fields of string type except BLOB
            AutoRelease<IMessageMetadata> newMeta(prepareTextMetaData(status, outputMetadata));
            FbFieldsInfo fields(status, newMeta);

            // initial specific FTS property for fields
            for (unsigned int i = 0; i < fields.size(); i++) {
                auto&& field = fields[i];
                auto iSegment = ftsIndex->findSegment(field.fieldName);
                if (iSegment == ftsIndex->segments.end()) {
                    throwException(status, R"(Cannot rebuild index "%s". Field "%s" not found.)", indexName.c_str(), field.fieldName.c_str());
                }
                auto&& segment = *iSegment;
                field.ftsFieldName = StringUtils::toUnicode(segment->fieldName);
                field.ftsKey = segment->key;
                field.ftsBoost = segment->boost;
                field.ftsBoostNull = segment->boostNull;
            }

            AutoRelease<IResultSet> rs(stmt->openCursor(
                status,
                tra,
                nullptr,
                nullptr,
                newMeta,
                0
            ));
                
            const unsigned colCount = newMeta->getCount(status);
            const unsigned msgLength = newMeta->getMessageLength(status);
            {
                // allocate output buffer
                std::vector<unsigned char> buffer(msgLength, 0);
                while (rs->fetchNext(status, buffer.data()) == IStatus::RESULT_OK) {						
                    bool emptyFlag = true;
                    auto doc = newLucene<Document>();
                        
                    for (unsigned int i = 0; i < colCount; i++) {
                        const auto& field = fields[i];

                        Lucene::String unicodeValue;	
                        if (!field.isNull(buffer.data())) {
                            const std::string value = field.getStringValue(status, att, tra, buffer.data());
                            if (!value.empty()) {
                                // re-encode content to Unicode only if the string is non-binary
                                if (!field.isBinary()) {
                                    unicodeValue = StringUtils::toUnicode(value);
                                }
                                else {
                                    // convert the binary string to a hexadecimal representation
                                    unicodeValue = StringUtils::toUnicode(string_to_hex(value));
                                }
                            }
                        }
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
                    // if all indexed fields are empty, then it makes no sense to add the document to the index
                    if (!emptyFlag) {
                        writer->addDocument(doc);
                    }
                    std::fill(buffer.begin(), buffer.end(), 0);
                }
                rs->close(status);
                rs.release();
            }
            writer->commit();

            writer->optimize();
            writer->commit();
            writer->close();

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

    void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
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

        if (in->index_nameNull) {
            throwException(status, "Index name can not be NULL");
        }
        const std::string indexName(in->index_name.str, in->index_name.length);

        const auto& ftsDirectoryPath = getFtsDirectory(status, context);	
        // check if there is a directory for full-text indexes
        if (!fs::is_directory(ftsDirectoryPath)) {
            throwException(status, R"(Fts directory "%s" not exists)", ftsDirectoryPath.u8string().c_str());
        }

        const unsigned int sqlDialect = getSqlDialect(status, att);

        try {
            // check for index existence
            auto ftsIndex = std::make_unique<FTSIndex>();
            procedure->indexRepository->getIndex(status, att, tra, sqlDialect, ftsIndex, indexName);
            // Check if the index directory exists. 
            const auto& indexDirectoryPath = ftsDirectoryPath / indexName;
            if (!fs::is_directory(indexDirectoryPath)) {
                throwException(status, R"(Index directory "%s" not exists.)", indexDirectoryPath.u8string().c_str());
            }

            const auto& analyzers = procedure->indexRepository->getAnalyzerRepository();

            const auto& fsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
            const auto& analyzer = analyzers->createAnalyzer(status, att, tra, sqlDialect, ftsIndex->analyzer);
            const auto& writer = newLucene<IndexWriter>(fsIndexDir, analyzer, false, IndexWriter::MaxFieldLengthLIMITED);

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
