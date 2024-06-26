/**
 *  Implementation of procedures and functions of the FTS$STATISTICS package.
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
#include "FBUtils.h"
#include "LuceneFiles.h"
#include "LuceneHeaders.h"
#include "FileUtils.h"
#include "IndexFileNames.h"
#include "SegmentInfos.h"
#include "SegmentInfo.h"
#include "IndexFileNameFilter.h"
#include "FieldInfos.h"
#include "FieldInfo.h"
#include "CompoundFileReader.h"
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;
using namespace FTSMetadata;

/***
FUNCTION FTS$LUCENE_VERSION ()
RETURNS VARCHAR(20) CHARACTER SET UTF8
DETERMINISTIC
EXTERNAL NAME 'luceneudr!getLuceneVersion'
ENGINE UDR;
***/
FB_UDR_BEGIN_FUNCTION(getLuceneVersion)
    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(80, CS_UTF8), luceneVersion)
    );


    FB_UDR_EXECUTE_FUNCTION
    {
        const std::string luceneVersion = StringUtils::toUTF8(Constants::LUCENE_VERSION);

        out->luceneVersionNull = false;
        out->luceneVersion.length = static_cast<ISC_USHORT>(luceneVersion.length());
        luceneVersion.copy(out->luceneVersion.str, out->luceneVersion.length);
    }
FB_UDR_END_FUNCTION

/***
PROCEDURE FTS$INDEX_STATISTICS (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
  FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
  FTS$INDEX_STATUS TYPE OF FTS$D_INDEX_STATUS,
  FTS$INDEX_DIRECTORY VARCHAR(255) CHARACTER SET UTF8,
  FTS$INDEX_EXISTS BOOLEAN,
  FTS$INDEX_OPTIMIZED BOOLEAN,
  FTS$HAS_DELETIONS BOOLEAN,
  FTS$NUM_DOCS INTEGER,
  FTS$NUM_DELETED_DOCS INTEGER,
  FTS$NUM_FIELDS SMALLINT,
  FTS$INDEX_SIZE BIGINT
)
EXTERNAL NAME 'luceneudr!getIndexStatistics'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getIndexStatistics)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
    );


    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
        (FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
        (FB_INTL_VARCHAR(1020, CS_UTF8), indexDir)
        (FB_BOOLEAN, indexExists)
        (FB_BOOLEAN, isOptimized)
        (FB_BOOLEAN, hasDeletions)
        (FB_INTEGER, numDocs)
        (FB_INTEGER, numDeletedDocs)
        (FB_SMALLINT, numFields)
        (FB_BIGINT, indexSize)
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

        const auto& ftsDirectoryPath = getFtsDirectory(status, context);
        // check if there is a directory for full-text indexes
        if (!fs::is_directory(ftsDirectoryPath)) {
            throwException(status, R"(Fts directory "%s" not exists)", ftsDirectoryPath.u8string().c_str());
        }

        att.reset(context->getAttachment(status));
        tra.reset(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        out->analyzerNameNull = true;
        out->indexStatusNull = true;
        out->indexDirNull = true;
        out->indexExistsNull = true;
        out->isOptimizedNull = true;
        out->hasDeletionsNull = true;
        out->numDocsNull = true;
        out->numDeletedDocsNull = true;
        out->numFieldsNull = true;
        out->indexSizeNull = true;

        try {
            // get FTS index metadata
            auto ftsIndex = procedure->indexRepository->getIndex(status, att, tra, sqlDialect, indexName);

            out->analyzerNameNull = false;
            out->analyzerName.length = static_cast<ISC_USHORT>(ftsIndex->analyzer.length());
            ftsIndex->analyzer.copy(out->analyzerName.str, out->analyzerName.length);

            const auto& indexDirectoryPath = ftsDirectoryPath / indexName;

            const std::string indexDir = indexDirectoryPath.u8string();
            out->indexDirNull = false;
            out->indexDir.length = static_cast<ISC_USHORT>(indexDir.length());
            indexDir.copy(out->indexDir.str, out->indexDir.length);

            out->indexExistsNull = false;
            out->indexExists = true;
            // Check if the index directory exists
            if (!fs::is_directory(indexDirectoryPath)) {
                // index created, but not build
                ftsIndex->status = "N";
                out->indexExists = false;
            }
            else {
                const auto& ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
                if (!IndexReader::indexExists(ftsIndexDir)) {
                    // index created, but not build
                    ftsIndex->status = "N";
                    out->indexExists = false;
                }
                else {
                    const auto reader = IndexReader::open(ftsIndexDir, true);
                    LuceneFileHelper luceneFileHelper(ftsIndexDir);

                    out->isOptimizedNull = false;
                    out->isOptimized = reader->isOptimized();

                    out->hasDeletionsNull = false;
                    out->hasDeletions = reader->hasDeletions();

                    out->numDocsNull = false;
                    out->numDocs = reader->numDocs();

                    out->numDeletedDocsNull = false;
                    out->numDeletedDocs = reader->numDeletedDocs();

                    //reader->getUniqueTermCount();

                    out->numFieldsNull = false;
                    auto fieldNames = reader->getFieldNames(IndexReader::FIELD_OPTION_ALL);
                    out->numFields = fieldNames.size();


                    // calculate index size
                    out->indexSizeNull = false;
                    out->indexSize = luceneFileHelper.getIndexSize();

                    reader->close();
                }
                ftsIndexDir->close();
            }
            out->indexStatusNull = false;
            out->indexStatus.length = static_cast<ISC_USHORT>(ftsIndex->status.length());
            ftsIndex->status.copy(out->indexStatus.str, out->indexStatus.length);
        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
    }

    AutoRelease<IAttachment> att{nullptr};
    AutoRelease<ITransaction> tra{nullptr};
    bool fetched = false;

    FB_UDR_FETCH_PROCEDURE
    {
        if (fetched) {
            return false;
        }
        fetched = !fetched;
        return true;
    }
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$INDEX_FIELDS (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
  FTS$FIELD_NAME VARCHAR(127) CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!getIndexFields'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getIndexFields)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
    );


    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(508, CS_UTF8), fieldName)
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

        const auto ftsDirectoryPath = getFtsDirectory(status, context);
        // check if there is a directory for full-text indexes
        if (!fs::is_directory(ftsDirectoryPath)) {
            throwException(status, R"(Fts directory "%s" not exists)", ftsDirectoryPath.u8string().c_str());
        }

        att.reset(context->getAttachment(status));
        tra.reset(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        out->fieldNameNull = true;

        try {
            // check for index existence
            if (!procedure->indexRepository->hasIndex(status, att, tra, sqlDialect, indexName)) {
                throwException(status, R"(Index "%s" not exists)", indexName.c_str());
            }


            const auto indexDirectoryPath = ftsDirectoryPath / indexName;

            // Check if the index directory exists
            if (!fs::is_directory(indexDirectoryPath)) {
                throwException(status, R"(Index directory "%s" not exists.)", indexDirectoryPath.u8string().c_str());
            }

            const auto ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
            if (!IndexReader::indexExists(ftsIndexDir)) {
                throwException(status, R"(Index "%s" not build.)", indexName.c_str());
            }
                
            const auto reader = IndexReader::open(ftsIndexDir, true);

            fieldNames = reader->getFieldNames(IndexReader::FIELD_OPTION_ALL);
            it = fieldNames.begin();

            reader->close();
            ftsIndexDir->close();

        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
    }

    AutoRelease<IAttachment> att;
    AutoRelease<ITransaction> tra;
    HashSet<String> fieldNames;
    HashSet<String>::const_iterator it;

    FB_UDR_FETCH_PROCEDURE
    {
        if (it == fieldNames.end()) {
            return false;
        }
        const auto unicodeFieldName = *it;
        const std::string fieldName = StringUtils::toUTF8(unicodeFieldName);
        out->fieldNameNull = false;
        out->fieldName.length = static_cast<ISC_USHORT>(fieldName.length());
        fieldName.copy(out->fieldName.str, out->fieldName.length);

        ++it;
        return true;
    }
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$INDEX_FILES (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
   FTS$FILE_NAME VARCHAR(127) CHARACTER SET UTF8,
   FTS$FILE_TYPE VARCHAR(63) CHARACTER SET UTF8,
   FTS$FILE_SIZE BIGINT
)
EXTERNAL NAME 'luceneudr!getIndexFiles'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getIndexFiles)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
    );


    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(508, CS_UTF8), fileName)
        (FB_INTL_VARCHAR(252, CS_UTF8), fileType)
        (FB_BIGINT, fileSize)
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

        const auto& ftsDirectoryPath = getFtsDirectory(status, context);
        // check if there is a directory for full-text indexes
        if (!fs::is_directory(ftsDirectoryPath)) {
            throwException(status, R"(Fts directory "%s" not exists)", ftsDirectoryPath.u8string().c_str());
        }

        att.reset(context->getAttachment(status));
        tra.reset(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        out->fileNameNull = true;
        out->fileTypeNull = true;
        out->fileSizeNull = true;

        try {
            // check for index existence
            if (!procedure->indexRepository->hasIndex(status, att, tra, sqlDialect, indexName)) {
                throwException(status, R"(Index "%s" not exists)", indexName.c_str());
            }


            const auto indexDirectoryPath = ftsDirectoryPath / indexName;

            // Check if the index directory exists
            if (!fs::is_directory(indexDirectoryPath)) {
                throwException(status, R"(Index directory "%s" not exists.)", indexDirectoryPath.u8string().c_str());
            }

            const auto unicodeIndexDir = indexDirectoryPath.wstring();
            const auto ftsIndexDir = FSDirectory::open(unicodeIndexDir);
            luceneFileHelper.setDirectory(ftsIndexDir);

            auto allFileNames = ftsIndexDir->listAll();
            fileNames.assign(allFileNames.begin(), allFileNames.end());
            fileNames.remove_if([&unicodeIndexDir](const auto &fileName) {
                return !IndexFileNameFilter::getFilter()->accept(unicodeIndexDir, fileName);
            });
            it = fileNames.begin();

        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
    }

    AutoRelease<IAttachment> att;
    AutoRelease<ITransaction> tra;
    LuceneFileHelper luceneFileHelper;
    std::list<String> fileNames;
    std::list<String>::const_iterator it;

    FB_UDR_FETCH_PROCEDURE
    {
        if (it == fileNames.end()) {
            return false;
        }
        const auto unicodeFileName = *it;
        const std::string fileName = StringUtils::toUTF8(unicodeFileName);
        out->fileNameNull = false;
        out->fileName.length = static_cast<ISC_USHORT>(fileName.length());
        fileName.copy(out->fileName.str, out->fileName.length);

        out->fileTypeNull = false;
        const std::string fileType = LuceneFileHelper::getIndexFileType(unicodeFileName);
        out->fileType.length = static_cast<ISC_USHORT>(fileType.length());
        fileType.copy(out->fileType.str, out->fileType.length);


        out->fileSizeNull = false;
        out->fileSize = luceneFileHelper.getFileSize(unicodeFileName);

        ++it;
        return true;
    }
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$INDEX_SEGMENT_INFOS (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
   FTS$SEGMENT_NAME VARCHAR(63) CHARACTER SET UTF8,
   FTS$DOC_COUNT INTEGER,
   FTS$SEGMENT_SIZE BIGINT,
   FTS$USE_COMPOUND_FILE BOOLEAN,
   FTS$HAS_DELETIONS BOOLEAN,
   FTS$DEL_COUNT INTEGER,
   FTS$DEL_FILENAME VARCHAR(255) CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!getIndexSegments'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getIndexSegments)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
    );


    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), segmentName)
        (FB_INTEGER, docCount)
        (FB_BIGINT, segmentSize)
        (FB_BOOLEAN, useCompoundFile)
        (FB_BOOLEAN, hasDeletions)
        (FB_INTEGER, delCount)
        (FB_INTL_VARCHAR(1020, CS_UTF8), delFileName)
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

        const auto& ftsDirectoryPath = getFtsDirectory(status, context);
        // check if there is a directory for full-text indexes
        if (!fs::is_directory(ftsDirectoryPath)) {
            throwException(status, R"(Fts directory "%s" not exists)", ftsDirectoryPath.u8string().c_str());
        }

        att.reset(context->getAttachment(status));
        tra.reset(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);

        out->segmentNameNull = true;
        out->docCountNull = true;
        out->segmentSizeNull = true;
        out->useCompoundFileNull = true;
        out->hasDeletionsNull = true;
        out->delCountNull = true;
        out->delFileNameNull = true;

        try {
            // check for index existence
            if (!procedure->indexRepository->hasIndex(status, att, tra, sqlDialect, indexName)) {
                throwException(status, R"(Index "%s" not exists)", indexName.c_str());
            }

            const auto indexDirectoryPath = ftsDirectoryPath / indexName;

            // Check if the index directory exists
            if (!fs::is_directory(indexDirectoryPath)) {
                throwException(status, R"(Index directory "%s" not exists.)", indexDirectoryPath.u8string().c_str());
            }
            
            auto ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
            segmentInfos = newLucene<SegmentInfos>();
            segmentInfos->read(ftsIndexDir);
            

        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
    }

    AutoRelease<IAttachment> att{nullptr};
    AutoRelease<ITransaction> tra{nullptr};
    SegmentInfosPtr segmentInfos;
    int32_t segNo = 0;

    FB_UDR_FETCH_PROCEDURE
    {
        if (segNo >= segmentInfos->size()) {
            return false;
        }
        auto segmentInfo = segmentInfos->info(segNo);

        const std::string segmentName = StringUtils::toUTF8(segmentInfo->name);

        out->segmentNameNull = false;
        out->segmentName.length = static_cast<ISC_USHORT>(segmentName.length());
        segmentName.copy(out->segmentName.str, out->segmentName.length);

        out->docCountNull = false;
        out->docCount = segmentInfo->docCount;

        out->segmentSizeNull = false;
        out->segmentSize = segmentInfo->sizeInBytes();

        out->useCompoundFileNull = false;
        out->useCompoundFile = segmentInfo->getUseCompoundFile();

        out->hasDeletionsNull = false;
        out->hasDeletions = segmentInfo->hasDeletions();

        out->delCountNull = false;
        out->delCount = segmentInfo->getDelCount();

        out->delFileNameNull = true;
        auto unicodeDelFileName = segmentInfo->getDelFileName();
        if (!unicodeDelFileName.empty()) {	
            std::string delFileName = StringUtils::toUTF8(unicodeDelFileName);
            out->delFileNameNull = false;
            out->delFileName.length = static_cast<ISC_USHORT>(delFileName.length());
            delFileName.copy(out->delFileName.str, out->delFileName.length);
        }

        segNo++;
        return true;
    }
FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$INDEX_FIELD_INFOS (
   FTS$INDEX_NAME   VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$SEGMENT_NAME VARCHAR(63) CHARACTER SET UTF8
)
RETURNS (
   FTS$FIELD_NAME VARCHAR(127) CHARACTER SET UTF8,
   FTS$FIELD_NUMBER SMALLINT,
   FTS$IS_INDEXED BOOLEAN,
   FTS$STORE_TERM_VECTOR BOOLEAN,
   FTS$STORE_OFFSET_WITH_TERM_VECTOR BOOLEAN,
   FTS$STORE_POSITION_WITH_TERM_VECTOR BOOLEAN,
   FTS$OMIT_NORMS BOOLEAN,
   FTS$OMIT_TERM_FREQ_AND_POSITIONS BOOLEAN,
   FTS$STORE_PAYLOADS BOOLEAN
)
EXTERNAL NAME 'luceneudr!getFieldInfos'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getFieldInfos)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), indexName)
        (FB_INTL_VARCHAR(252, CS_UTF8), segmentName)
    );


    FB_UDR_MESSAGE(OutMessage,		
        (FB_INTL_VARCHAR(508, CS_UTF8), fieldName)
        (FB_SMALLINT, fieldNumber)
        (FB_BOOLEAN, isIndexed)
        (FB_BOOLEAN, storeTermVector)
        (FB_BOOLEAN, storeOffsetWithTermVector)
        (FB_BOOLEAN, storePositionWithTermVector)
        (FB_BOOLEAN, omitNorms)
        (FB_BOOLEAN, omitTermFreqAndPositions)
        (FB_BOOLEAN, storePayloads)
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

        String unicodeSegmentName;
        std::string segmentName;
        if (!in->segmentNameNull) {
            segmentName.assign(in->segmentName.str, in->segmentName.length);
            unicodeSegmentName = StringUtils::toUnicode(segmentName);
        }

        const auto ftsDirectoryPath = getFtsDirectory(status, context);
        // check if there is a directory for full-text indexes
        if (!fs::is_directory(ftsDirectoryPath)) {
            throwException(status, R"(Fts directory "%s" not exists)", ftsDirectoryPath.u8string().c_str());
        }

        att.reset(context->getAttachment(status));
        tra.reset(context->getTransaction(status));

        const unsigned int sqlDialect = getSqlDialect(status, att);


        try {
            // check for index existence
            if (!procedure->indexRepository->hasIndex(status, att, tra, sqlDialect, indexName)) {
                throwException(status, R"(Index "%s" not exists)", indexName.c_str());
            }

            const auto indexDirectoryPath = ftsDirectoryPath / indexName;

            // Check if the index directory exists
            if (!fs::is_directory(indexDirectoryPath)) {
                throwException(status, R"(Index directory "%s" not exists.)", indexDirectoryPath.u8string().c_str());
            }

            auto ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
            auto segmentInfos = newLucene<SegmentInfos>();
            segmentInfos->read(ftsIndexDir);
        
            SegmentInfoPtr segmentInfo = nullptr;
            if (unicodeSegmentName.empty()) {
                int32_t segNo = segmentInfos->size() - 1;
                segmentInfo = segmentInfos->info(segNo);
            }
            else {
                for (int32_t segNo = 0; segNo < segmentInfos->size(); segNo++) {
                    const auto curSegmentInfo = segmentInfos->info(segNo);
                    if (curSegmentInfo->name == unicodeSegmentName) {
                        segmentInfo = curSegmentInfo;
                        break;
                    }
                }
            }
            
            if (segmentInfo == nullptr) {
                throwException(status, R"(Segment "%s" not found)", segmentName.c_str());
            }
            
            DirectoryPtr ftsFieldDir(ftsIndexDir);
            if (segmentInfo->getUseCompoundFile()) {
                ftsFieldDir = newLucene<CompoundFileReader>(ftsIndexDir, segmentInfo->name + L"." + IndexFileNames::COMPOUND_FILE_EXTENSION());
            }
            fieldInfos = newLucene<FieldInfos>(ftsFieldDir, segmentInfo->name + L"." + IndexFileNames::FIELD_INFOS_EXTENSION());

        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
    }

    AutoRelease<IAttachment> att{nullptr};
    AutoRelease<ITransaction> tra{nullptr};
    FieldInfosPtr fieldInfos;
    int32_t fieldNo = 0;

    FB_UDR_FETCH_PROCEDURE
    {
        if (fieldNo >= fieldInfos->size()) {
            return false;
        }
        auto fieldInfo = fieldInfos->fieldInfo(fieldNo);

        const std::string fieldName = StringUtils::toUTF8(fieldInfo->name);
        out->fieldNameNull = false;
        out->fieldName.length = static_cast<ISC_USHORT>(fieldName.length());
        fieldName.copy(out->fieldName.str, out->fieldName.length);

        out->fieldNumberNull = false;
        out->fieldNumber = static_cast<ISC_USHORT>(fieldInfo->number);

        out->isIndexedNull = false;
        out->isIndexed = fieldInfo->isIndexed;

        out->storeTermVectorNull = false;
        out->storeTermVector = fieldInfo->storeTermVector;

        out->storeOffsetWithTermVectorNull = false;
        out->storeOffsetWithTermVector = fieldInfo->storeOffsetWithTermVector;

        out->storePositionWithTermVectorNull = false;
        out->storePositionWithTermVector = fieldInfo->storePositionWithTermVector;

        out->omitNormsNull = false;
        out->omitNorms = fieldInfo->omitNorms;

        out->omitTermFreqAndPositionsNull = false;
        out->omitTermFreqAndPositions = fieldInfo->omitTermFreqAndPositions;

        out->storePayloadsNull = false;
        out->storePayloads = fieldInfo->storePayloads;

        fieldNo++;
        return true;
    }
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$INDEX_TERMS (
    FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
    FTS$FIELD_NAME  VARCHAR(63) CHARACTER SET UTF8,
    FTS$TERM        VARCHAR(8191) CHARACTER SET UTF8,
    FTS$DOC_FREQ    INTEGER
)
EXTERNAL NAME 'luceneudr!indexTerms'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(indexTerms)
    FB_UDR_MESSAGE(InMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
    );

    FB_UDR_MESSAGE(OutMessage,
        (FB_INTL_VARCHAR(252, CS_UTF8), field_name)
        (FB_INTL_VARCHAR(8191 * 4, CS_UTF8), term)
        (FB_INTEGER, doc_freq)
    );

    FB_UDR_CONSTRUCTOR
        , indexRepository(std::make_unique<FTSIndexRepository>(context->getMaster()))
    {
    }

    FTSIndexRepositoryPtr indexRepository{ nullptr };

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
            const auto indexDirectoryPath = ftsDirectoryPath / indexName;
            if (!fs::is_directory(indexDirectoryPath)) {
                throwException(status, R"(Index directory "%s" not exists.)", indexDirectoryPath.u8string().c_str());
            }

            auto ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
            auto reader = IndexReader::open(ftsIndexDir, true);
            termIt = reader->terms();
            out->field_nameNull = true;
            out->termNull = true;
            out->doc_freqNull = true;
        }
        catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            throwException(status, error_message.c_str());
        }
    }

    TermEnumPtr termIt{ nullptr };

    FB_UDR_FETCH_PROCEDURE
    {
        if (!termIt->next()) {
            termIt->close();
            return false;
        }

        auto term = termIt->term();
        auto wFieldName = term->field();
        auto fieldName = StringUtils::toUTF8(wFieldName);
        auto wText = term->text();
        if (wText.length() > 8191) {
            throwException(status, "Term size exceeds 8191 characters");
        }
        auto text = StringUtils::toUTF8(wText);

        out->field_nameNull = false;
        out->field_name.length = static_cast<ISC_USHORT>(fieldName.size());
        fieldName.copy(out->field_name.str, out->field_name.length);

        out->termNull = false;
        out->term.length = static_cast<ISC_USHORT>(text.size());
        text.copy(out->term.str, out->term.length);

        out->doc_freqNull = false;
        out->doc_freq = static_cast<ISC_LONG>(termIt->docFreq());

        return true;
    }

FB_UDR_END_PROCEDURE
