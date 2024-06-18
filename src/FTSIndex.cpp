/**
 *  Utilities for getting and managing metadata for full-text indexes.
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

#include "FTSIndex.h"
#include "FBUtils.h"
#include "LazyFactory.h"
#include "Relations.h"
#include "Analyzers.h"

using namespace Firebird;
using namespace std;
using namespace LuceneUDR;

namespace {

    constexpr const char* SQL_CREATE_FTS_INDEX = R"SQL(
INSERT INTO FTS$INDICES (
  FTS$INDEX_NAME, 
  FTS$RELATION_NAME, 
  FTS$ANALYZER, 
  FTS$DESCRIPTION, 
  FTS$INDEX_STATUS)
VALUES(?, ?, ?, ?, ?)
)SQL";

    constexpr const char* SQL_DROP_FTS_INDEX = R"SQL(
DELETE FROM FTS$INDICES WHERE FTS$INDEX_NAME = ?
)SQL";

    constexpr const char* SQL_FTS_INDEX_EXISTS = R"SQL(
SELECT COUNT(*) AS CNT
FROM FTS$INDICES
WHERE FTS$INDEX_NAME = ?
)SQL";

    constexpr const char* SQL_SET_FTS_INDEX_STATUS = R"SQL(
UPDATE FTS$INDICES SET FTS$INDEX_STATUS = ? WHERE FTS$INDEX_NAME = ?
)SQL";

    constexpr const char* SQL_GET_FTS_INDEX = R"SQL(
SELECT 
  FTS$INDEX_NAME, 
  FTS$RELATION_NAME, 
  FTS$ANALYZER, 
  FTS$DESCRIPTION, 
  FTS$INDEX_STATUS
FROM FTS$INDICES
WHERE FTS$INDEX_NAME = ?
)SQL";

    constexpr const char* SQL_ALL_FTS_INDECES = R"SQL(
SELECT 
  FTS$INDEX_NAME, 
  FTS$RELATION_NAME, 
  FTS$ANALYZER, 
  FTS$DESCRIPTION, 
  FTS$INDEX_STATUS
FROM FTS$INDICES
ORDER BY FTS$INDEX_NAME
)SQL";

    constexpr const char* SQL_ALL_FTS_INDECES_AND_SEGMENTS = R"SQL(
SELECT
  FTS$INDICES.FTS$INDEX_NAME,
  FTS$INDICES.FTS$RELATION_NAME,
  FTS$INDICES.FTS$ANALYZER,
  FTS$INDICES.FTS$INDEX_STATUS,
  FTS$INDEX_SEGMENTS.FTS$FIELD_NAME,
  FTS$INDEX_SEGMENTS.FTS$KEY,
  FTS$INDEX_SEGMENTS.FTS$BOOST,
  (RF.RDB$FIELD_NAME IS NOT NULL) AS FIELD_EXISTS
FROM FTS$INDICES
LEFT JOIN FTS$INDEX_SEGMENTS ON FTS$INDEX_SEGMENTS.FTS$INDEX_NAME = FTS$INDICES.FTS$INDEX_NAME
LEFT JOIN RDB$RELATION_FIELDS RF
    ON RF.RDB$RELATION_NAME = FTS$INDICES.FTS$RELATION_NAME
   AND RF.RDB$FIELD_NAME = FTS$INDEX_SEGMENTS.FTS$FIELD_NAME
ORDER BY FTS$INDICES.FTS$INDEX_NAME
)SQL";

    constexpr const char* SQL_FTS_INDEX_SEGMENTS = R"SQL(
SELECT
  FTS$INDEX_SEGMENTS.FTS$INDEX_NAME,
  FTS$INDEX_SEGMENTS.FTS$FIELD_NAME,
  FTS$INDEX_SEGMENTS.FTS$KEY,
  FTS$INDEX_SEGMENTS.FTS$BOOST,
  (RF.RDB$FIELD_NAME IS NOT NULL) AS FIELD_EXISTS
FROM FTS$INDICES
JOIN FTS$INDEX_SEGMENTS
    ON FTS$INDEX_SEGMENTS.FTS$INDEX_NAME = FTS$INDICES.FTS$INDEX_NAME
LEFT JOIN RDB$RELATION_FIELDS RF
    ON RF.RDB$RELATION_NAME = FTS$INDICES.FTS$RELATION_NAME
   AND RF.RDB$FIELD_NAME = FTS$INDEX_SEGMENTS.FTS$FIELD_NAME
WHERE FTS$INDICES.FTS$INDEX_NAME = ?
)SQL";

    constexpr const char* SQL_FTS_INDEX_FIELD_EXISTS = R"SQL(
SELECT COUNT(*) AS CNT
FROM FTS$INDEX_SEGMENTS
WHERE FTS$INDEX_NAME = ? AND FTS$FIELD_NAME = ?
)SQL";

    constexpr const char* SQL_FTS_KEY_INDEX_FIELD_EXISTS = R"SQL(
SELECT COUNT(*) AS CNT
FROM FTS$INDEX_SEGMENTS
WHERE FTS$INDEX_NAME = ? AND FTS$KEY IS TRUE
)SQL";

    constexpr const char* SQL_GET_FTS_KEY_INDEX_FIELD = R"SQL(
SELECT FTS$INDEX_NAME, FTS$FIELD_NAME
FROM FTS$INDEX_SEGMENTS
WHERE FTS$INDEX_NAME = ? AND FTS$KEY IS TRUE
)SQL";

    constexpr const char* SQL_FTS_ADD_INDEX_FIELD = R"SQL(
INSERT INTO FTS$INDEX_SEGMENTS (
  FTS$INDEX_NAME, 
  FTS$FIELD_NAME, 
  FTS$KEY, 
  FTS$BOOST
)
VALUES(?, ?, ?, ?)
)SQL";

    constexpr const char* SQL_FTS_DROP_INDEX_FIELD = R"SQL(
DELETE FROM FTS$INDEX_SEGMENTS
WHERE FTS$INDEX_NAME = ? AND FTS$FIELD_NAME = ?
)SQL";

    constexpr const char* SQL_FTS_SET_INDEX_FIELD_BOOST = R"SQL(
UPDATE FTS$INDEX_SEGMENTS
SET FTS$BOOST = ?
WHERE FTS$INDEX_NAME = ? AND FTS$FIELD_NAME = ?
)SQL";

    constexpr const char* SQL_HAS_INDEX_BY_ANALYZER = R"SQL(
SELECT COUNT(*) AS CNT
FROM FTS$INDICES
WHERE FTS$ANALYZER = ?
)SQL";

    constexpr const char* SQL_ACTIVE_INDEXES_BY_ANALYZER = R"SQL(
SELECT FTS$INDEX_NAME
FROM FTS$INDICES
WHERE FTS$ANALYZER = ? AND FTS$INDEX_STATUS = 'C'
)SQL";

}

namespace FTSMetadata
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

    FTSIndex::FTSIndex(const FTSIndexRecord& record)
    {
        indexName.assign(record->indexName.str, record->indexName.length);
        relationName.assign(record->relationName.str, record->relationName.length);
        analyzer.assign(record->analyzer.str, record->analyzer.length);
        status.assign(record->indexStatus.str, record->indexStatus.length);
    }

    void FTSIndex::init(const FTSIndexRecord& record)
    {
        indexName.assign(record->indexName.str, record->indexName.length);
        relationName.assign(record->relationName.str, record->relationName.length);
        analyzer.assign(record->analyzer.str, record->analyzer.length);
        status.assign(record->indexStatus.str, record->indexStatus.length);
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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect
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
        ThrowStatusWrapper* status,
        unsigned int sqlDialect,
        bool whereKey)
    {
        auto iKeySegment = findKey();
        if (iKeySegment == segments.end()) {
            throwException(status, R"(Key field not exists in index "%s".)", indexName.c_str());
        }
        const string keyFieldName = (*iKeySegment)->fieldName;

        std::stringstream ss;
        ss << "SELECT\n";
        int field_cnt = 0;
        for (const auto& segment : segments) {
            if (field_cnt == 0) {
                ss << "  " << escapeMetaName(sqlDialect, segment->fieldName);
            }
            else {
                ss << ",\n  " << escapeMetaName(sqlDialect, segment->fieldName);
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
            for (const auto& segment : segments) {
                if (segment->fieldName == keyFieldName) continue;
                if (where.empty())
                    where += escapeMetaName(sqlDialect, segment->fieldName) + " IS NOT NULL";
                else
                    where += " OR " + escapeMetaName(sqlDialect, segment->fieldName) + " IS NOT NULL";
            }
            if (!where.empty())
                ss << "\nAND (" << where << ")";
        }
        return ss.str();
    }

    //
    // FTSIndexRepository implementation
    //

    FTSIndexRepository::FTSIndexRepository(IMaster* master)
        : m_master(master)
        , m_analyzerRepository(new AnalyzerRepository(master))
        , m_relationHelper(new RelationHelper(master))
    {
    }

    FTSIndexRepository::~FTSIndexRepository()
    {
        delete m_relationHelper;
        delete m_analyzerRepository;
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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view indexName,
        std::string_view relationName,
        std::string_view analyzerName,
        ISC_QUAD* description)
    {
        // check for index existence
        if (hasIndex(status, att, tra, sqlDialect, indexName)) {
            std::string sIndexName{ indexName };
            throwException(status, R"(Index "%s" already exists)", sIndexName.c_str());
        }

        // checking the existence of the analyzer
        if (!m_analyzerRepository->hasAnalyzer(status, att, tra, sqlDialect, analyzerName)) {
            std::string sAnalyzerName{ analyzerName };
            throwException(status, R"(Analyzer "%s" not exists)", sAnalyzerName.c_str());
        }

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

        if (description) {
            input->descriptionNull = false;
            input->description = *description;
        }
        else {
            input->descriptionNull = true;
        }

        const char* indexStatus = "N";
        input->indexStatus.length = 1;
        memcpy(input->indexStatus.str, indexStatus, 1);


        att->execute(
            status,
            tra,
            0,
            SQL_CREATE_FTS_INDEX,
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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view indexName)
    {

        FB_MESSAGE(Input, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), indexName)
        ) input(status, m_master);

        input.clear();
    
        input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
        indexName.copy(input->indexName.str, input->indexName.length);

        // check for index existence
        if (!hasIndex(status, att, tra, sqlDialect, indexName)) {
            std::string sIndexName{ indexName };
            throwException(status, R"(Index "%s" not exists)", sIndexName.c_str());
        }

        att->execute(
            status,
            tra,
            0,
            SQL_DROP_FTS_INDEX,
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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view indexName,
        std::string_view indexStatus)
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
            SQL_SET_FTS_INDEX_STATUS,
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
        ThrowStatusWrapper* status, 
        IAttachment* att, 
        ITransaction* tra, 
        unsigned int sqlDialect, 
        std::string_view indexName)
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
                IStatement::PREPARE_PREFETCH_NONE
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_exists_index->openCursor(
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
        rs.release();

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
    FTSIndexPtr FTSIndexRepository::getIndex (
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view indexName,
        bool withSegments)
    {	
        FTSIndexNameInput input(status, m_master);		
        FTSIndexRecord output(status, m_master);

        input.clear();
        input->indexNameNull = false;
        input->indexName.length = static_cast<ISC_USHORT>(indexName.length());
        indexName.copy(input->indexName.str, input->indexName.length);

        if (!m_stmt_get_index.hasData()) {
            m_stmt_get_index.reset(att->prepare(
                status,
                tra,
                0,
                SQL_GET_FTS_INDEX,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_NONE
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_get_index->openCursor(
            status,
            tra,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            0
        ));
        
        int result = rs->fetchNext(status, output.getData());
        rs->close(status);
        rs.release();

        if (result == IStatus::RESULT_NO_DATA) {
            std::string sIndexName(indexName);
            throwException(status, R"(Index "%s" not exists)", sIndexName.c_str());
        }
        // index found
        auto ftsIndex = std::make_unique<FTSIndex>();

        ftsIndex->indexName.assign(output->indexName.str, output->indexName.length);
        ftsIndex->relationName.assign(output->relationName.str, output->relationName.length);
        ftsIndex->analyzer.assign(output->analyzer.str, output->analyzer.length);
        ftsIndex->status.assign(output->indexStatus.str, output->indexStatus.length);	

        if (withSegments) {
            fillIndexFields(status, att, tra, sqlDialect, indexName, ftsIndex->segments);
        }
        return ftsIndex;
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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        FTSIndexList& indexes)
    {

        FTSIndexRecord output(status, m_master);
    
        AutoRelease<IResultSet> rs(att->openCursor(
            status,
            tra,
            0,
            SQL_ALL_FTS_INDECES,
            sqlDialect,
            nullptr,
            nullptr,
            output.getMetadata(),
            nullptr,
            0
        ));

        while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
            auto ftsIndex = make_unique<FTSIndex>(output);
            indexes.push_back(std::move(ftsIndex));
        }
        rs->close(status);
        rs.release();
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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
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


        AutoRelease<IResultSet> rs(att->openCursor(
            status,
            tra,
            0,
            SQL_ALL_FTS_INDECES_AND_SEGMENTS,
            sqlDialect,
            nullptr,
            nullptr,
            output.getMetadata(),
            nullptr,
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
        rs.release();

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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view indexName,
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
                IStatement::PREPARE_PREFETCH_NONE
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_index_fields->openCursor(
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
        rs.release();
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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view indexName
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
        
        AutoRelease<IResultSet> rs(att->openCursor(
            status,
            tra,
            0,
            SQL_FTS_KEY_INDEX_FIELD_EXISTS,
            sqlDialect,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            nullptr,
            0
        ));
        
        bool foundFlag = false;
        if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
            foundFlag = (output->cnt > 0);
        }
        rs->close(status);
        rs.release();

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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        const FTSIndexSegmentPtr& keyIndexSegment,
        std::string_view indexName)
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
                IStatement::PREPARE_PREFETCH_NONE
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_index_key_field->openCursor(
            status,
            tra,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            0
        ));

        int	result = rs->fetchNext(status, output.getData());
        rs->close(status);
        rs.release();

        if (result == IStatus::RESULT_NO_DATA) {
            std::string sIndexName{ indexName };
            throwException(status, R"(Key field not exists in index "%s".)", sIndexName.c_str());
        }
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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view indexName,
        std::string_view fieldName,
        bool key,
        double boost,
        bool boostNull)
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

        auto ftsIndex = getIndex(status, att, tra, sqlDialect, indexName);

        // Checking whether the key field exists in the index.
        if (key && hasKeyIndexField(status, att, tra, sqlDialect, indexName)) {
            std::string sIndexName{ indexName };
            throwException(status, R"(The key field already exists in the "%s" index.)", sIndexName.c_str());
        }

        // Checking whether the field exists in the index.
        if (hasIndexField(status, att, tra, sqlDialect, indexName, fieldName)) {
            std::string sIndexName{ indexName };
            std::string sFieldName{ indexName };
            throwException(status, R"(Field "%s" already exists in index "%s")", sFieldName.c_str(), sIndexName.c_str());
        }

        // Checking whether the field exists in relation.
        if (!m_relationHelper->fieldExists(status, att, tra, sqlDialect, ftsIndex->relationName, fieldName)) {
            std::string sFieldName{ indexName };
            throwException(status, R"(Field "%s" not exists in relation "%s".)", sFieldName.c_str(), ftsIndex->relationName.c_str());
        }


        att->execute(
            status,
            tra,
            0,
            SQL_FTS_ADD_INDEX_FIELD,
            sqlDialect,
            input.getMetadata(),
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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view indexName,
        std::string_view fieldName)
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
            std::string sIndexName{ indexName };
            throwException(status, R"(Index "%s" not exists)", sIndexName.c_str());
        }

        // Checking whether the field exists in the index.
        if (!hasIndexField(status, att, tra, sqlDialect, indexName, fieldName)) {
            std::string sIndexName{ indexName };
            std::string sFieldName{ fieldName };
            throwException(status, R"(Field "%s" not exists in index "%s")", sFieldName.c_str(), sIndexName.c_str());
        }

        att->execute(
            status,
            tra,
            0,
            SQL_FTS_DROP_INDEX_FIELD,
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
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view indexName,
        std::string_view fieldName,
        double boost,
        bool boostNull)
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
            std::string sIndexName{ indexName };
            throwException(status, R"(Index "%s" not exists)", sIndexName.c_str());
        }

        // Checking whether the field exists in the index.
        if (!hasIndexField(status, att, tra, sqlDialect, indexName, fieldName)) {
            std::string sIndexName{ indexName };
            std::string sFieldName{ fieldName };
            throwException(status, R"(Field "%s" not exists in index "%s")", sFieldName.c_str(), sIndexName.c_str());
        }

        att->execute(
            status,
            tra,
            0,
            SQL_FTS_SET_INDEX_FIELD_BOOST,
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
    bool FTSIndexRepository::hasIndexField(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view indexName,
        std::string_view fieldName)
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

        AutoRelease<IResultSet> rs(att->openCursor(
            status,
            tra,
            0,
            SQL_FTS_INDEX_FIELD_EXISTS,
            sqlDialect,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            nullptr,
            0
        ));

        bool foundFlag = false;
        if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
            foundFlag = (output->cnt > 0);
        }
        rs->close(status);
        rs.release();

        return foundFlag;
    }

    bool FTSIndexRepository::hasIndexByAnalyzer(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view analyzerName)
    {
        FB_MESSAGE(Input, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
        ) input(status, m_master);

        FB_MESSAGE(Output, ThrowStatusWrapper,
            (FB_BIGINT, cnt)
        ) output(status, m_master);

        input.clear();

        input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
        analyzerName.copy(input->analyzerName.str, input->analyzerName.length);


        AutoRelease<IResultSet> rs(att->openCursor(
            status,
            tra,
            0,
            SQL_HAS_INDEX_BY_ANALYZER,
            sqlDialect,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            nullptr,
            0
        ));

        bool foundFlag = false;
        if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
            foundFlag = (output->cnt > 0);
        }
        rs->close(status);
        rs.release();

        return foundFlag;
    }

    unordered_set<string> FTSIndexRepository::getActiveIndexByAnalyzer(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view analyzerName)
    {
        // m_stmt_active_indexes_by_analyzer
        FB_MESSAGE(Input, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
        ) input(status, m_master);

        FB_MESSAGE(Output, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), indexName)
        ) output(status, m_master);

        input.clear();

        input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
        analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

        unordered_set<string> indexNames;

        if (!m_stmt_active_indexes_by_analyzer.hasData()) {
            m_stmt_active_indexes_by_analyzer.reset(att->prepare(
                status,
                tra,
                0,
                SQL_ACTIVE_INDEXES_BY_ANALYZER,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_NONE
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_active_indexes_by_analyzer->openCursor(
            status,
            tra,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            0
        ));

        while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
            indexNames.emplace(output->indexName.str, output->indexName.length);
        }
        rs->close(status);
        rs.release();

        return indexNames;
    }

}
