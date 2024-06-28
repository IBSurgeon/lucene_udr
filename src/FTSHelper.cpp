#include "FTSHelper.h"

#include "FBUtils.h"
#include "FTSUtils.h"
#include "LuceneHeaders.h"

namespace LuceneUDR
{
    FTSPreparedIndex::FTSPreparedIndex(
        Firebird::ThrowStatusWrapper* status,
        Firebird::IAttachment* att,
        Firebird::ITransaction* tra,
        unsigned int sqlDialect,
        FTSMetadata::FTSIndex&& ftsIndex,
        bool whereKey
    )
        : m_ftsIndex(std::move(ftsIndex))
        , m_fields()
        , m_stmtExtractRecord{ nullptr }
        , m_inMetaExtractRecord{ nullptr }
        , m_outMetaExtractRecord{ nullptr }
    {
        // check all field present
        for (const auto& segment : m_ftsIndex.segments) {
            if (!segment.isFieldExists()) {
                auto iscStatus = IscRandomStatus::createFmtStatus(
                    R"(Invalid FTS index "%s". Field "%s" not exists in relation "%s".)",
                    m_ftsIndex.indexName.c_str(),
                    segment.fieldName().c_str(),
                    m_ftsIndex.relationName.c_str()
                );
                throw Firebird::FbException(status, iscStatus);
            }
        }

        std::string sql = m_ftsIndex.buildSqlSelectFieldValues(status, sqlDialect, whereKey);

        m_stmtExtractRecord.reset(att->prepare(
            status,
            tra,
            0,
            sql.c_str(),
            sqlDialect,
            Firebird::IStatement::PREPARE_PREFETCH_METADATA
        ));
        // get a description of the fields				
        Firebird::AutoRelease<Firebird::IMessageMetadata> outputMetadata(m_stmtExtractRecord->getOutputMetadata(status));
        // make all fields of string type except BLOB
        m_outMetaExtractRecord.reset(prepareTextMetaData(status, outputMetadata));
        m_inMetaExtractRecord.reset(m_stmtExtractRecord->getInputMetadata(status));

        const auto fieldCount = m_outMetaExtractRecord->getCount(status);
        m_fields.reserve(fieldCount);
        for (unsigned i = 0; i < fieldCount; i++) {
            m_fields.emplace_back(status, m_outMetaExtractRecord, i);
        }

        // initial specific FTS property for fields
        for (auto& field : m_fields) {
            auto iSegment = ftsIndex.findSegment(field.fieldName);
            if (iSegment == ftsIndex.segments.end()) {
                auto iscStatus = IscRandomStatus::createFmtStatus(
                    R"(Invalid FTS index "%s". Field "%s" not found.)", 
                    m_ftsIndex.indexName.c_str(), 
                    field.fieldName.c_str()
                );
                throw Firebird::FbException(status, iscStatus);
            }
            auto&& segment = *iSegment;
            field.ftsFieldName = Lucene::StringUtils::toUnicode(segment.fieldName());
            field.ftsKey = segment.isKey();
            field.ftsBoost = segment.boost();
            field.ftsBoostNull = segment.isBoostNull();
        }
    }


    void FTSPreparedIndex::rebuild(
        Firebird::ThrowStatusWrapper* status,
        const std::filesystem::path& ftsDirectoryPath,
        Firebird::ITransaction* tra
    )
    {
        // Check if the index directory exists, and if it doesn't exist, create it. 
        const auto indexDirectoryPath = ftsDirectoryPath / m_ftsIndex.indexName;

        if (!LuceneUDR::createIndexDirectory(indexDirectoryPath)) {
            throwException(status, R"(Cannot create index directory "%s".)", indexDirectoryPath.u8string().c_str());
    }
}