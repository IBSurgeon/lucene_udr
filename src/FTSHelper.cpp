#include "FTSHelper.h"

#include "Analyzers.h"
#include "FBUtils.h"
#include "FTSUtils.h"



namespace LuceneUDR
{
    using namespace Firebird;
    using namespace Lucene;

    FTSPreparedIndex prepareFtsIndex(
        Firebird::ThrowStatusWrapper* status,
        Firebird::IMaster* master,
        Firebird::IAttachment* att,
        Firebird::ITransaction* tra,
        unsigned int sqlDialect,
        FTSMetadata::FTSIndex&& ftsIndex,
        const std::filesystem::path& ftsDirectoryPath,
        bool whereKey)
    {
        return FTSPreparedIndex(status, master, att, tra, sqlDialect, std::move(ftsIndex), ftsDirectoryPath, whereKey);
    }

    FTSPreparedIndex::FTSPreparedIndex(
        ThrowStatusWrapper* status,
        Firebird::IMaster* master,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        FTSMetadata::FTSIndex&& ftsIndex,
        const std::filesystem::path& ftsDirectoryPath,
        bool whereKey
    )
        : m_master(master)
        , m_ftsIndex(std::move(ftsIndex))
        , m_fields()
        , m_params()
        , m_indexDirectoryPath(ftsDirectoryPath / m_ftsIndex.indexName)
        , m_stmtExtractRecord{ nullptr }
        , m_inMetaExtractRecord{ nullptr }
        , m_outMetaExtractRecord{ nullptr }
        , m_outputBuffer()
        , m_indexWriter()
        , m_unicodeKeyFieldName()
    {
        // check segments exists
        if (m_ftsIndex.emptySegments()) {
            auto iscStatus = IscRandomStatus::createFmtStatus(
                R"(Invalid FTS index "%s". The index does not contain fields.)", 
                m_ftsIndex.indexName.c_str()
            );
        }

        // check all field present
        for (const auto& segment : m_ftsIndex.segments) {
            if (!segment.isFieldExists()) {
                auto iscStatus = IscRandomStatus::createFmtStatus(
                    R"(Invalid FTS index "%s". Field "%s" not exists in relation "%s".)",
                    m_ftsIndex.indexName.c_str(),
                    segment.fieldName().c_str(),
                    m_ftsIndex.relationName.c_str()
                );
                throw FbException(status, iscStatus);
            }
        }

        std::string sql = m_ftsIndex.buildSqlSelectFieldValues(status, sqlDialect, whereKey);

        m_stmtExtractRecord.reset(att->prepare(
            status,
            tra,
            0,
            sql.c_str(),
            sqlDialect,
            IStatement::PREPARE_PREFETCH_METADATA
        ));
        // get a description of the fields				
        AutoRelease<IMessageMetadata> outputMetadata(m_stmtExtractRecord->getOutputMetadata(status));
        // make all fields of string type except BLOB
        m_outMetaExtractRecord.reset(prepareTextMetaData(status, outputMetadata));
        m_inMetaExtractRecord.reset(m_stmtExtractRecord->getInputMetadata(status));

        // preallocate output message buffer
        m_outputBuffer = std::vector<unsigned char>(m_outMetaExtractRecord->getMessageLength(status));

        // parameters description
        const auto paramCount = m_outMetaExtractRecord->getCount(status);
        if (whereKey) {
            if (paramCount != 1) {
                auto iscStatus = IscRandomStatus::createFmtStatus(
                    R"(Invalid FTS index "%s". Updates are only supported for single-key indexes.)",
                    m_ftsIndex.indexName.c_str()
                );
                throw FbException(status, iscStatus);
            }
            m_params.reserve(paramCount);
            for (unsigned i = 0; i < paramCount; i++) {
                m_params.emplace_back(status, m_inMetaExtractRecord, i);
            }
            // TODO: I'm not sure if this is necessary at all
            const auto& keyParam = m_params[0];
            if (keyParam.isBinary()) {
                switch (keyParam.length) {
                case 8:
                    m_ftsIndex.keyFieldType = FTSMetadata::FTSKeyType::DB_KEY;
                    break;
                case 16:
                    m_ftsIndex.keyFieldType = FTSMetadata::FTSKeyType::UUID;
                    break;
                default:
                    auto iscStatus = IscRandomStatus::createFmtStatus(
                        R"(Invalid FTS index "%s". The full-text index key has an unsupported data type to update.)",
                        m_ftsIndex.indexName.c_str());
                    throw FbException(status, iscStatus);
                }
            } else if (keyParam.isInt()) {
                m_ftsIndex.keyFieldType = FTSMetadata::FTSKeyType::INT_ID;
            } else {
                auto iscStatus = IscRandomStatus::createFmtStatus(
                    R"(Invalid FTS index "%s". The full-text index key has an unsupported data type to update.)",
                    m_ftsIndex.indexName.c_str());
                throw FbException(status, iscStatus);
            }
        }

        // field description
        const auto fieldCount = m_outMetaExtractRecord->getCount(status);
        m_fields.reserve(fieldCount);
        for (unsigned i = 0; i < fieldCount; i++) {
            m_fields.emplace_back(status, m_outMetaExtractRecord, i);
        }


        // initial specific FTS property for fields
        for (auto& field : m_fields) {
            auto iSegment = m_ftsIndex.findSegment(field.fieldName);
            if (iSegment == m_ftsIndex.segments.end()) {
                auto iscStatus = IscRandomStatus::createFmtStatus(
                    R"(Invalid FTS index "%s". Field "%s" not found.)", 
                    m_ftsIndex.indexName.c_str(), 
                    field.fieldName.c_str()
                );
                throw FbException(status, iscStatus);
            }
            auto&& segment = *iSegment;
            field.ftsFieldName = StringUtils::toUnicode(segment.fieldName());
            field.ftsKey = segment.isKey();
            field.ftsBoost = segment.boost();
            field.ftsBoostNull = segment.isBoostNull();
            if (field.ftsKey) {
                m_unicodeKeyFieldName = field.ftsFieldName;
            }
        }

        // Check if the index directory exists, and if it doesn't exist, create it.
        if (!LuceneUDR::createIndexDirectory(m_indexDirectoryPath)) {
            auto iscStatus = IscRandomStatus::createFmtStatus(
                R"(Cannot create index directory "%s".)", 
                m_indexDirectoryPath.u8string().c_str()
            );
            throw FbException(status, iscStatus);
        }

        FTSMetadata::AnalyzerRepository analyzerRepository(master);
        auto wIndexDirectoryPath = m_indexDirectoryPath.wstring();

        try {
            auto fsIndexDir = FSDirectory::open(wIndexDirectoryPath);
            auto analyzer = analyzerRepository.createAnalyzer(status, att, tra, sqlDialect, m_ftsIndex.analyzer);
            m_indexWriter = newLucene<IndexWriter>(fsIndexDir, analyzer, true, IndexWriter::MaxFieldLengthUNLIMITED);
        } catch (const LuceneException& e) {
            const std::string error_message = StringUtils::toUTF8(e.getError());
            auto iscStatus = IscRandomStatus(error_message);
            throw FbException(status, iscStatus);
        }
    }

    void FTSPreparedIndex::deleteAll(Firebird::ThrowStatusWrapper* status)
    try
    {
        m_indexWriter->deleteAll();
    } catch (const LuceneException& e) {
        const std::string error_message = StringUtils::toUTF8(e.getError());
        auto iscStatus = IscRandomStatus(error_message);
        throw FbException(status, iscStatus);
    }

    void FTSPreparedIndex::optimize(Firebird::ThrowStatusWrapper* status)
    try {
        m_indexWriter->optimize();
    } catch (const LuceneException& e) {
        const std::string error_message = StringUtils::toUTF8(e.getError());
        auto iscStatus = IscRandomStatus(error_message);
        throw FbException(status, iscStatus);
    }

    void FTSPreparedIndex::commit(Firebird::ThrowStatusWrapper* status)
    try {
        m_indexWriter->commit();
    } catch (const LuceneException& e) {
        const std::string error_message = StringUtils::toUTF8(e.getError());
        auto iscStatus = IscRandomStatus(error_message);
        throw FbException(status, iscStatus);
    }

    void FTSPreparedIndex::close(Firebird::ThrowStatusWrapper* status)
    try {
        m_indexWriter->close();
    } catch (const LuceneException& e) {
        const std::string error_message = StringUtils::toUTF8(e.getError());
        auto iscStatus = IscRandomStatus(error_message);
        throw FbException(status, iscStatus);
    }

    Lucene::DocumentPtr FTSPreparedIndex::makeDocument(
        Firebird::ThrowStatusWrapper* status,
        Firebird::IAttachment* att,
        Firebird::ITransaction* tra)
    {
        bool emptyFlag = true;
        auto doc = newLucene<Document>();

        for (const auto& field : m_fields) {
            const std::string value = field.getStringValue(status, att, tra, m_outputBuffer.data());
            Lucene::String unicodeValue = StringUtils::toUnicode(value);
            // add field to document
            if (field.ftsKey) {
                auto luceneField = newLucene<Field>(field.ftsFieldName, unicodeValue, Field::STORE_YES, Field::INDEX_NOT_ANALYZED);
                doc->add(luceneField);
            } else {
                auto luceneField = newLucene<Field>(field.ftsFieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED);
                if (!field.ftsBoostNull) {
                    luceneField->setBoost(field.ftsBoost);
                }
                doc->add(luceneField);
                emptyFlag = emptyFlag && unicodeValue.empty();
            }
        }
        if (emptyFlag) { 
            doc.reset();
        }
        return doc;
    }

    void FTSPreparedIndex::rebuild(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra
    )
    {
        AutoRelease<IResultSet> rs(m_stmtExtractRecord->openCursor(
            status,
            tra,
            nullptr,
            nullptr,
            m_outMetaExtractRecord,
            0
        ));

        while (rs->fetchNext(status, m_outputBuffer.data()) == IStatus::RESULT_OK) {
            auto doc = makeDocument(status, att, tra);
            if (doc) {
                m_indexWriter->addDocument(doc);
            }
        }
        rs->close(status);
        rs.release();
    }

    void FTSPreparedIndex::updateIndexById(
        Firebird::ThrowStatusWrapper* status,
        Firebird::IAttachment* att,
        Firebird::ITransaction* tra,
        ISC_INT64 id,
        std::string_view changeType
    )
    {
        std::string sId = std::to_string(id);
        Lucene::String unicodeKeyValue = StringUtils::toUnicode(sId);

        if (changeType == "D") {
            TermPtr term = newLucene<Term>(m_unicodeKeyFieldName, unicodeKeyValue);
            m_indexWriter->deleteDocuments(term);
            return;
        }

        FB_MESSAGE(IDInput, Firebird::ThrowStatusWrapper,
            (FB_BIGINT, id)
        ) input(status, m_master);

        input->idNull = FB_FALSE;
        input->id = id;

        AutoRelease<IResultSet> rs(
            m_stmtExtractRecord->openCursor(
                status,
                tra,
                input.getMetadata(),
                input.getData(),
                m_outMetaExtractRecord,
                0
            )        
        );

        while (rs->fetchNext(status, m_outputBuffer.data()) == IStatus::RESULT_OK) {
            auto doc = makeDocument(status, att, tra);

            if ((changeType == "I") && doc) {
                m_indexWriter->addDocument(doc);
            }
            if (changeType == "U") {
                TermPtr term = newLucene<Term>(m_ftsIndex.unicodeKeyFieldName, unicodeKeyValue);
                if (doc) {
                    m_indexWriter->updateDocument(term, doc);
                } else {
                    m_indexWriter->deleteDocuments(term);
                }
            }
        }
        rs->close(status);
        rs.release();
    } // void FTSPreparedIndex::updateIndexById()

    void FTSPreparedIndex::updateIndexByUuui(
        Firebird::ThrowStatusWrapper* status,
        Firebird::IAttachment* att,
        Firebird::ITransaction* tra,
        const unsigned char* uuid,
        ISC_USHORT uuidLength,
        std::string_view changeType)
    {
        std::string sUuid = binary_to_hex(uuid, uuidLength);
        Lucene::String unicodeKeyValue = StringUtils::toUnicode(sUuid);

        if (changeType == "D") {
            TermPtr term = newLucene<Term>(m_unicodeKeyFieldName, unicodeKeyValue);
            m_indexWriter->deleteDocuments(term);
            return;
        }

        FB_MESSAGE(UUIDInput, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(16, CS_BINARY), uuid))
        input(status, m_master);

        input->uuidNull = FB_FALSE;
        input->uuid.length = uuidLength;
        memcpy(input->uuid.str, uuid, uuidLength);

        AutoRelease<IResultSet> rs(
            m_stmtExtractRecord->openCursor(
                status,
                tra,
                input.getMetadata(),
                input.getData(),
                m_outMetaExtractRecord,
                0));

        while (rs->fetchNext(status, m_outputBuffer.data()) == IStatus::RESULT_OK) {
            auto doc = makeDocument(status, att, tra);

            if ((changeType == "I") && doc) {
                m_indexWriter->addDocument(doc);
            }
            if (changeType == "U") {
                TermPtr term = newLucene<Term>(m_ftsIndex.unicodeKeyFieldName, unicodeKeyValue);
                if (doc) {
                    m_indexWriter->updateDocument(term, doc);
                } else {
                    m_indexWriter->deleteDocuments(term);
                }
            }
        }
        rs->close(status);
        rs.release();
    }

    void FTSPreparedIndex::updateIndexByDbkey(
        Firebird::ThrowStatusWrapper* status,
        Firebird::IAttachment* att,
        Firebird::ITransaction* tra,
        const unsigned char* dbkey,
        ISC_USHORT dbkeyLength,
        std::string_view changeType)
    {
        std::string sDbkey = binary_to_hex(dbkey, dbkeyLength);
        Lucene::String unicodeKeyValue = StringUtils::toUnicode(sDbkey);

        if (changeType == "D") {
            TermPtr term = newLucene<Term>(m_unicodeKeyFieldName, unicodeKeyValue);
            m_indexWriter->deleteDocuments(term);
            return;
        }

        FB_MESSAGE(UUIDInput, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(8, CS_BINARY), dbkey))
        input(status, m_master);

        input->dbkeyNull = FB_FALSE;
        input->dbkey.length = dbkeyLength;
        memcpy(input->dbkey.str, dbkey, dbkeyLength);

        AutoRelease<IResultSet> rs(
            m_stmtExtractRecord->openCursor(
                status,
                tra,
                input.getMetadata(),
                input.getData(),
                m_outMetaExtractRecord,
                0));

        while (rs->fetchNext(status, m_outputBuffer.data()) == IStatus::RESULT_OK) {
            auto doc = makeDocument(status, att, tra);

            if ((changeType == "I") && doc) {
                m_indexWriter->addDocument(doc);
            }
            if (changeType == "U") {
                TermPtr term = newLucene<Term>(m_ftsIndex.unicodeKeyFieldName, unicodeKeyValue);
                if (doc) {
                    m_indexWriter->updateDocument(term, doc);
                } else {
                    m_indexWriter->deleteDocuments(term);
                }
            }
        }
        rs->close(status);
        rs.release();
    }

}