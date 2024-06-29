#pragma once

#include "LuceneUdr.h"
#include "LuceneHeaders.h"
#include "FBFieldInfo.h"
#include "FTSIndex.h"

#include <filesystem>

namespace LuceneUDR
{
	class FTSPreparedIndex final
	{
    public:
        FTSPreparedIndex() = default;

        FTSPreparedIndex(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IMaster* master,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            FTSMetadata::FTSIndex&& ftsIndex,
            const std::filesystem::path& ftsDirectoryPath,
            bool whereKey);

        void rebuild(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra
        );

        void updateIndexById(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            ISC_INT64 id,
            std::string_view changeType
        );

        void deleteAll(Firebird::ThrowStatusWrapper* status);
        void optimize(Firebird::ThrowStatusWrapper* status);
        void commit(Firebird::ThrowStatusWrapper* status);
        void close(Firebird::ThrowStatusWrapper* status);


        Lucene::IndexWriterPtr getIndexWriter() { 
            return m_indexWriter;
        }

    private:
        Lucene::DocumentPtr makeDocument(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra
        );
    private:
        Firebird::IMaster* m_master { nullptr };
        FTSMetadata::FTSIndex m_ftsIndex;
        FTSMetadata::FbFieldsInfo m_fields;
        FTSMetadata::FbFieldsInfo m_params;
        std::filesystem::path m_indexDirectoryPath;
        Firebird::AutoRelease<Firebird::IStatement> m_stmtExtractRecord;
        Firebird::AutoRelease<Firebird::IMessageMetadata> m_inMetaExtractRecord;
        Firebird::AutoRelease<Firebird::IMessageMetadata> m_outMetaExtractRecord;
        std::vector<unsigned char> m_outputBuffer;
        Lucene::IndexWriterPtr m_indexWriter;
        Lucene::String m_unicodeKeyFieldName; 
	};

    FTSPreparedIndex prepareFtsIndex(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IMaster* master,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            FTSMetadata::FTSIndex&& ftsIndex,
            const std::filesystem::path& ftsDirectoryPath,
            bool whereKey = false   
    );

}
