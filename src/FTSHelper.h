#pragma once

#include "LuceneUdr.h"
#include "FBFieldInfo.h"
#include "FTSIndex.h"

#include <fileinfo>

namespace LuceneUDR
{
	class FTSPreparedIndex final
	{
    public:
        FTSPreparedIndex() = default;

        FTSPreparedIndex(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            FTSMetadata::FTSIndex&& ftsIndex,
            bool whereKey);

        void rebuild(
            Firebird::ThrowStatusWrapper* status,
            const std::filesystem::path& ftsDirectoryPath,
            Firebird::ITransaction* tra
        );
    private:
        FTSMetadata::FTSIndex m_ftsIndex;
        FTSMetadata::FbFieldsInfo m_fields;
        Firebird::AutoRelease<Firebird::IStatement> m_stmtExtractRecord;
        Firebird::AutoRelease<Firebird::IMessageMetadata> m_inMetaExtractRecord;
        Firebird::AutoRelease<Firebird::IMessageMetadata> m_outMetaExtractRecord;
	};
}
