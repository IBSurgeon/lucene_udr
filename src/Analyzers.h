#ifndef LUCENE_ANALYZERS_H
#define LUCENE_ANALYZERS_H

/**
 *  Utilities for getting and managing metadata for analyzers.
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

#include <string>
#include <string_view>

#include "LuceneHeaders.h"
#include "LuceneUdr.h"


namespace LuceneUDR 
{
    class LuceneAnalyzerFactory;
    struct AnalyzerInfo;
}

namespace FTSMetadata
{
    class AnalyzerRepository final
    {
    private:
        Firebird::IMaster* m_master = nullptr;
        LuceneUDR::LuceneAnalyzerFactory* m_analyzerFactory = nullptr;

        // prepared statements
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_get_analyzer{ nullptr };
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_has_analyzer{ nullptr };
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_get_stopwords{ nullptr };

    public:
        AnalyzerRepository() = delete;
        explicit AnalyzerRepository(Firebird::IMaster* master);

        // non-copyable
        AnalyzerRepository(const AnalyzerRepository& rhs) = delete;
        AnalyzerRepository& operator=(const AnalyzerRepository& rhs) = delete;

        // movable
        AnalyzerRepository(AnalyzerRepository&&) = default;
        AnalyzerRepository& operator=(AnalyzerRepository&& rhs) noexcept = default;
        

        ~AnalyzerRepository();


        Lucene::AnalyzerPtr createAnalyzer (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view analyzerName
        );

        LuceneUDR::AnalyzerInfo getAnalyzerInfo (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view analyzerName
        );

        bool hasAnalyzer (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view analyzerName
        );

        Lucene::HashSet<Lucene::String> getStopWords (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            std::string_view analyzerName
        );
    };
}

#endif // LUCENE_ANALYZERS_H