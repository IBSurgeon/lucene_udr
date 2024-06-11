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

#include "LuceneUdr.h"
#include "LuceneHeaders.h"
#include <list>
#include <string>


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
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_get_analyzers{ nullptr };
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_has_analyzer{ nullptr };
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_get_stopwords{ nullptr };
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_insert_stopword{ nullptr };
        Firebird::AutoRelease<Firebird::IStatement> m_stmt_delete_stopword{ nullptr };

    public:
        AnalyzerRepository() = delete;
        AnalyzerRepository(AnalyzerRepository&&) = default;
        explicit AnalyzerRepository(Firebird::IMaster* master);

        ~AnalyzerRepository();


        Lucene::AnalyzerPtr createAnalyzer (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            const std::string& analyzerName
        );

        LuceneUDR::AnalyzerInfo getAnalyzerInfo (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            const std::string& analyzerName
        );

        std::list<LuceneUDR::AnalyzerInfo> getAnalyzerInfos (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect
        );

        bool hasAnalyzer (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            const std::string& analyzerName
        );

        void addAnalyzer (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            const std::string& analyzerName,
            const std::string& baseAnalyzer,
            ISC_QUAD* description
        );

        void deleteAnalyzer(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            const std::string& analyzerName
        );

        const Lucene::HashSet<Lucene::String> getStopWords (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            const std::string& analyzerName
        );

        void addStopWord (
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            const std::string& analyzerName,
            const std::string& stopWord
        );

        void deleteStopWord(
            Firebird::ThrowStatusWrapper* status,
            Firebird::IAttachment* att,
            Firebird::ITransaction* tra,
            unsigned int sqlDialect,
            const std::string& analyzerName,
            const std::string& stopWord
        );
    };
}

#endif // LUCENE_ANALYZERS_H