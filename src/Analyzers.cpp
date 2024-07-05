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

#include "Analyzers.h"

#include "FBUtils.h"
#include "LuceneAnalyzerFactory.h"

using namespace LuceneUDR;
using namespace Firebird;
using namespace Lucene;

namespace {
    // SQL texts
    constexpr const char* SQL_ANALYZER_INFO = R"SQL(
SELECT
    A.FTS$ANALYZER_NAME
  , A.FTS$BASE_ANALYZER
  , A.FTS$DESCRIPTION
FROM FTS$ANALYZERS A
WHERE A.FTS$ANALYZER_NAME = ?
)SQL";

    constexpr const char* SQL_ANALYZER_INFOS = R"SQL(
SELECT
    A.FTS$ANALYZER_NAME
  , A.FTS$BASE_ANALYZER
  , A.FTS$DESCRIPTION
FROM FTS$ANALYZERS A
ORDER BY A.FTS$ANALYZER_NAME
)SQL";

    constexpr const char* SQL_ANALYZER_EXISTS = R"SQL(
SELECT COUNT(*) AS CNT
FROM FTS$ANALYZERS A
WHERE A.FTS$ANALYZER_NAME = ?
)SQL";


    constexpr const char* SQL_STOP_WORDS = R"SQL(
SELECT
    W.FTS$WORD
FROM FTS$STOP_WORDS W
WHERE W.FTS$ANALYZER_NAME = ?
)SQL";

}

namespace FTSMetadata
{

    AnalyzerRepository::AnalyzerRepository(IMaster* master)
        : m_master(master)
        , m_analyzerFactory(new LuceneAnalyzerFactory())
    {}

    AnalyzerRepository::~AnalyzerRepository()
    {
        delete m_analyzerFactory;
    }

    AnalyzerPtr AnalyzerRepository::createAnalyzer(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view analyzerName
    )
    {
        if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
            return m_analyzerFactory->createAnalyzer(status, analyzerName);
        }
        const auto info = getAnalyzerInfo(status, att, tra, sqlDialect, analyzerName);
        if (!m_analyzerFactory->hasAnalyzer(info.baseAnalyzer)) {
            throwException(status, R"(Base analyzer "%s" not exists)", info.baseAnalyzer.c_str());
        }
        const auto stopWords = getStopWords(status, att, tra, sqlDialect, analyzerName);
        return m_analyzerFactory->createAnalyzer(status, info.baseAnalyzer, stopWords);
    }

    AnalyzerInfo AnalyzerRepository::getAnalyzerInfo(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view analyzerName
    )
    {
        if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
            return m_analyzerFactory->getAnalyzerInfo(status, analyzerName);
        }
        

        FB_MESSAGE(Input, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
        ) input(status, m_master);

        FB_MESSAGE(Output, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
            (FB_INTL_VARCHAR(252, CS_UTF8), baseAnalyzer)
            (FB_BLOB, description)
        ) output(status, m_master);

        input.clear();
        input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
        analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

        if (!m_stmt_get_analyzer.hasData()) {
            m_stmt_get_analyzer.reset(att->prepare(
                status,
                tra,
                0,
                SQL_ANALYZER_INFO,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_get_analyzer->openCursor(
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
            std::string sAnalyzerName{ analyzerName };
            throwException(status, R"(Analyzer "%s" not exists)", sAnalyzerName.c_str());
        }

        return {
            std::string_view(output->analyzerName.str, output->analyzerName.length),
            std::string_view(output->baseAnalyzer.str, output->baseAnalyzer.length),
            m_analyzerFactory->isStopWordsSupported(std::string_view(output->baseAnalyzer.str, output->baseAnalyzer.length)),
            false
        };
    }

    bool AnalyzerRepository::hasAnalyzer (
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view analyzerName
    )
    {
        if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
            return true;
        }

        FB_MESSAGE(Input, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
        ) input(status, m_master);

        FB_MESSAGE(Output, ThrowStatusWrapper,
            (FB_INTEGER, cnt)
        ) output(status, m_master);

        input.clear();
        input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
        analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

        if (!m_stmt_has_analyzer.hasData()) {
            m_stmt_has_analyzer.reset(att->prepare(
                status,
                tra,
                0,
                SQL_ANALYZER_EXISTS,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_has_analyzer->openCursor(
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

    HashSet<String> AnalyzerRepository::getStopWords(
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        std::string_view analyzerName
    )
    {
        if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
            return m_analyzerFactory->getAnalyzerStopWords(status, analyzerName);
        }

        FB_MESSAGE(Input, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
        ) input(status, m_master);

        FB_MESSAGE(Output, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), stopWord)
        ) output(status, m_master);

        input.clear();

        input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
        analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

        auto stopWords = HashSet<String>::newInstance();
        
        if (!m_stmt_get_stopwords.hasData()) {
            m_stmt_get_stopwords.reset(att->prepare(
                status,
                tra,
                0,
                SQL_STOP_WORDS,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }
        
        AutoRelease<IResultSet> rs(m_stmt_get_stopwords->openCursor(
            status,
            tra,
            input.getMetadata(),
            input.getData(),
            output.getMetadata(),
            0
        ));
        
        while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
            const std::string stopWord(output->stopWord.str, output->stopWord.length);
            const String uStopWord = StringUtils::toUnicode(stopWord);
            stopWords.add(uStopWord);
        }
        rs->close(status);
        rs.release();
        
        return stopWords;
    }

}