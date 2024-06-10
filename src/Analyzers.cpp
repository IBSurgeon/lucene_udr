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
#include "LuceneAnalyzerFactory.h"
#include "FBUtils.h"
#include "Utils.h"

using namespace LuceneUDR;
using namespace Firebird;
using namespace Lucene;

namespace FTSMetadata
{

    // SQL texts
    constexpr char SQL_ANALYZER_INFO[] = R"SQL(
SELECT
    A.FTS$ANALYZER_NAME
  , A.FTS$BASE_ANALYZER
  , A.FTS$DESCRIPTION
FROM FTS$ANALYZERS A
WHERE A.FTS$ANALYZER_NAME = ?
)SQL";

    constexpr char SQL_ANALYZER_INFOS[] = R"SQL(
SELECT
    A.FTS$ANALYZER_NAME
  , A.FTS$BASE_ANALYZER
  , A.FTS$DESCRIPTION
FROM FTS$ANALYZERS A
ORDER BY A.FTS$ANALYZER_NAME
)SQL";

    constexpr char SQL_ANALYZER_EXISTS[] = R"SQL(
SELECT COUNT(*) AS CNT
FROM FTS$ANALYZERS A
WHERE A.FTS$ANALYZER_NAME = ?
)SQL";

    constexpr char SQL_INSERT_ANALYZER[] = R"SQL(
INSERT INTO FTS$ANALYZERS (
    FTS$ANALYZER_NAME,
    FTS$BASE_ANALYZER,
    FTS$DESCRIPTION)
VALUES (
    ?,
    ?,
    ?)
)SQL";

    constexpr char SQL_DELETE_ANALYZER[] = R"SQL(
DELETE FROM FTS$ANALYZERS A
WHERE A.FTS$ANALYZER_NAME = ?
)SQL";

    constexpr char SQL_STOP_WORDS[] = R"SQL(
SELECT
    W.FTS$WORD
FROM FTS$STOP_WORDS W
WHERE W.FTS$ANALYZER_NAME = ?
)SQL";

    constexpr char SQL_INSERT_STOP_WORD[] = R"SQL(
EXECUTE BLOCK (
    FTS$ANALYZER_NAME VARCHAR(63) CHARACTER SET UTF8 = ?,
    FTS$WORD          VARCHAR(63) CHARACTER SET UTF8 = ?)
AS
BEGIN
  INSERT INTO FTS$STOP_WORDS (
      FTS$ANALYZER_NAME,
      FTS$WORD)
  VALUES (
      :FTS$ANALYZER_NAME,
      LOWER(:FTS$WORD));

  WHEN GDSCODE UNIQUE_KEY_VIOLATION DO
    EXCEPTION FTS$EXCEPTION 'Stop word "' || FTS$WORD || '" already exists for analyzer "' || FTS$ANALYZER_NAME || '"';
END
)SQL";

    constexpr char SQL_DELETE_STOP_WORD[] = R"SQL(
DELETE FROM FTS$STOP_WORDS
WHERE FTS$ANALYZER_NAME = ? AND FTS$WORD = ?
)SQL";

    AnalyzerRepository::AnalyzerRepository(IMaster* const master)
        : m_master(master)
        , m_analyzerFactory(new LuceneAnalyzerFactory())
    {}

    AnalyzerRepository::~AnalyzerRepository()
    {
        delete m_analyzerFactory;
    }

    AnalyzerPtr AnalyzerRepository::createAnalyzer(
        ThrowStatusWrapper* const status,
        IAttachment* const att,
        ITransaction* const tra,
        unsigned int sqlDialect,
        const std::string& analyzerName
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
        ThrowStatusWrapper* const status,
        IAttachment* const att,
        ITransaction* const tra,
        unsigned int sqlDialect,
        const std::string& analyzerName
    )
    {
        if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
            return m_analyzerFactory->getAnalyzerInfo(status, analyzerName);
        }
        AnalyzerInfo info;

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
            throwException(status, R"(Analyzer "%s" not exists)", analyzerName.c_str());
        }
        if (result == IStatus::RESULT_OK) {
            info.analyzerName.assign(output->analyzerName.str, output->analyzerName.length);
            info.baseAnalyzer.assign(output->baseAnalyzer.str, output->baseAnalyzer.length);
            info.stopWordsSupported = m_analyzerFactory->isStopWordsSupported(info.baseAnalyzer);
            info.systemFlag = false;
        }

        return info;
    }

    std::list<AnalyzerInfo> AnalyzerRepository::getAnalyzerInfos(
        ThrowStatusWrapper* const status,
        IAttachment* const att,
        ITransaction* const tra,
        unsigned int sqlDialect
    )
    {
        auto infos = m_analyzerFactory->getAnalyzerInfos();

        FB_MESSAGE(Output, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
            (FB_INTL_VARCHAR(252, CS_UTF8), baseAnalyzer)
            (FB_BLOB, description)
        ) output(status, m_master);

        if (!m_stmt_get_analyzers.hasData()) {
            m_stmt_get_analyzers.reset(att->prepare(
                status,
                tra,
                0,
                SQL_ANALYZER_INFOS,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }

        AutoRelease<IResultSet> rs(m_stmt_get_analyzers->openCursor(
            status,
            tra,
            nullptr,
            nullptr,
            output.getMetadata(),
            0
        ));

        while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
            AnalyzerInfo info;
            info.analyzerName.assign(output->analyzerName.str, output->analyzerName.length);
            info.baseAnalyzer.assign(output->baseAnalyzer.str, output->baseAnalyzer.length);
            info.stopWordsSupported = m_analyzerFactory->isStopWordsSupported(info.baseAnalyzer);
            info.systemFlag = false;
            infos.push_back(info);
        }
        rs->close(status);
        rs.release();

        return infos;
    }

    bool AnalyzerRepository::hasAnalyzer (
        ThrowStatusWrapper* const status,
        IAttachment* const att,
        ITransaction* const tra,
        unsigned int sqlDialect,
        const std::string& analyzerName
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

    void AnalyzerRepository::addAnalyzer (
        ThrowStatusWrapper* status,
        IAttachment* att,
        ITransaction* tra,
        unsigned int sqlDialect,
        const std::string& analyzerName,
        const std::string& baseAnalyzer,
        ISC_QUAD* description
    )
    {
        if (hasAnalyzer(status, att, tra, sqlDialect, analyzerName)) {
            throwException(status, R"(Cannot create analyzer. Analyzer "%s" already exists)", analyzerName.c_str());
        }

        if (!m_analyzerFactory->hasAnalyzer(baseAnalyzer)) {
            throwException(status, R"(Cannot create analyzer. Base analyzer "%s" not exists or not system analyzer)", baseAnalyzer.c_str());
        }

        FB_MESSAGE(Input, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
            (FB_INTL_VARCHAR(252, CS_UTF8), baseAnalyzer)
            (FB_BLOB, description)
        ) input(status, m_master);

        input.clear();

        input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
        analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

        input->baseAnalyzer.length = static_cast<ISC_USHORT>(baseAnalyzer.length());
        baseAnalyzer.copy(input->baseAnalyzer.str, input->baseAnalyzer.length);

        if (description) {
            input->descriptionNull = false;
            input->description = *description;
        }
        else {
            input->descriptionNull = true;
        }

        att->execute(
            status,
            tra,
            0,
            SQL_INSERT_ANALYZER,
            sqlDialect,
            input.getMetadata(),
            input.getData(),
            nullptr,
            nullptr
        );
    }

    void AnalyzerRepository::deleteAnalyzer(
        ThrowStatusWrapper* const status,
        IAttachment* const att,
        ITransaction* const tra,
        unsigned int sqlDialect,
        const std::string& analyzerName
    )
    {
        if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
            throwException(status, R"(Cannot drop system analyzer "%s")", analyzerName.c_str());
        }
        if (!hasAnalyzer(status, att, tra, sqlDialect, analyzerName)) {
            throwException(status, R"(Cannot drop analyzer. Analyzer "%s" not exists)", analyzerName.c_str());
        }

        FB_MESSAGE(Input, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
        ) input(status, m_master);

        input.clear();

        input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
        analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

        att->execute(
            status,
            tra,
            0,
            SQL_DELETE_ANALYZER,
            sqlDialect,
            input.getMetadata(),
            input.getData(),
            nullptr,
            nullptr
        );
    }

    const HashSet<String> AnalyzerRepository::getStopWords(
        ThrowStatusWrapper* const status,
        IAttachment* const att,
        ITransaction* const tra,
        unsigned int sqlDialect,
        const std::string& analyzerName
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

    void AnalyzerRepository::addStopWord(
        ThrowStatusWrapper* const status,
        IAttachment* const att,
        ITransaction* const tra,
        unsigned int sqlDialect,
        const std::string& analyzerName,
        const std::string& stopWord
    )
    {
        if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
            throwException(status, R"(Cannot add stop word to system analyzer "%s")", analyzerName.c_str());
        }
        const auto info = getAnalyzerInfo(status, att, tra, sqlDialect, analyzerName);
        if (!info.stopWordsSupported) {
            throwException(status, R"(Cannot add stop word. Base analyzer "%s" not supported stop words)", info.baseAnalyzer.c_str());
        }

        FB_MESSAGE(Input, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
            (FB_INTL_VARCHAR(252, CS_UTF8), stopWord)
        ) input(status, m_master);

        input.clear();

        input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
        analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

        input->stopWord.length = static_cast<ISC_USHORT>(stopWord.length());
        stopWord.copy(input->stopWord.str, input->stopWord.length);

        if (input->stopWord.length == 0) {
            throwException(status, "Cannot add empty stop word");
        }

        if (!m_stmt_insert_stopword.hasData()) {
            m_stmt_insert_stopword.reset(att->prepare(
                status,
                tra,
                0,
                SQL_INSERT_STOP_WORD,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }

        m_stmt_insert_stopword->execute(
            status,
            tra,
            input.getMetadata(),
            input.getData(),
            nullptr,
            nullptr);
    }

    void AnalyzerRepository::deleteStopWord(
        ThrowStatusWrapper* const status,
        IAttachment* const att,
        ITransaction* const tra,
        unsigned int sqlDialect,
        const std::string& analyzerName,
        const std::string& stopWord
    )
    {
        if (m_analyzerFactory->hasAnalyzer(analyzerName)) {
            throwException(status, R"(Cannot delete stop word from system analyzer "%s")", analyzerName.c_str());
        }
        const auto info = getAnalyzerInfo(status, att, tra, sqlDialect, analyzerName);
        if (!info.stopWordsSupported) {
            throwException(status, R"(Cannot delete stop word. Base analyzer "%s" not supported stop words)", info.baseAnalyzer.c_str());
        }

        FB_MESSAGE(Input, ThrowStatusWrapper,
            (FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
            (FB_INTL_VARCHAR(252, CS_UTF8), stopWord)
        ) input(status, m_master);

        input.clear();

        input->analyzerName.length = static_cast<ISC_USHORT>(analyzerName.length());
        analyzerName.copy(input->analyzerName.str, input->analyzerName.length);

        input->stopWord.length = static_cast<ISC_USHORT>(stopWord.length());
        stopWord.copy(input->stopWord.str, input->stopWord.length);

        if (!m_stmt_delete_stopword.hasData()) {
            m_stmt_delete_stopword.reset(att->prepare(
                status,
                tra,
                0,
                SQL_DELETE_STOP_WORD,
                sqlDialect,
                IStatement::PREPARE_PREFETCH_METADATA
            ));
        }

        m_stmt_delete_stopword->execute(
            status,
            tra,
            input.getMetadata(),
            input.getData(),
            nullptr,
            nullptr);
    }

}