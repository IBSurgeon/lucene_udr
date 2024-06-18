/**
 *  Factory for creating Lucene analyzers.
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

#include "LuceneAnalyzerFactory.h"
#include "WhitespaceAnalyzer.h"
#include "ArabicAnalyzer.h"
#include "BrazilianAnalyzer.h"
#include "CJKAnalyzer.h"
#include "ChineseAnalyzer.h"
#include "CzechAnalyzer.h"
#include "DutchAnalyzer.h"
#include "FrenchAnalyzer.h"
#include "GermanAnalyzer.h"
#include "GreekAnalyzer.h"
#include "PersianAnalyzer.h"
#include "RussianAnalyzer.h"
#include "EnglishAnalyzer.h"
#include "SnowballAnalyzer.h"
#include "FBUtils.h"
#include <functional>
#include <stdexcept>

using namespace Firebird;
using namespace Lucene;

namespace LuceneUDR 
{

    LuceneAnalyzerFactory::LuceneAnalyzerFactory()
        : m_factories
        {
            {
                "STANDARD",
                {
                    []() -> AnalyzerPtr { return newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return StopAnalyzer::ENGLISH_STOP_WORDS_SET(); },
                    true
                }
            },
            {
                "SIMPLE",
                {
                    []() -> AnalyzerPtr { return newLucene<SimpleAnalyzer>(); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return nullptr; },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    false
                }
            },
            {
                "WHITESPACE",
                {
                    []() -> AnalyzerPtr { return newLucene<WhitespaceAnalyzer>(); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return nullptr; },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    false
                }
            },
            {
                "KEYWORD",
                {
                    []() -> AnalyzerPtr { return newLucene<KeywordAnalyzer>(); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return nullptr; },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    false
                }
            },
            {
                "STOP",
                {
                    []() -> AnalyzerPtr { return newLucene<StopAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<StopAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return StopAnalyzer::ENGLISH_STOP_WORDS_SET(); },
                    true
                }
            },
            {
                "ARABIC",
                {
                    []() -> AnalyzerPtr { return newLucene<ArabicAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<ArabicAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return ArabicAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "BRAZILIAN",
                {
                    []() -> AnalyzerPtr { return newLucene<BrazilianAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<BrazilianAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return BrazilianAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "CHINESE",
                {
                    []() -> AnalyzerPtr { return newLucene<ChineseAnalyzer>(); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return nullptr; },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    false
                }
            },
            {
                "CJK",
                {
                    []() -> AnalyzerPtr { return newLucene<CJKAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<CJKAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return CJKAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "CZECH",
                {
                    []() -> AnalyzerPtr { return newLucene<CzechAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<CzechAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return CzechAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "DUTCH",
                {
                    []() -> AnalyzerPtr { return newLucene<DutchAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<DutchAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return DutchAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "ENGLISH",
                {
                    []() -> AnalyzerPtr { return newLucene<EnglishAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<EnglishAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return EnglishAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "FRENCH",
                {
                    []() -> AnalyzerPtr { return newLucene<FrenchAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<FrenchAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return FrenchAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "GERMAN",
                {
                    []() -> AnalyzerPtr { return newLucene<GermanAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<GermanAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return GermanAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "GREEK",
                {
                    []() -> AnalyzerPtr { return newLucene<GreekAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<GreekAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return GreekAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "PERSIAN",
                {
                    []() -> AnalyzerPtr { return newLucene<PersianAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<PersianAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return PersianAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "RUSSIAN",
                {
                    []() -> AnalyzerPtr { return newLucene<RussianAnalyzer>(LuceneVersion::LUCENE_CURRENT); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<RussianAnalyzer>(LuceneVersion::LUCENE_CURRENT, stopWords); },
                    []() -> HashSet<String> { return RussianAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "SNOWBALL(DANISH)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"danish"); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"danish", stopWords); },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    true
                }
            },
            {
                "SNOWBALL(DUTCH)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"dutch", DutchAnalyzer::getDefaultStopSet()); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"dutch", stopWords); },
                    []() -> HashSet<String> { return DutchAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "SNOWBALL(ENGLISH)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"english", StopAnalyzer::ENGLISH_STOP_WORDS_SET()); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"english", stopWords); },
                    []() -> HashSet<String> { return StopAnalyzer::ENGLISH_STOP_WORDS_SET(); },
                    true
                }
            },
            {
                "SNOWBALL(FINNISH)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"finnish"); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"finnish", stopWords); },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    true
                }
            },
            {
                "SNOWBALL(FRENCH)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"french", FrenchAnalyzer::getDefaultStopSet()); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"french", stopWords); },
                    []() -> HashSet<String> { return FrenchAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "SNOWBALL(GERMAN)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"german", GermanAnalyzer::getDefaultStopSet()); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"german", stopWords); },
                    []() -> HashSet<String> { return GermanAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "SNOWBALL(HUNGARIAN)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"hungarian"); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"hungarian", stopWords); },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    true
                }
            },
            {
                "SNOWBALL(ITALIAN)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"italian"); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"italian", stopWords); },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    true
                }
            },
            {
                "SNOWBALL(NORWEGIAN)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"norwegian"); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"norwegian", stopWords); },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    true
                }
            },
            {
                "SNOWBALL(PORTER)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"porter", StopAnalyzer::ENGLISH_STOP_WORDS_SET()); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"porter", stopWords); },
                    []() -> HashSet<String> { return StopAnalyzer::ENGLISH_STOP_WORDS_SET(); },
                    true
                }
            },
            {
                "SNOWBALL(PORTUGUESE)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"portuguese"); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"portuguese", stopWords); },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    true
                }
            },
            {
                "SNOWBALL(ROMANIAN)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"romanian"); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"romanian", stopWords); },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    true
                }
            },
            {
                "SNOWBALL(RUSSIAN)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"russian", RussianAnalyzer::getDefaultStopSet()); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"russian", stopWords); },
                    []() -> HashSet<String> { return RussianAnalyzer::getDefaultStopSet(); },
                    true
                }
            },
            {
                "SNOWBALL(SPANISH)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"spanish"); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"spanish", stopWords); },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    true
                }
            },
            {
                "SNOWBALL(SWEDISH)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"swedish"); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"swedish", stopWords); },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    true
                }
            },
            {
                "SNOWBALL(TURKISH)",
                {
                    []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"turkish"); },
                    [](const HashSet<String> stopWords) -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"turkish", stopWords); },
                    []() -> HashSet<String> { return HashSet<String>::newInstance(); },
                    true
                }
            }
        }
    {

    }

    LuceneAnalyzerFactory::~LuceneAnalyzerFactory() = default;

    bool LuceneAnalyzerFactory::hasAnalyzer(std::string_view analyzerName) const
    {
        return (m_factories.find(analyzerName) != m_factories.end());
    }

    bool LuceneAnalyzerFactory::isStopWordsSupported(std::string_view analyzerName) const
    {
        auto pFactory = m_factories.find(analyzerName);
        if (pFactory == m_factories.end()) {
            return false;
        }
        auto factory = pFactory->second;
        return factory.stopWordsSupported;
    }

    AnalyzerPtr LuceneAnalyzerFactory::createAnalyzer(ThrowStatusWrapper* status, std::string_view analyzerName) const
    {
        auto pFactory = m_factories.find(analyzerName);
        if (pFactory == m_factories.end()) {
            std::string sAnalyzerName{ analyzerName };
            throwException(status, R"(Analyzer "%s" not found.)", sAnalyzerName.c_str());
        }
        auto factory = pFactory->second;
        return factory.simpleFactory();
    }

    AnalyzerPtr LuceneAnalyzerFactory::createAnalyzer(ThrowStatusWrapper* status, std::string_view analyzerName, const HashSet<String> stopWords) const
    {
        auto pFactory = m_factories.find(analyzerName);
        if (pFactory == m_factories.end()) {
            std::string sAnalyzerName{ analyzerName };
            throwException(status, R"(Analyzer "%s" not found.)", sAnalyzerName.c_str());
        }
        auto factory = pFactory->second;
        return factory.extFactory(stopWords);
    }

    std::unordered_set<std::string> LuceneAnalyzerFactory::getAnalyzerNames() const
    {
        std::unordered_set<std::string> names;
        for (auto&& pFactory : m_factories) {
            names.insert(pFactory.first);
        }
        return names;
    }

    AnalyzerInfo LuceneAnalyzerFactory::getAnalyzerInfo(ThrowStatusWrapper* status, std::string_view analyzerName) const
    {
        auto pFactory = m_factories.find(analyzerName);
        if (pFactory == m_factories.end()) {
            std::string sAnalyzerName{ analyzerName };
            throwException(status, R"(Analyzer "%s" not found.)", sAnalyzerName.c_str());
        }
        auto factory = pFactory->second;
        return { analyzerName, "", factory.stopWordsSupported, true };
    }

    std::list<AnalyzerInfo> LuceneAnalyzerFactory::getAnalyzerInfos() const
    {
        std::list<AnalyzerInfo> infos;
        for (const auto& pFactory : m_factories) {
            infos.emplace_back(pFactory.first, "", pFactory.second.stopWordsSupported, true);
        }
        return infos;
    }

    HashSet<String> LuceneAnalyzerFactory::getAnalyzerStopWords(ThrowStatusWrapper* status, std::string_view analyzerName) const
    {
        auto pFactory = m_factories.find(analyzerName);
        if (pFactory == m_factories.end()) {
            std::string sAnalyzerName{ analyzerName };
            throwException(status, R"(Analyzer "%s" not found.)", sAnalyzerName.c_str());
        }
        auto factory = pFactory->second;
        return factory.getStopWords();
    }
}