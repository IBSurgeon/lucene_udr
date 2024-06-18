#ifndef LUCENE_ANALYZER_FACTORY_H
#define LUCENE_ANALYZER_FACTORY_H

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

#include "LuceneUdr.h"
#include <map>
#include <list>
#include <unordered_set>
#include <string>
#include <string_view>
#include "LuceneHeaders.h"

namespace LuceneUDR 
{

    struct ci_less
    {
        using is_transparent = void;

        // case-independent (ci) compare_less binary function
        struct nocase_compare
        {
            bool operator() (const unsigned char& c1, const unsigned char& c2) const {
                return toupper(c1) < toupper(c2);
            }
        };

        bool operator() (std::string_view s1, std::string_view s2) const {
            return std::lexicographical_compare(
                s1.begin(), s1.end(),   // source range
                s2.begin(), s2.end(),   // dest range
                nocase_compare()                // comparison
            );
        }
    };

    constexpr const char* DEFAULT_ANALYZER_NAME = "STANDARD";

    struct AnalyzerInfo
    {
        AnalyzerInfo() = default;
        AnalyzerInfo(std::string_view analyzerName_, std::string_view baseAnalyzer_, bool stopWordsSupported_, bool systemFlag_)
            : analyzerName(analyzerName_)
            , baseAnalyzer(baseAnalyzer_)
            , stopWordsSupported(stopWordsSupported_)
            , systemFlag(systemFlag_)
        {}

        std::string analyzerName;
        std::string baseAnalyzer;
        bool stopWordsSupported;
        bool systemFlag;
    };

    class LuceneAnalyzerFactory final {
    private:
        struct AnalyzerFactory
        {
            std::function<Lucene::AnalyzerPtr()> simpleFactory;
            std::function<Lucene::AnalyzerPtr(const Lucene::HashSet<Lucene::String>)> extFactory;
            std::function<Lucene::HashSet<Lucene::String>()> getStopWords;
            bool stopWordsSupported;
        };

        std::map<std::string, AnalyzerFactory, ci_less> m_factories;

    public:

        LuceneAnalyzerFactory();

        ~LuceneAnalyzerFactory();

        bool hasAnalyzer(std::string_view analyzerName) const;

        bool isStopWordsSupported(std::string_view analyzerName) const;

        Lucene::AnalyzerPtr createAnalyzer(Firebird::ThrowStatusWrapper* status, std::string_view analyzerName) const;

        Lucene::AnalyzerPtr createAnalyzer(Firebird::ThrowStatusWrapper* status, std::string_view analyzerName, const Lucene::HashSet<Lucene::String> stopWords) const;

        std::unordered_set<std::string> getAnalyzerNames() const;

        AnalyzerInfo getAnalyzerInfo(Firebird::ThrowStatusWrapper* status, std::string_view analyzerName) const;

        std::list<AnalyzerInfo> getAnalyzerInfos() const;

        Lucene::HashSet<Lucene::String> getAnalyzerStopWords(Firebird::ThrowStatusWrapper* status, std::string_view analyzerName) const;

    };

}

#endif	// LUCENE_ANALYZER_FACTORY_H