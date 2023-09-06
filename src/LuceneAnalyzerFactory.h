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
#include "LuceneHeaders.h"

namespace LuceneUDR 
{

    struct ci_more
    {
        // case-independent (ci) compare_more binary function
        struct nocase_compare
        {
            bool operator() (const unsigned char& c1, const unsigned char& c2) const {
                return toupper(c1) < toupper(c2);
            }
        };

        bool operator() (const std::string& s1, const std::string& s2) const {
            return std::lexicographical_compare(
                s1.begin(), s1.end(),   // source range
                s2.begin(), s2.end(),   // dest range
                nocase_compare()        // comparison
            );
        }
    };

    constexpr char DEFAULT_ANALYZER_NAME[] = "STANDARD";

    struct AnalyzerInfo
    {
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
            std::function<const Lucene::HashSet<Lucene::String>()> getStopWords;
            bool stopWordsSupported;
        };

        std::map<std::string, AnalyzerFactory, ci_more> m_factories;

    public:

        LuceneAnalyzerFactory();

        ~LuceneAnalyzerFactory();

        bool hasAnalyzer(const std::string& analyzerName);

        bool isStopWordsSupported(const std::string& analyzerName);

        Lucene::AnalyzerPtr createAnalyzer(Firebird::ThrowStatusWrapper* status, const std::string& analyzerName);

        Lucene::AnalyzerPtr createAnalyzer(Firebird::ThrowStatusWrapper* status, const std::string& analyzerName, const Lucene::HashSet<Lucene::String> stopWords);

        std::unordered_set<std::string> getAnalyzerNames();

        const AnalyzerInfo getAnalyzerInfo(Firebird::ThrowStatusWrapper* status, const std::string& analyzerName);

        std::list<AnalyzerInfo> getAnalyzerInfos();

        const Lucene::HashSet<Lucene::String> getAnalyzerStopWords(Firebird::ThrowStatusWrapper* status, const std::string& analyzerName);

    };

}

#endif	// LUCENE_ANALYZER_FACTORY_H