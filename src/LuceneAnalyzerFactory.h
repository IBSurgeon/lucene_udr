#ifndef LUCENE_ANALYZER_FACTORY_H
#define LUCENE_ANALYZER_FACTORY_H

/**
 *  Factory for creating Lucene analyzers.
 *
 *  The original code was created by Simonov Denis
 *  for the open source Lucene UDR full-text search library for Firebird DBMS.
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
#include <string>
#include <functional>
#include <stdexcept>
#include "LuceneHeaders.h"
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

using namespace std;
using namespace Lucene;

namespace LuceneUDR {

	struct ci_more
	{
		// case-independent (ci) compare_more binary function
		struct nocase_compare
		{
			bool operator() (const unsigned char& c1, const unsigned char& c2) const {
				return toupper(c1) < toupper(c2);
			}
		};

		bool operator() (const string& s1, const string& s2) const {
			return lexicographical_compare (
				s1.begin(), s1.end(),   // source range
				s2.begin(), s2.end(),   // dest range
				nocase_compare()        // comparison
			);     
		}
	};

	static const string DEFAULT_ANALYZER_NAME = "STANDARD";

	class LuceneAnalyzerFactory {
	private:
		map<string, function<AnalyzerPtr()>, ci_more> factories;
	public:

		LuceneAnalyzerFactory()
			: factories()
		{
			factories.insert(
				{
					{ "STANDARD", []() -> AnalyzerPtr { return newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "SIMPLE", []() -> AnalyzerPtr { return newLucene<SimpleAnalyzer>(); } },
					{ "WHITESPACE", []() -> AnalyzerPtr { return newLucene<WhitespaceAnalyzer>(); } },
					{ "KEYWORD", []() -> AnalyzerPtr { return newLucene<KeywordAnalyzer>(); } },
					{ "STOP", []() -> AnalyzerPtr { return newLucene<StopAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "ARABIC", []() -> AnalyzerPtr { return newLucene<ArabicAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "BRAZILIAN", []() -> AnalyzerPtr { return newLucene<BrazilianAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "CHINESE", []() -> AnalyzerPtr { return newLucene<ChineseAnalyzer>(); } },
					{ "CJK", []() -> AnalyzerPtr { return newLucene<CJKAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "CZECH", []() -> AnalyzerPtr { return newLucene<CzechAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "DUTCH", []() -> AnalyzerPtr { return newLucene<DutchAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "ENGLISH", []() -> AnalyzerPtr { return newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "FRENCH", []() -> AnalyzerPtr { return newLucene<FrenchAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "GERMAN", []() -> AnalyzerPtr { return newLucene<GermanAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "GREEK", []() -> AnalyzerPtr { return newLucene<GreekAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "PERSIAN", []() -> AnalyzerPtr { return newLucene<PersianAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "RUSSIAN", []() -> AnalyzerPtr { return newLucene<RussianAnalyzer>(LuceneVersion::LUCENE_CURRENT); } }
				}
			);
		}

		bool hasAnalyzer(const string &analyzerName)
		{
			return (factories.find(analyzerName) != factories.end());
		}

		AnalyzerPtr createAnalyzer(ThrowStatusWrapper* status, const string &analyzerName)
		{
			auto pFactory = factories.find(analyzerName);
			if (pFactory == factories.end()) {
				string error_message = string_format(R"(Analyzer "%s" not found.)"s, analyzerName);
				throwException(status, error_message.c_str());
			}
			auto factory = pFactory->second;
			return factory();
		}

		list<string> getAnalyzerNames()
		{
			list<string> names;
			for (const auto& pFactory : factories) {
				names.push_back(pFactory.first);
			}
			return names;
		}

	};

}

#endif	// LUCENE_ANALYZER_FACTORY_H