#ifndef LUCENE_ANALYZER_FACTORY_H
#define LUCENE_ANALYZER_FACTORY_H

#include <map>
#include <list>
#include <string>
#include <functional>
#include <stdexcept>
#include "lucene++\LuceneHeaders.h"
#include "lucene++\ArabicAnalyzer.h"
#include "lucene++\BrazilianAnalyzer.h"
#include "lucene++\CJKAnalyzer.h"
#include "lucene++\ChineseAnalyzer.h"
#include "lucene++\CzechAnalyzer.h"
#include "lucene++\DutchAnalyzer.h"
#include "lucene++\FrenchAnalyzer.h"
#include "lucene++\GermanAnalyzer.h"
#include "lucene++\GreekAnalyzer.h"
#include "lucene++\PersianAnalyzer.h"
#include "lucene++\RussianAnalyzer.h"

using namespace std;
using namespace Lucene;

namespace LuceneFTS {

	struct ci_more
	{
		// case-independent (ci) compare_less binary function
		struct nocase_compare
		{
			bool operator() (const unsigned char& c1, const unsigned char& c2) const {
				return toupper(c1) < toupper(c2);
			}
		};
		bool operator() (const std::string& s1, const std::string& s2) const {
			return std::lexicographical_compare
			(s1.begin(), s1.end(),   // source range
				s2.begin(), s2.end(),   // dest range
				nocase_compare());  // comparison
		}
	};

	class LuceneAnalyzerFactory {
	private:
		std::map<string, std::function<AnalyzerPtr()>, ci_more> factories;
	public:
		LuceneAnalyzerFactory()
			: factories()
		{
			factories.insert(
				{
					{ "STANDARD", []() -> AnalyzerPtr { return newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
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

		bool hasAnalyzer(string analyzerName)
		{
			return (factories.find(analyzerName) != factories.end());
		}

		AnalyzerPtr createAnalyzer(string analyzerName)
		{
			auto pFactory = factories.find(analyzerName);
			if (pFactory == factories.end()) {
				// исключение
				throw std::runtime_error("Analyzer" + analyzerName + " not found");
			}
			auto factory = pFactory->second;
			return factory();
		}

		list<string> getAnalyzerNames()
		{
			list<string> names;
			for (auto& pFactory : factories) {
				names.push_back(pFactory.first);
			}
			return names;
		}

	};

}

#endif	// LUCENE_ANALYZER_FACTORY_H