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
//#include "lucene++\ChineseAnalyzer.h"
#include "lucene++\CJKAnalyzer.h"
#include "lucene++\CzechAnalyzer.h"
#include "lucene++\DutchAnalyzer.h"
#include "lucene++\FrenchAnalyzer.h"
#include "lucene++\GermanAnalyzer.h"
#include "lucene++\GreekAnalyzer.h"
#include "lucene++\PersianAnalyzer.h"
#include "lucene++\RussianAnalyzer.h"
//#include "lucene++\SnowballAnalyzer.h"

using namespace std;
using namespace Lucene;

namespace LuceneFTS {

	struct ci_less
	{
		// case-independent (ci) compare_less binary function
		struct nocase_compare
		{
			bool operator() (const unsigned char& c1, const unsigned char& c2) const {
				return tolower(c1) < tolower(c2);
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
		std::map<string, std::function<AnalyzerPtr()>, ci_less> factories;
	public:
		LuceneAnalyzerFactory()
			: factories()
		{
			factories.insert(
				{
					{ "Standard", []() -> AnalyzerPtr { return newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "Arabic", []() -> AnalyzerPtr { return newLucene<ArabicAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
				//	{ "Chinese", []() -> AnalyzerPtr { return newLucene<ChineseAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "CJK", []() -> AnalyzerPtr { return newLucene<CJKAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "Czech", []() -> AnalyzerPtr { return newLucene<CzechAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "Dutch", []() -> AnalyzerPtr { return newLucene<DutchAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "English", []() -> AnalyzerPtr { return newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "French", []() -> AnalyzerPtr { return newLucene<FrenchAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "German", []() -> AnalyzerPtr { return newLucene<GermanAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "Greek", []() -> AnalyzerPtr { return newLucene<GreekAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "Persian", []() -> AnalyzerPtr { return newLucene<PersianAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
					{ "Russian", []() -> AnalyzerPtr { return newLucene<RussianAnalyzer>(LuceneVersion::LUCENE_CURRENT); } }
				//	{ "Snowball", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT); } }
				}
			);
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