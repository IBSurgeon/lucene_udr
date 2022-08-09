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

namespace LuceneUDR {

	LuceneAnalyzerFactory::LuceneAnalyzerFactory()
		: m_factories()
	{
		m_factories.insert(
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
				{ "ENGLISH", []() -> AnalyzerPtr { return newLucene<EnglishAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
				{ "FRENCH", []() -> AnalyzerPtr { return newLucene<FrenchAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
				{ "GERMAN", []() -> AnalyzerPtr { return newLucene<GermanAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
				{ "GREEK", []() -> AnalyzerPtr { return newLucene<GreekAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
				{ "PERSIAN", []() -> AnalyzerPtr { return newLucene<PersianAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
				{ "RUSSIAN", []() -> AnalyzerPtr { return newLucene<RussianAnalyzer>(LuceneVersion::LUCENE_CURRENT); } },
				{ "SNOWBALL(DANISH)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"danish"); } },
				{ "SNOWBALL(DUTCH)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"dutch", DutchAnalyzer::getDefaultStopSet()); } },
				{ "SNOWBALL(ENGLISH)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"english", StopAnalyzer::ENGLISH_STOP_WORDS_SET()); } },
				{ "SNOWBALL(FINNISH)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"finnish"); } },
				{ "SNOWBALL(FRENCH)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"french", FrenchAnalyzer::getDefaultStopSet()); } },
				{ "SNOWBALL(GERMAN)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"german", GermanAnalyzer::getDefaultStopSet()); } },
				{ "SNOWBALL(HUNGARIAN)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"hungarian"); } },
				{ "SNOWBALL(ITALIAN)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"italian"); } },
				{ "SNOWBALL(NORWEGIAN)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"norwegian"); } },
				{ "SNOWBALL(PORTER)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"porter", StopAnalyzer::ENGLISH_STOP_WORDS_SET()); } },
				{ "SNOWBALL(PORTUGUESE)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"portuguese"); } },
				{ "SNOWBALL(ROMANIAN)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"romanian"); } },
				{ "SNOWBALL(RUSSIAN)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"russian", RussianAnalyzer::getDefaultStopSet()); } },
				{ "SNOWBALL(SPANISH)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"spanish"); } },
				{ "SNOWBALL(SWEDISH)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"swedish"); } },
				{ "SNOWBALL(TURKISH)", []() -> AnalyzerPtr { return newLucene<SnowballAnalyzer>(LuceneVersion::LUCENE_CURRENT, L"turkish"); } }
			}
		);

		m_stopwords_map.insert(
			{
				{ "STANDARD", []() -> const HashSet<String> { return StopAnalyzer::ENGLISH_STOP_WORDS_SET(); } },
				{ "SIMPLE", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "WHITESPACE", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "KEYWORD", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "STOP", []() -> const HashSet<String> { return StopAnalyzer::ENGLISH_STOP_WORDS_SET(); } },
				{ "ARABIC", []() -> const HashSet<String> { return ArabicAnalyzer::getDefaultStopSet(); } },
				{ "BRAZILIAN", []() -> const HashSet<String> { return BrazilianAnalyzer::getDefaultStopSet(); } },
				{ "CHINESE", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "CJK", []() -> const HashSet<String> { return CJKAnalyzer::getDefaultStopSet(); } },
				{ "CZECH", []() -> const HashSet<String> { return CzechAnalyzer::getDefaultStopSet(); } },
				{ "DUTCH", []() -> const HashSet<String> { return DutchAnalyzer::getDefaultStopSet(); } },
				{ "ENGLISH", []() -> const HashSet<String> { return StopAnalyzer::ENGLISH_STOP_WORDS_SET(); } },
				{ "FRENCH", []() -> const HashSet<String> { return FrenchAnalyzer::getDefaultStopSet(); } },
				{ "GERMAN", []() -> const HashSet<String> { return GermanAnalyzer::getDefaultStopSet(); } },
				{ "GREEK", []() -> const HashSet<String> { return GreekAnalyzer::getDefaultStopSet(); } },
				{ "PERSIAN", []() -> const HashSet<String> { return PersianAnalyzer::getDefaultStopSet(); } },
				{ "RUSSIAN", []() -> const HashSet<String> { return RussianAnalyzer::getDefaultStopSet(); } },
				{ "SNOWBALL(DANISH)", []() -> const HashSet<String> { return HashSet<String>();; } },
				{ "SNOWBALL(DUTCH)", []() -> const HashSet<String> { return DutchAnalyzer::getDefaultStopSet(); } },
				{ "SNOWBALL(ENGLISH)", []() -> const HashSet<String> { return StopAnalyzer::ENGLISH_STOP_WORDS_SET(); } },
				{ "SNOWBALL(FINNISH)", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "SNOWBALL(FRENCH)", []() -> const HashSet<String> { return FrenchAnalyzer::getDefaultStopSet(); } },
				{ "SNOWBALL(GERMAN)", []() -> const HashSet<String> { return GermanAnalyzer::getDefaultStopSet(); } },
				{ "SNOWBALL(HUNGARIAN)", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "SNOWBALL(ITALIAN)", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "SNOWBALL(NORWEGIAN)", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "SNOWBALL(PORTER)", []() -> const HashSet<String> { return StopAnalyzer::ENGLISH_STOP_WORDS_SET(); } },
				{ "SNOWBALL(PORTUGUESE)", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "SNOWBALL(ROMANIAN)", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "SNOWBALL(RUSSIAN)", []() -> const HashSet<String> { return RussianAnalyzer::getDefaultStopSet(); } },
				{ "SNOWBALL(SPANISH)", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "SNOWBALL(SWEDISH)", []() -> const HashSet<String> { return HashSet<String>(); } },
				{ "SNOWBALL(TURKISH)", []() -> const HashSet<String> { return HashSet<String>(); } }
			}
		);
	}

	bool LuceneAnalyzerFactory::hasAnalyzer(const string& analyzerName)
	{
		return (m_factories.find(analyzerName) != m_factories.end());
	}

	AnalyzerPtr LuceneAnalyzerFactory::createAnalyzer(ThrowStatusWrapper* status, const string& analyzerName)
	{
		auto pFactory = m_factories.find(analyzerName);
		if (pFactory == m_factories.end()) {
			throwException(status, R"(Analyzer "%s" not found.)", analyzerName.c_str());
		}
		auto factory = pFactory->second;
		return factory();
	}

	list<string> LuceneAnalyzerFactory::getAnalyzerNames()
	{
		list<string> names;
		for (const auto& pFactory : m_factories) {
			names.push_back(pFactory.first);
		}
		return names;
	}

	const HashSet<String> LuceneAnalyzerFactory::getAnalyzerStopWords(ThrowStatusWrapper* status, const string& analyzerName)
	{
		auto pStopWords = m_stopwords_map.find(analyzerName);
		if (pStopWords == m_stopwords_map.end()) {
			throwException(status, R"(Analyzer "%s" not found.)", analyzerName.c_str());
		}
		auto fnStopWords = pStopWords->second;
		return fnStopWords();
	}
}