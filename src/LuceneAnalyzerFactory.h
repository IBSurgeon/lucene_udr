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
#include <unordered_set>
#include <string>
#include "LuceneHeaders.h"

using namespace std;
using namespace Lucene;
using namespace Firebird;

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
			return lexicographical_compare(
				s1.begin(), s1.end(),   // source range
				s2.begin(), s2.end(),   // dest range
				nocase_compare()        // comparison
			);
		}
	};

	static const string DEFAULT_ANALYZER_NAME = "STANDARD";

	struct AnalyzerInfo
	{
		string analyzerName;
		string baseAnalyzer;
		bool stopWordsSupported;
		bool systemFlag;
	};

	class LuceneAnalyzerFactory final {
	private:
		struct AnalyzerFactory
		{
			function<AnalyzerPtr()> simpleFactory;
			function<AnalyzerPtr(const HashSet<String>)> extFactory;
			function<const HashSet<String>()> getStopWords;
			bool stopWordsSupported;
		};

		map<string, AnalyzerFactory, ci_more> m_factories;

	public:

		LuceneAnalyzerFactory();

		bool hasAnalyzer(const string& analyzerName);

		bool isStopWordsSupported(const string& analyzerName);

		AnalyzerPtr createAnalyzer(ThrowStatusWrapper* status, const string& analyzerName);

		AnalyzerPtr createAnalyzer(ThrowStatusWrapper* status, const string& analyzerName, const HashSet<String> stopWords);

		unordered_set<string> getAnalyzerNames();

		const AnalyzerInfo getAnalyzerInfo(ThrowStatusWrapper* status, const string& analyzerName);

		list<AnalyzerInfo> getAnalyzerInfos();

		const HashSet<String> getAnalyzerStopWords(ThrowStatusWrapper* status, const string& analyzerName);

	};

}

#endif	// LUCENE_ANALYZER_FACTORY_H