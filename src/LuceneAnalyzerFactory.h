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
			return lexicographical_compare (
				s1.begin(), s1.end(),   // source range
				s2.begin(), s2.end(),   // dest range
				nocase_compare()        // comparison
			);     
		}
	};

	static const string DEFAULT_ANALYZER_NAME = "STANDARD";

	class LuceneAnalyzerFactory final {
	private:
		map<string, function<AnalyzerPtr()>, ci_more> m_factories;
	public:

		LuceneAnalyzerFactory();

		bool hasAnalyzer(const string& analyzerName);

		AnalyzerPtr createAnalyzer(ThrowStatusWrapper* status, const string& analyzerName);

		list<string> getAnalyzerNames();

	};

}

#endif	// LUCENE_ANALYZER_FACTORY_H