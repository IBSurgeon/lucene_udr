#ifndef LUCENE_ANALYZING_QUERY_PARSER_H
#define LUCENE_ANALYZING_QUERY_PARSER_H

/**
 *  QueryParser that runs wildcard, prefix and fuzzy query terms through the
 *  configured Analyzer instead of only lower-casing them.
 *
 *  Lucene's stock QueryParser deliberately does not run wildcard/prefix/fuzzy
 *  query terms through the Analyzer (only optional lower-casing is applied),
 *  since a general-purpose Analyzer is not guaranteed to behave sensibly on
 *  partial terms. When the Analyzer only performs safe, per-character
 *  normalization (case folding, accent stripping), applying it to those terms
 *  is exactly what most users expect: a query like "cafe*" should match
 *  documents containing "café" if the field's analyzer folds accents.
**/
#include "LuceneHeaders.h"
#include "QueryParser.h"

namespace Lucene
{
    class AnalyzingQueryParser : public QueryParser {
    public:
        AnalyzingQueryParser(LuceneVersion::Version matchVersion, const String& fieldName, const AnalyzerPtr& analyzer);

        virtual ~AnalyzingQueryParser();

        LUCENE_CLASS(AnalyzingQueryParser);

    protected:
        /// Runs term through the Analyzer, preserving unescaped '*' and '?' wildcard
        /// characters at their original positions.
        virtual QueryPtr getWildcardQuery(const String& fieldName, const String& termStr);

        /// Runs the prefix term through the Analyzer before building a PrefixQuery.
        virtual QueryPtr getPrefixQuery(const String& fieldName, const String& termStr);

        /// Runs the term through the Analyzer before building a FuzzyQuery.
        virtual QueryPtr getFuzzyQuery(const String& fieldName, const String& termStr, double minSimilarity);

    private:
        String analyzeText(const String& fieldName, const String& text);
        String analyzeWildcard(const String& fieldName, const String& termStr);
    };

}

#endif
