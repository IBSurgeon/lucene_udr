#ifndef LUCENE_ANALYZING_MULTI_FIELD_QUERY_PARSER_H
#define LUCENE_ANALYZING_MULTI_FIELD_QUERY_PARSER_H

/**
 *  MultiFieldQueryParser that runs wildcard, prefix and fuzzy query terms through the
 *  configured Analyzer instead of only lower-casing them.
 *
 *  Mirrors MultiFieldQueryParser's own fan-out behavior: when a query term has no
 *  explicit "field:" prefix, a query is built per configured field (each normalized
 *  through that field's Analyzer) and combined with OR; when a term explicitly names
 *  a field, normalization and query construction are delegated to that field alone.
**/
#include "LuceneHeaders.h"
#include "MultiFieldQueryParser.h"

#include <functional>

namespace Lucene
{
    class AnalyzingMultiFieldQueryParser : public MultiFieldQueryParser {
    public:
        AnalyzingMultiFieldQueryParser(LuceneVersion::Version matchVersion, Collection<String> fields, const AnalyzerPtr& analyzer);

        virtual ~AnalyzingMultiFieldQueryParser();

        LUCENE_CLASS(AnalyzingMultiFieldQueryParser);

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

        typedef std::function<QueryPtr(const String&)> FieldQueryBuilder;

        /// If fieldName is explicit, delegates to buildForField for that one field.
        /// Otherwise builds a query per configured field and combines them with OR,
        /// applying any configured per-field boost.
        QueryPtr fanOutOverFields(const String& fieldName, const FieldQueryBuilder& buildForField);
    };

}

#endif
