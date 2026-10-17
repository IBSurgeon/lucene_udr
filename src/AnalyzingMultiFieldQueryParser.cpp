/**
 *  MultiFieldQueryParser that runs wildcard, prefix and fuzzy query terms through the
 *  configured Analyzer instead of only lower-casing them.
**/

#include "AnalyzingMultiFieldQueryParser.h"

#include "TermAttribute.h"

namespace Lucene
{

    AnalyzingMultiFieldQueryParser::AnalyzingMultiFieldQueryParser(LuceneVersion::Version matchVersion, Collection<String> fields, const AnalyzerPtr& analyzer)
        : MultiFieldQueryParser(matchVersion, fields, analyzer)
    {
    }

    AnalyzingMultiFieldQueryParser::~AnalyzingMultiFieldQueryParser() {
    }

    String AnalyzingMultiFieldQueryParser::analyzeText(const String& fieldName, const String& text)
    {
        if (text.empty()) {
            return text;
        }

        TokenStreamPtr tokenStream = getAnalyzer()->tokenStream(fieldName, newLucene<StringReader>(text));
        TermAttributePtr termAttribute = tokenStream->addAttribute<TermAttribute>();
        tokenStream->reset();

        if (tokenStream->incrementToken()) {
            return termAttribute->term();
        }
        return text;
    }

    String AnalyzingMultiFieldQueryParser::analyzeWildcard(const String& fieldName, const String& termStr)
    {
        String result;
        String chunk;

        for (size_t i = 0; i < termStr.length(); ++i) {
            wchar_t ch = termStr[i];
            if (ch == L'\\' && i + 1 < termStr.length()) {
                // Preserve escaped characters literally within the current chunk.
                chunk += ch;
                chunk += termStr[++i];
                continue;
            }
            if (ch == L'*' || ch == L'?') {
                result += analyzeText(fieldName, chunk);
                chunk.clear();
                result += ch;
            }
            else {
                chunk += ch;
            }
        }
        result += analyzeText(fieldName, chunk);
        return result;
    }

    QueryPtr AnalyzingMultiFieldQueryParser::fanOutOverFields(const String& fieldName, const FieldQueryBuilder& buildForField)
    {
        if (!fieldName.empty()) {
            return buildForField(fieldName);
        }

        Collection<BooleanClausePtr> clauses = Collection<BooleanClausePtr>::newInstance();
        for (const auto& f : fields) {
            QueryPtr q = buildForField(f);
            if (!q) {
                continue;
            }
            if (boosts && boosts.contains(f)) {
                q->setBoost(boosts.get(f));
            }
            clauses.add(newBooleanClause(q, BooleanClause::SHOULD));
        }
        if (clauses.empty()) {
            return QueryPtr();
        }
        return getBooleanQuery(clauses, true);
    }

    QueryPtr AnalyzingMultiFieldQueryParser::getWildcardQuery(const String& fieldName, const String& termStr)
    {
        return fanOutOverFields(fieldName, [this, termStr](const String& f) -> QueryPtr {
            const String normalized = analyzeWildcard(f, termStr);
            return newWildcardQuery(newLucene<Term>(f, normalized));
        });
    }

    QueryPtr AnalyzingMultiFieldQueryParser::getPrefixQuery(const String& fieldName, const String& termStr)
    {
        return fanOutOverFields(fieldName, [this, termStr](const String& f) -> QueryPtr {
            const String normalized = analyzeText(f, termStr);
            return newPrefixQuery(newLucene<Term>(f, normalized));
        });
    }

    QueryPtr AnalyzingMultiFieldQueryParser::getFuzzyQuery(const String& fieldName, const String& termStr, double minSimilarity)
    {
        return fanOutOverFields(fieldName, [this, termStr, minSimilarity](const String& f) -> QueryPtr {
            const String normalized = analyzeText(f, termStr);
            return newFuzzyQuery(newLucene<Term>(f, normalized), minSimilarity, getFuzzyPrefixLength());
        });
    }

}
