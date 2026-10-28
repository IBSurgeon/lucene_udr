/**
 *  QueryParser that runs wildcard, prefix and fuzzy query terms through the
 *  configured Analyzer instead of only lower-casing them.
**/

#include "AnalyzingQueryParser.h"

#include "TermAttribute.h"

namespace Lucene
{

    AnalyzingQueryParser::AnalyzingQueryParser(LuceneVersion::Version matchVersion, const String& fieldName, const AnalyzerPtr& analyzer)
        : QueryParser(matchVersion, fieldName, analyzer)
    {
    }

    AnalyzingQueryParser::~AnalyzingQueryParser() {
    }

    String AnalyzingQueryParser::analyzeText(const String& fieldName, const String& text)
    {
        if (text.empty()) {
            return text;
        }

        TokenStreamPtr tokenStream = getAnalyzer()->tokenStream(fieldName, newLucene<StringReader>(text));
        TermAttributePtr termAttribute = tokenStream->addAttribute<TermAttribute>();
        tokenStream->reset();

        if (!tokenStream->incrementToken()) {
            return text;
        }
        const String term = termAttribute->term();

        // Several tokens can't be one wildcard/prefix/fuzzy term - keeping only the first would match far more
        // (ABC-XY-000123* -> abc*). Behave as stock QueryParser: lower-cased text, which matches nothing.
        if (tokenStream->incrementToken()) {
            return getLowercaseExpandedTerms() ? StringUtils::toLower(text) : text;
        }
        return term;
    }

    String AnalyzingQueryParser::analyzeWildcard(const String& fieldName, const String& termStr)
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

    QueryPtr AnalyzingQueryParser::getWildcardQuery(const String& fieldName, const String& termStr)
    {
        const String normalized = analyzeWildcard(fieldName, termStr);
        return newWildcardQuery(newLucene<Term>(fieldName, normalized));
    }

    QueryPtr AnalyzingQueryParser::getPrefixQuery(const String& fieldName, const String& termStr)
    {
        const String normalized = analyzeText(fieldName, termStr);
        return newPrefixQuery(newLucene<Term>(fieldName, normalized));
    }

    QueryPtr AnalyzingQueryParser::getFuzzyQuery(const String& fieldName, const String& termStr, double minSimilarity)
    {
        const String normalized = analyzeText(fieldName, termStr);
        return newFuzzyQuery(newLucene<Term>(fieldName, normalized), minSimilarity, getFuzzyPrefixLength());
    }

}
