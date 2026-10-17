#ifndef LUCENE_PUNCT_STRIP_LOWERCASE_ASCIIFOLDING_ANALYZER_H
#define LUCENE_PUNCT_STRIP_LOWERCASE_ASCIIFOLDING_ANALYZER_H

/**
 *  Analyzer that uses WhitespaceTokenizer filtered by PunctStripFilter, LowerCaseFilter and ASCIIFoldingFilter.
 *  "V10-0423" -> "v100423"
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/
#include "LuceneHeaders.h"
#include "Analyzer.h"

namespace Lucene
{
    class PunctStripLowerCaseASCIIFoldingAnalyzer : public Analyzer {
    public:
        virtual ~PunctStripLowerCaseASCIIFoldingAnalyzer();

        LUCENE_CLASS(PunctStripLowerCaseASCIIFoldingAnalyzer);

    public:
        /// Constructs a {@link WhitespaceTokenizer} filtered by a {@link PunctStripFilter},
        /// a {@link LowerCaseFilter} and an {@link ASCIIFoldingFilter}.
        virtual TokenStreamPtr tokenStream(const String& fieldName, const ReaderPtr& reader);

        virtual TokenStreamPtr reusableTokenStream(const String& fieldName, const ReaderPtr& reader);
    };

    class PunctStripLowerCaseASCIIFoldingAnalyzerSavedStreams : public LuceneObject {
    public:
        virtual ~PunctStripLowerCaseASCIIFoldingAnalyzerSavedStreams();

    public:
        TokenizerPtr tokenStream;
        TokenStreamPtr filteredTokenStream;
    };

}

#endif
