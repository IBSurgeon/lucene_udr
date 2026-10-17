#ifndef LUCENE_PUNCT_SPLIT_LOWERCASE_ASCIIFOLDING_ANALYZER_H
#define LUCENE_PUNCT_SPLIT_LOWERCASE_ASCIIFOLDING_ANALYZER_H

/**
 *  Analyzer that uses PunctSplitTokenizer filtered by PunctStripFilter, LowerCaseFilter and ASCIIFoldingFilter.
 *  "V10-0423" -> "v10" | "0423"
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/
#include "LuceneHeaders.h"
#include "Analyzer.h"

namespace Lucene
{
    class PunctSplitLowerCaseASCIIFoldingAnalyzer : public Analyzer {
    public:
        virtual ~PunctSplitLowerCaseASCIIFoldingAnalyzer();

        LUCENE_CLASS(PunctSplitLowerCaseASCIIFoldingAnalyzer);

    public:
        /// Constructs a {@link PunctSplitTokenizer} filtered by a {@link PunctStripFilter} (drops combining marks),
        /// a {@link LowerCaseFilter} and an {@link ASCIIFoldingFilter}.
        virtual TokenStreamPtr tokenStream(const String& fieldName, const ReaderPtr& reader);

        virtual TokenStreamPtr reusableTokenStream(const String& fieldName, const ReaderPtr& reader);
    };

    class PunctSplitLowerCaseASCIIFoldingAnalyzerSavedStreams : public LuceneObject {
    public:
        virtual ~PunctSplitLowerCaseASCIIFoldingAnalyzerSavedStreams();

    public:
        TokenizerPtr tokenStream;
        TokenStreamPtr filteredTokenStream;
    };

}

#endif
