#ifndef LUCENE_STANDARD_NOSTOP_ASCIIFOLDING_ANALYZER_H
#define LUCENE_STANDARD_NOSTOP_ASCIIFOLDING_ANALYZER_H

/**
 *  Analyzer that uses StandardTokenizer filtered by StandardFilter, LowerCaseFilter
 *  and ASCIIFoldingFilter, without any stop-word filtering.
 *
 *  Unlike StandardAnalyzer, this analyzer does not drop English stop words, so it is
 *  suitable for non-English text and identifiers (VAT numbers, addresses, emails)
 *  where an English stop-word list would be inappropriate or harmful.
 *
 *  The original code was created by Simonov Denis
 *  for the open source project "IBSurgeon Full Text Search UDR".
 *
 *  Copyright (c) 2022 Simonov Denis <sim-mail@list.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/
#include "LuceneHeaders.h"
#include "Analyzer.h"

namespace Lucene
{
    class StandardNoStopASCIIFoldingAnalyzer : public Analyzer {
    public:
        StandardNoStopASCIIFoldingAnalyzer();

        virtual ~StandardNoStopASCIIFoldingAnalyzer();

        LUCENE_CLASS(StandardNoStopASCIIFoldingAnalyzer);

    protected:
        int32_t maxTokenLength;

    public:
        /// Set the maximum allowed token length. Any token longer than this is discarded.
        void setMaxTokenLength(int32_t length);

        /// @see #setMaxTokenLength
        int32_t getMaxTokenLength();

        /// Constructs a {@link StandardTokenizer} filtered by a {@link StandardFilter}, a {@link LowerCaseFilter}
        /// and an {@link ASCIIFoldingFilter}.
        virtual TokenStreamPtr tokenStream(const String& fieldName, const ReaderPtr& reader);

        virtual TokenStreamPtr reusableTokenStream(const String& fieldName, const ReaderPtr& reader);
    };

    class StandardNoStopASCIIFoldingAnalyzerSavedStreams : public LuceneObject {
    public:
        virtual ~StandardNoStopASCIIFoldingAnalyzerSavedStreams();

    public:
        StandardTokenizerPtr tokenStream;
        TokenStreamPtr filteredTokenStream;
    };

}

#endif
