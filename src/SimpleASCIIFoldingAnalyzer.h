#ifndef LUCENE_SIMPLE_ASCIIFOLDING_ANALYZER_H
#define LUCENE_SIMPLE_ASCIIFOLDING_ANALYZER_H

/**
 *  Analyzer that uses LetterTokenizer filtered by LowerCaseFilter and ASCIIFoldingFilter.
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
    class SimpleASCIIFoldingAnalyzer : public Analyzer {
    public:
        virtual ~SimpleASCIIFoldingAnalyzer();

        LUCENE_CLASS(SimpleASCIIFoldingAnalyzer);

    public:
        /// Constructs a {@link LetterTokenizer} filtered by a {@link LowerCaseFilter} and an {@link ASCIIFoldingFilter}.
        virtual TokenStreamPtr tokenStream(const String& fieldName, const ReaderPtr& reader);

        virtual TokenStreamPtr reusableTokenStream(const String& fieldName, const ReaderPtr& reader);
    };

    class SimpleASCIIFoldingAnalyzerSavedStreams : public LuceneObject {
    public:
        virtual ~SimpleASCIIFoldingAnalyzerSavedStreams();

    public:
        TokenizerPtr tokenStream;
        TokenStreamPtr filteredTokenStream;
    };

}

#endif
