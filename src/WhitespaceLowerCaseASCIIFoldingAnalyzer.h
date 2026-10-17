#ifndef LUCENE_WHITESPACE_LOWERCASE_ASCIIFOLDING_ANALYZER_H
#define LUCENE_WHITESPACE_LOWERCASE_ASCIIFOLDING_ANALYZER_H

/**
 *  Analyzer that uses WhitespaceTokenizer filtered by LowerCaseFilter and ASCIIFoldingFilter.
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
    class WhitespaceLowerCaseASCIIFoldingAnalyzer : public Analyzer {
    public:
        virtual ~WhitespaceLowerCaseASCIIFoldingAnalyzer();

        LUCENE_CLASS(WhitespaceLowerCaseASCIIFoldingAnalyzer);

    public:
        /// Constructs a {@link WhitespaceTokenizer} filtered by a {@link LowerCaseFilter} and an {@link ASCIIFoldingFilter}.
        virtual TokenStreamPtr tokenStream(const String& fieldName, const ReaderPtr& reader);

        virtual TokenStreamPtr reusableTokenStream(const String& fieldName, const ReaderPtr& reader);
    };

    class WhitespaceLowerCaseASCIIFoldingAnalyzerSavedStreams : public LuceneObject {
    public:
        virtual ~WhitespaceLowerCaseASCIIFoldingAnalyzerSavedStreams();

    public:
        TokenizerPtr tokenStream;
        TokenStreamPtr filteredTokenStream;
    };

}

#endif
