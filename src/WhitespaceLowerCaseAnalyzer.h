#ifndef LUCENE_WHITESPACE_LOWERCASE_ANALYZER_H
#define LUCENE_WHITESPACE_LOWERCASE_ANALYZER_H

/**
 *  Analyzer that uses WhitespaceTokenizer filtered by LowerCaseFilter.
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
    class WhitespaceLowerCaseAnalyzer : public Analyzer {
    public:
        virtual ~WhitespaceLowerCaseAnalyzer();

        LUCENE_CLASS(WhitespaceLowerCaseAnalyzer);

    public:
        /// Constructs a {@link WhitespaceTokenizer} filtered by a {@link LowerCaseFilter}.
        virtual TokenStreamPtr tokenStream(const String& fieldName, const ReaderPtr& reader);

        virtual TokenStreamPtr reusableTokenStream(const String& fieldName, const ReaderPtr& reader);
    };

    class WhitespaceLowerCaseAnalyzerSavedStreams : public LuceneObject {
    public:
        virtual ~WhitespaceLowerCaseAnalyzerSavedStreams();

    public:
        TokenizerPtr tokenStream;
        TokenStreamPtr filteredTokenStream;
    };

}

#endif
