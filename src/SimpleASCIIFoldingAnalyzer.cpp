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

#include "SimpleASCIIFoldingAnalyzer.h"

#include "LetterTokenizer.h"
#include "LowerCaseFilter.h"
#include "ASCIIFoldingFilter.h"

namespace Lucene
{

    SimpleASCIIFoldingAnalyzer::~SimpleASCIIFoldingAnalyzer() {
    }

    /// Constructs a {@link LetterTokenizer} filtered by a {@link LowerCaseFilter} and an {@link ASCIIFoldingFilter}.
    TokenStreamPtr SimpleASCIIFoldingAnalyzer::tokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader)
    {
        TokenStreamPtr result(newLucene<LetterTokenizer>(reader));
        result = newLucene<LowerCaseFilter>(result);
        result = newLucene<ASCIIFoldingFilter>(result);
        return result;
    }

    TokenStreamPtr SimpleASCIIFoldingAnalyzer::reusableTokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader) {
        auto streams = boost::dynamic_pointer_cast<SimpleASCIIFoldingAnalyzerSavedStreams>(getPreviousTokenStream());
        if (!streams) {
            streams = newLucene<SimpleASCIIFoldingAnalyzerSavedStreams>();
            setPreviousTokenStream(streams);
            streams->tokenStream = newLucene<LetterTokenizer>(reader);
            streams->filteredTokenStream = newLucene<LowerCaseFilter>(streams->tokenStream);
            streams->filteredTokenStream = newLucene<ASCIIFoldingFilter>(streams->filteredTokenStream);
        }
        else {
            streams->tokenStream->reset(reader);
        }
        return streams->filteredTokenStream;
    }

    SimpleASCIIFoldingAnalyzerSavedStreams::~SimpleASCIIFoldingAnalyzerSavedStreams() {
    }

}
