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

#include "WhitespaceLowerCaseASCIIFoldingAnalyzer.h"

#include "WhitespaceTokenizer.h"
#include "LowerCaseFilter.h"
#include "ASCIIFoldingFilter.h"

namespace Lucene
{

    WhitespaceLowerCaseASCIIFoldingAnalyzer::~WhitespaceLowerCaseASCIIFoldingAnalyzer() {
    }

    /// Constructs a {@link WhitespaceTokenizer} filtered by a {@link LowerCaseFilter} and an {@link ASCIIFoldingFilter}.
    TokenStreamPtr WhitespaceLowerCaseASCIIFoldingAnalyzer::tokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader)
    {
        TokenStreamPtr result(newLucene<WhitespaceTokenizer>(reader));
        result = newLucene<LowerCaseFilter>(result);
        result = newLucene<ASCIIFoldingFilter>(result);
        return result;
    }

    TokenStreamPtr WhitespaceLowerCaseASCIIFoldingAnalyzer::reusableTokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader) {
        auto streams = boost::dynamic_pointer_cast<WhitespaceLowerCaseASCIIFoldingAnalyzerSavedStreams>(getPreviousTokenStream());
        if (!streams) {
            streams = newLucene<WhitespaceLowerCaseASCIIFoldingAnalyzerSavedStreams>();
            setPreviousTokenStream(streams);
            streams->tokenStream = newLucene<WhitespaceTokenizer>(reader);
            streams->filteredTokenStream = newLucene<LowerCaseFilter>(streams->tokenStream);
            streams->filteredTokenStream = newLucene<ASCIIFoldingFilter>(streams->filteredTokenStream);
        }
        else {
            streams->tokenStream->reset(reader);
        }
        return streams->filteredTokenStream;
    }

    WhitespaceLowerCaseASCIIFoldingAnalyzerSavedStreams::~WhitespaceLowerCaseASCIIFoldingAnalyzerSavedStreams() {
    }

}
