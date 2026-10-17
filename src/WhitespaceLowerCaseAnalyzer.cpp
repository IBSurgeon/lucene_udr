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

#include "WhitespaceLowerCaseAnalyzer.h"

#include "WhitespaceTokenizer.h"
#include "LowerCaseFilter.h"

namespace Lucene
{

    WhitespaceLowerCaseAnalyzer::~WhitespaceLowerCaseAnalyzer() {
    }

    /// Constructs a {@link WhitespaceTokenizer} filtered by a {@link LowerCaseFilter}.
    TokenStreamPtr WhitespaceLowerCaseAnalyzer::tokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader)
    {
        TokenStreamPtr result(newLucene<WhitespaceTokenizer>(reader));
        result = newLucene<LowerCaseFilter>(result);
        return result;
    }

    TokenStreamPtr WhitespaceLowerCaseAnalyzer::reusableTokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader) {
        auto streams = boost::dynamic_pointer_cast<WhitespaceLowerCaseAnalyzerSavedStreams>(getPreviousTokenStream());
        if (!streams) {
            streams = newLucene<WhitespaceLowerCaseAnalyzerSavedStreams>();
            setPreviousTokenStream(streams);
            streams->tokenStream = newLucene<WhitespaceTokenizer>(reader);
            streams->filteredTokenStream = newLucene<LowerCaseFilter>(streams->tokenStream);
        }
        else {
            streams->tokenStream->reset(reader);
        }
        return streams->filteredTokenStream;
    }

    WhitespaceLowerCaseAnalyzerSavedStreams::~WhitespaceLowerCaseAnalyzerSavedStreams() {
    }

}
