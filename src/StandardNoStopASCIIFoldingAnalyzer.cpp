/**
 *  Analyzer that uses StandardTokenizer filtered by StandardFilter, LowerCaseFilter
 *  and ASCIIFoldingFilter, without any stop-word filtering.
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

#include "StandardNoStopASCIIFoldingAnalyzer.h"

#include "StandardAnalyzer.h"
#include "StandardTokenizer.h"
#include "StandardFilter.h"
#include "LowerCaseFilter.h"
#include "ASCIIFoldingFilter.h"

namespace Lucene
{

    StandardNoStopASCIIFoldingAnalyzer::StandardNoStopASCIIFoldingAnalyzer()
        : maxTokenLength(StandardAnalyzer::DEFAULT_MAX_TOKEN_LENGTH)
    {
    }

    StandardNoStopASCIIFoldingAnalyzer::~StandardNoStopASCIIFoldingAnalyzer() {
    }

    void StandardNoStopASCIIFoldingAnalyzer::setMaxTokenLength(int32_t length) {
        maxTokenLength = length;
    }

    int32_t StandardNoStopASCIIFoldingAnalyzer::getMaxTokenLength() {
        return maxTokenLength;
    }

    /// Constructs a {@link StandardTokenizer} filtered by a {@link StandardFilter}, a {@link LowerCaseFilter}
    /// and an {@link ASCIIFoldingFilter}.
    TokenStreamPtr StandardNoStopASCIIFoldingAnalyzer::tokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader)
    {
        StandardTokenizerPtr tokenStream(newLucene<StandardTokenizer>(LuceneVersion::LUCENE_CURRENT, reader));
        tokenStream->setMaxTokenLength(maxTokenLength);
        TokenStreamPtr result(newLucene<StandardFilter>(tokenStream));
        result = newLucene<LowerCaseFilter>(result);
        result = newLucene<ASCIIFoldingFilter>(result);
        return result;
    }

    TokenStreamPtr StandardNoStopASCIIFoldingAnalyzer::reusableTokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader) {
        auto streams = boost::dynamic_pointer_cast<StandardNoStopASCIIFoldingAnalyzerSavedStreams>(getPreviousTokenStream());
        if (!streams) {
            streams = newLucene<StandardNoStopASCIIFoldingAnalyzerSavedStreams>();
            setPreviousTokenStream(streams);
            streams->tokenStream = newLucene<StandardTokenizer>(LuceneVersion::LUCENE_CURRENT, reader);
            streams->filteredTokenStream = newLucene<StandardFilter>(streams->tokenStream);
            streams->filteredTokenStream = newLucene<LowerCaseFilter>(streams->filteredTokenStream);
            streams->filteredTokenStream = newLucene<ASCIIFoldingFilter>(streams->filteredTokenStream);
        }
        else {
            streams->tokenStream->reset(reader);
        }
        streams->tokenStream->setMaxTokenLength(maxTokenLength);
        return streams->filteredTokenStream;
    }

    StandardNoStopASCIIFoldingAnalyzerSavedStreams::~StandardNoStopASCIIFoldingAnalyzerSavedStreams() {
    }

}
