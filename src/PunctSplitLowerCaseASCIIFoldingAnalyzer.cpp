/**
 *  Analyzer that uses PunctSplitTokenizer filtered by PunctStripFilter, LowerCaseFilter and ASCIIFoldingFilter.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/

#include "PunctSplitLowerCaseASCIIFoldingAnalyzer.h"

#include "PunctTokenizers.h"
#include "LowerCaseFilter.h"
#include "ASCIIFoldingFilter.h"

namespace Lucene
{

    PunctSplitLowerCaseASCIIFoldingAnalyzer::~PunctSplitLowerCaseASCIIFoldingAnalyzer() {
    }

    TokenStreamPtr PunctSplitLowerCaseASCIIFoldingAnalyzer::tokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader)
    {
        TokenStreamPtr result(newLucene<PunctSplitTokenizer>(reader));
        result = newLucene<PunctStripFilter>(result);
        result = newLucene<LowerCaseFilter>(result);
        result = newLucene<ASCIIFoldingFilter>(result);
        return result;
    }

    TokenStreamPtr PunctSplitLowerCaseASCIIFoldingAnalyzer::reusableTokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader) {
        auto streams = boost::dynamic_pointer_cast<PunctSplitLowerCaseASCIIFoldingAnalyzerSavedStreams>(getPreviousTokenStream());
        if (!streams) {
            streams = newLucene<PunctSplitLowerCaseASCIIFoldingAnalyzerSavedStreams>();
            setPreviousTokenStream(streams);
            streams->tokenStream = newLucene<PunctSplitTokenizer>(reader);
            streams->filteredTokenStream = newLucene<PunctStripFilter>(streams->tokenStream);
            streams->filteredTokenStream = newLucene<LowerCaseFilter>(streams->filteredTokenStream);
            streams->filteredTokenStream = newLucene<ASCIIFoldingFilter>(streams->filteredTokenStream);
        }
        else {
            streams->tokenStream->reset(reader);
        }
        return streams->filteredTokenStream;
    }

    PunctSplitLowerCaseASCIIFoldingAnalyzerSavedStreams::~PunctSplitLowerCaseASCIIFoldingAnalyzerSavedStreams() {
    }

}
