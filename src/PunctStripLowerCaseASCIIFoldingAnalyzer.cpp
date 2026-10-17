/**
 *  Analyzer that uses WhitespaceTokenizer filtered by PunctStripFilter, LowerCaseFilter and ASCIIFoldingFilter.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/

#include "PunctStripLowerCaseASCIIFoldingAnalyzer.h"

#include "PunctTokenizers.h"
#include "WhitespaceTokenizer.h"
#include "LowerCaseFilter.h"
#include "ASCIIFoldingFilter.h"

namespace Lucene
{

    PunctStripLowerCaseASCIIFoldingAnalyzer::~PunctStripLowerCaseASCIIFoldingAnalyzer() {
    }

    TokenStreamPtr PunctStripLowerCaseASCIIFoldingAnalyzer::tokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader)
    {
        TokenStreamPtr result(newLucene<WhitespaceTokenizer>(reader));
        result = newLucene<PunctStripFilter>(result);
        result = newLucene<LowerCaseFilter>(result);
        result = newLucene<ASCIIFoldingFilter>(result);
        return result;
    }

    TokenStreamPtr PunctStripLowerCaseASCIIFoldingAnalyzer::reusableTokenStream([[maybe_unused]] const String& fieldName, const ReaderPtr& reader) {
        auto streams = boost::dynamic_pointer_cast<PunctStripLowerCaseASCIIFoldingAnalyzerSavedStreams>(getPreviousTokenStream());
        if (!streams) {
            streams = newLucene<PunctStripLowerCaseASCIIFoldingAnalyzerSavedStreams>();
            setPreviousTokenStream(streams);
            streams->tokenStream = newLucene<WhitespaceTokenizer>(reader);
            streams->filteredTokenStream = newLucene<PunctStripFilter>(streams->tokenStream);
            streams->filteredTokenStream = newLucene<LowerCaseFilter>(streams->filteredTokenStream);
            streams->filteredTokenStream = newLucene<ASCIIFoldingFilter>(streams->filteredTokenStream);
        }
        else {
            streams->tokenStream->reset(reader);
        }
        return streams->filteredTokenStream;
    }

    PunctStripLowerCaseASCIIFoldingAnalyzerSavedStreams::~PunctStripLowerCaseASCIIFoldingAnalyzerSavedStreams() {
    }

}
