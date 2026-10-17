/**
 *  Tokenizer and filter for treating punctuation as a word separator or removing it.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/

#include "PunctTokenizers.h"

#include "TermAttribute.h"
#include "UnicodeUtils.h"

namespace Lucene
{

    PunctSplitTokenizer::PunctSplitTokenizer(const ReaderPtr& input) : CharTokenizer(input) {
    }

    PunctSplitTokenizer::~PunctSplitTokenizer() {
    }

    bool PunctSplitTokenizer::isTokenChar(wchar_t c) {
        return UnicodeUtil::isAlnum(c) || UnicodeUtil::isNonSpacing(c);
    }

    PunctStripFilter::PunctStripFilter(const TokenStreamPtr& input) : TokenFilter(input) {
        termAtt = addAttribute<TermAttribute>();
    }

    PunctStripFilter::~PunctStripFilter() {
    }

    bool PunctStripFilter::incrementToken() {
        while (input->incrementToken()) {
            wchar_t* buffer = termAtt->termBufferArray();
            const int32_t termLength = termAtt->termLength();
            int32_t length = 0;
            for (int32_t i = 0; i < termLength; ++i) {
                if (UnicodeUtil::isAlnum(buffer[i])) {
                    buffer[length++] = buffer[i];
                }
            }
            if (length) {
                termAtt->setTermLength(length);
                return true;
            }
        }
        return false;
    }

}
