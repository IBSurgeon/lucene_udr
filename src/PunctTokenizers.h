#ifndef LUCENE_PUNCT_TOKENIZERS_H
#define LUCENE_PUNCT_TOKENIZERS_H

/**
 *  Tokenizer and filter for treating punctuation as a word separator or removing it.
 *  Letters and digits are decided by UnicodeUtil (Unicode tables), not by the C locale.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/
#include "LuceneHeaders.h"
#include "CharTokenizer.h"
#include "TokenFilter.h"

namespace Lucene
{
    /// Tokens are runs of letters and digits, anything else separates them: "V10-0423" -> "V10" | "0423".
    /// Combining marks don't split a word (decomposed "a" + U+0304 stays one token).
    class PunctSplitTokenizer : public CharTokenizer {
    public:
        PunctSplitTokenizer(const ReaderPtr& input);
        virtual ~PunctSplitTokenizer();

        LUCENE_CLASS(PunctSplitTokenizer);

    public:
        virtual bool isTokenChar(wchar_t c);
    };

    /// Keeps only letters and digits in each token: "V10-0423" -> "V100423".
    /// Tokens left empty ("-", "/") are dropped.
    class PunctStripFilter : public TokenFilter {
    public:
        PunctStripFilter(const TokenStreamPtr& input);
        virtual ~PunctStripFilter();

        LUCENE_CLASS(PunctStripFilter);

    protected:
        TermAttributePtr termAtt;

    public:
        virtual bool incrementToken();
    };

}

#endif
