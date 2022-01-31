/////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2009-2014 Alan Wright. All rights reserved.
// Distributable under the terms of either the Apache License (Version 2.0)
// or the GNU Lesser General Public License.
/////////////////////////////////////////////////////////////////////////////

#ifndef KEYWORDANALYZER_H
#define KEYWORDANALYZER_H

#include "Analyzer.h"

namespace Lucene {

/// Tokenizes the entire stream as a single token. This is useful for data like zip codes, ids, and some
/// product names.
class LPPAPI KeywordAnalyzer : public Analyzer {
public:
    virtual ~KeywordAnalyzer();

    LUCENE_CLASS(KeywordAnalyzer);

public:
    virtual TokenStreamPtr tokenStream(const String& fieldName, const ReaderPtr& reader);
    virtual TokenStreamPtr reusableTokenStream(const String& fieldName, const ReaderPtr& reader);
};

}

#endif
