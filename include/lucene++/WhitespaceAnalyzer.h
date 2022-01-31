/////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2009-2014 Alan Wright. All rights reserved.
// Distributable under the terms of either the Apache License (Version 2.0)
// or the GNU Lesser General Public License.
/////////////////////////////////////////////////////////////////////////////

#ifndef WHITESPACEANALYZER_H
#define WHITESPACEANALYZER_H

#include "Analyzer.h"

namespace Lucene {

/// An Analyzer that uses {@link WhitespaceTokenizer}.
class LPPAPI WhitespaceAnalyzer : public Analyzer {
public:
    virtual ~WhitespaceAnalyzer();

    LUCENE_CLASS(WhitespaceAnalyzer);

public:
    virtual TokenStreamPtr tokenStream(const String& fieldName, const ReaderPtr& reader);
    virtual TokenStreamPtr reusableTokenStream(const String& fieldName, const ReaderPtr& reader);
};

}

#endif
