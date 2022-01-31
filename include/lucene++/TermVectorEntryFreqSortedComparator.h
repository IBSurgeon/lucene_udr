/////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2009-2014 Alan Wright. All rights reserved.
// Distributable under the terms of either the Apache License (Version 2.0)
// or the GNU Lesser General Public License.
/////////////////////////////////////////////////////////////////////////////

#ifndef TERMVECTORENTRYFREQSORTEDCOMPARATOR_H
#define TERMVECTORENTRYFREQSORTEDCOMPARATOR_H

#include "LuceneObject.h"

namespace Lucene {

/// Compares {@link TermVectorEntry}s first by frequency and then by the term (case-sensitive)
class LPPAPI TermVectorEntryFreqSortedComparator : public LuceneObject {
public:
    virtual ~TermVectorEntryFreqSortedComparator();

    LUCENE_CLASS(TermVectorEntryFreqSortedComparator);

public:
    static bool compare(const TermVectorEntryPtr& first, const TermVectorEntryPtr& second);
};

}

#endif
