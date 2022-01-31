/////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2009-2014 Alan Wright. All rights reserved.
// Distributable under the terms of either the Apache License (Version 2.0)
// or the GNU Lesser General Public License.
/////////////////////////////////////////////////////////////////////////////

#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include "Config.h"

namespace Lucene {

/// Allocate block of memory.
LPPAPI void* AllocMemory(size_t size);

/// Reallocate a given block of memory.
LPPAPI void* ReallocMemory(void* memory, size_t size);

/// Release a given block of memory.
LPPAPI void FreeMemory(void* memory);
}

#endif
