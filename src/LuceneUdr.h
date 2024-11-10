#ifndef UDR_LUCENE_H
#define UDR_LUCENE_H

/**
 *  Lucene UDR full text search library.
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

#define FB_UDR_STATUS_TYPE ::Firebird::ThrowStatusWrapper

#include "FBAutoPtr.h"
#include "firebird/UdrCppEngine.h"
#include "charsets.h"

constexpr char INTERNAL_UDR_CHARSET[] = "UTF8";

#endif // UDR_LUCENE_H
