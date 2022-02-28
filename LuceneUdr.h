#ifndef UDR_LUCENE_H
#define UDR_LUCENE_H

#define FB_UDR_STATUS_TYPE ::Firebird::ThrowStatusWrapper

#include "charsets.h"
#include "FBAutoPtr.h"
#include "firebird/UdrCppEngine.h"
#include <assert.h>
#include <stdio.h>
#include <string>
#include <sstream>

using namespace Firebird;

namespace
{

	template <class StatusType> FbException throwFbException(StatusType status, const char* message)
	{
		ISC_STATUS statusVector[] = {
			isc_arg_gds, isc_random,
			isc_arg_string, (ISC_STATUS)message,
			isc_arg_end
		};
		throw FbException(status, statusVector);
	}



}

#endif	// UDR_LUCENE_H
