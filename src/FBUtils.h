#ifndef FB_BLOB_UTILS_H
#define FB_BLOB_UTILS_H

/**
 *  Various helper functions.
 *
 *  The original code was created by Simonov Denis
 *  for the open source Lucene UDR full-text search library for Firebird DBMS.
 *
 *  Copyright (c) 2022 Simonov Denis <sim-mail@list.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/

#include "LuceneUdr.h"
#include <string>

using namespace std;
using namespace Firebird;

namespace LuceneUDR
{

	class BlobUtils final {
	public:
		static const size_t MAX_SEGMENT_SIZE = 65535;

		static string getString(ThrowStatusWrapper* status, IBlob* blob);

		static void setString(ThrowStatusWrapper* status, IBlob* blob, const string& str);
	};


	const unsigned int getSqlDialect(ThrowStatusWrapper* status, IAttachment* att);

	/// <summary>
	/// Escapes the name of the metadata object depending on the SQL dialect. 
	/// </summary>
	/// 
	/// <param name="sqlDialect">SQL dialect</param>
	/// <param name="name">Metadata object name</param>
	/// 
	/// <returns>Returns the escaped name of the metadata object.</returns>
	inline string escapeMetaName(const unsigned int sqlDialect, const string& name)
	{
		if (name == "RDB$DB_KEY")
			return name;
		switch (sqlDialect) {
		case 1:
			return name;
		case 3:
		default:
			return "\"" + name + "\"";
		}
	}

	inline void throwException(ThrowStatusWrapper* const status, const char* message)
	{
		ISC_STATUS statusVector[] = {
			isc_arg_gds, isc_random,
			isc_arg_string, (ISC_STATUS)message,
			isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	IMessageMetadata* prepareTextMetaData(ThrowStatusWrapper* status, IMessageMetadata* meta);
}

#endif	// FB_BLOB_UTILS_H