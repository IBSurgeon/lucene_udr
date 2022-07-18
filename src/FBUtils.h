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

#include "firebird/UdrCppEngine.h"
#include <string>
#include <sstream>
#include <algorithm>

using namespace std;
using namespace Firebird;

namespace LuceneUDR
{

	class BlobUtils {
	public:
		static const size_t MAX_SEGMENT_SIZE = 65535;

		template <class StatusType>
		static string getString(StatusType* status, IBlob* blob)
		{
			std::stringstream ss("");			
			auto b = make_unique<char[]>(MAX_SEGMENT_SIZE + 1);
			{
				char* buffer = b.get();
				bool eof = false;
				unsigned int l;
				while (!eof) {
					switch (blob->getSegment(status, MAX_SEGMENT_SIZE, buffer, &l))
					{
					case IStatus::RESULT_OK:
					case IStatus::RESULT_SEGMENT:
						ss.write(buffer, l);
						continue;
					default:
						eof = true;
						break;
					}
				}
			}
			return ss.str();
		}

		template <class StatusType>
		static void setString(StatusType* status, IBlob* blob, const string& str)
		{
			size_t str_len = str.length();
			size_t offset = 0;
			auto b = make_unique<char[]>(MAX_SEGMENT_SIZE + 1);
			{
				char* buffer = b.get();
				while (str_len > 0) {
					const auto len = static_cast<unsigned int>(min(str_len, MAX_SEGMENT_SIZE));
					memset(buffer, 0, MAX_SEGMENT_SIZE + 1);
					memcpy(buffer, str.data() + offset, len);
					blob->putSegment(status, len, buffer);
					offset += len;
					str_len -= len;
				}
			}
		}
	};


	template <class StatusType>
	const unsigned int getSqlDialect(StatusType* status, IAttachment* att) 
	{
		unsigned int sql_dialect = 1;
		const unsigned char info_options[] = { isc_info_db_sql_dialect, isc_info_end };
		ISC_UCHAR buffer[256];
		att->getInfo(status, sizeof(info_options), info_options, sizeof(buffer), buffer);
		/* Extract the values returned in the result buffer. */
		for (ISC_UCHAR* p = buffer; *p != isc_info_end; ) {
			const unsigned char item = *p++;
			const ISC_SHORT length = static_cast<ISC_SHORT>(isc_portable_integer(p, 2));
			p += 2;
			switch (item) {
			case isc_info_db_sql_dialect:
				sql_dialect = static_cast<unsigned int>(isc_portable_integer(p, length));
				break;
			default:
				break;
			}
			p += length;
		};
		return sql_dialect;
	}

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

	template <class StatusType>
	inline void throwException(StatusType* const status, const char* message)
	{
		ISC_STATUS statusVector[] = {
			isc_arg_gds, isc_random,
			isc_arg_string, (ISC_STATUS)message,
			isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	template <class StatusType>
	IMessageMetadata* prepareTextMetaData(StatusType* status, IMessageMetadata* meta)
	{
		unsigned colCount = meta->getCount(status);
		// make all fields of string type except BLOB
		AutoRelease<IMetadataBuilder> builder(meta->getBuilder(status));
		for (unsigned i = 0; i < colCount; i++) {
			unsigned dataType = meta->getType(status, i);
			switch (dataType) {
			case SQL_VARYING:
				break;
			case SQL_TEXT:
				builder->setType(status, i, SQL_VARYING);
				break;
			case SQL_SHORT:
			case SQL_LONG:
			case SQL_INT64:
#if FB_API_VER >= 40
			case SQL_INT128:
#endif
				builder->setType(status, i, SQL_VARYING);
				builder->setLength(status, i, 40 * 4);
				break;
			case SQL_FLOAT:
			case SQL_D_FLOAT:
			case SQL_DOUBLE:
				builder->setType(status, i, SQL_VARYING);
				builder->setLength(status, i, 50 * 4);
				break;
			case SQL_BOOLEAN:
				builder->setType(status, i, SQL_VARYING);
				builder->setLength(status, i, 5 * 4);
				break;
			case SQL_TYPE_DATE:
			case SQL_TYPE_TIME:
			case SQL_TIMESTAMP:
				builder->setType(status, i, SQL_VARYING);
				builder->setLength(status, i, 35 * 4);
				break;
#if FB_API_VER >= 40
			case SQL_TIME_TZ:
			case SQL_TIMESTAMP_TZ:
				builder->setType(status, i, SQL_VARYING);
				builder->setLength(status, i, 42 * 4);
				break;
			case SQL_DEC16:
			case SQL_DEC34:
				builder->setType(status, i, SQL_VARYING);
				builder->setLength(status, i, 60 * 4);
				break;
#endif
			}
		}
		return builder->getMetadata(status);
	}
}

#endif	// FB_BLOB_UTILS_H