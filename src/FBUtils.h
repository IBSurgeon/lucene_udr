#ifndef FB_BLOB_UTILS_H
#define FB_BLOB_UTILS_H

#include "firebird/UdrCppEngine.h"
#include <string>
#include <sstream>
#include <algorithm>

using namespace std;
using namespace Firebird;

namespace LuceneUDR
{

	const size_t MAX_SEGMENT_SIZE = 65535;

	template <class StatusType> 
	string blob_get_string(StatusType* status, IBlob* blob)
	{
		std::stringstream ss("");
		bool eof = false;
		auto b = make_unique<char[]>(MAX_SEGMENT_SIZE + 1);
		char* buffer = b.get();
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
		return ss.str();
	}

	template <class StatusType> 
	void blob_set_string(StatusType* status, IBlob* blob, const string &str)
	{
		size_t str_len = str.length();
		size_t offset = 0;
		auto b = make_unique<char[]>(MAX_SEGMENT_SIZE + 1);
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

	template <class StatusType>
	const unsigned int getSqlDialect(StatusType* status, IAttachment* att) 
	{
		unsigned int sql_dialect = 1;
		const unsigned char info_options[] = { isc_info_db_sql_dialect, isc_info_end };
		unsigned char buffer[256];
		att->getInfo(status, sizeof(info_options), info_options, sizeof(buffer), buffer);
		/* Extract the values returned in the result buffer. */
		for (unsigned char* p = buffer; *p != isc_info_end; ) {
			const unsigned char item = *p++;
			const int length = isc_vax_integer(reinterpret_cast<ISC_SCHAR*>(p), 2);
			p += 2;
			switch (item) {
			case isc_info_db_sql_dialect:
				sql_dialect = isc_vax_integer(reinterpret_cast<ISC_SCHAR*>(p), length);
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
	static inline string escapeMetaName(const unsigned int sqlDialect, const string& name)
	{
		switch (sqlDialect) {
		case 1:
			return name;
		case 3:
		default:
			return "\"" + name + "\"";
		}
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
			case SQL_INT128:
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
			}
		}
		return builder->getMetadata(status);
	}
}

#endif	// FB_BLOB_UTILS_H