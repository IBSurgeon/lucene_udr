/**
 *  Various helper functions.
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

#include "FBUtils.h"
#include "FBAutoPtr.h"
#include <sstream>
#include <memory>
#include <algorithm>
#include <cstdarg>
#include <vector>

using namespace Firebird;

namespace {

    int64_t portable_integer(const unsigned char* ptr, short length);

    int64_t portable_integer(const unsigned char* ptr, short length)
    {
        if (!ptr || length <= 0 || length > 8)
            return 0;

        int64_t value = 0;
        int shift = 0;

        while (--length > 0) {
            value += (static_cast<int64_t>(*ptr++)) << shift;
            shift += 8;
        }

        value += (static_cast<int64_t>(static_cast<char>(*ptr))) << shift;

        return value;
    }
}

namespace LuceneUDR
{
    constexpr unsigned int BUFFER_LARGE = 2048;
    constexpr size_t MAX_SEGMENT_SIZE = 65535;

    std::string BlobUtils::getString(ThrowStatusWrapper* status, IBlob* blob)
    {
        std::stringstream ss;
        auto buffer = std::vector<char>(MAX_SEGMENT_SIZE);
        {
            bool eof = false;
            unsigned int l;
            while (!eof) {
                switch (blob->getSegment(status, MAX_SEGMENT_SIZE, buffer.data(), &l))
                {
                case IStatus::RESULT_OK:
                case IStatus::RESULT_SEGMENT:
                    ss.write(buffer.data(), l);
                    continue;
                default:
                    eof = true;
                    break;
                }
            }
        }
        return ss.str();
    }

    std::string BlobUtils::getString(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att, Firebird::ITransaction* tra, ISC_QUAD* blobIdPtr)
    {
        if (!blobIdPtr) {
            return "";
        }
        std::stringstream ss;
        AutoRelease<IBlob> blob(att->openBlob(status, tra, blobIdPtr, 0, nullptr));
        auto buffer = std::vector<char>(MAX_SEGMENT_SIZE);
        {
            bool eof = false;
            unsigned int l;
            while (!eof) {
                switch (blob->getSegment(status, MAX_SEGMENT_SIZE, buffer.data(), &l)) {
                case IStatus::RESULT_OK:
                case IStatus::RESULT_SEGMENT:
                    ss.write(buffer.data(), l);
                    continue;
                default:
                    eof = true;
                    break;
                }
            }
        }
        blob->close(status);
        blob.release();
        return ss.str();
    }

    void BlobUtils::setString(ThrowStatusWrapper* status, IBlob* blob, const std::string& str)
    {
        size_t str_len = str.length();
        size_t offset = 0;
        auto buffer = std::vector<char>(MAX_SEGMENT_SIZE);
        {
            while (str_len > 0) {
                const auto len = static_cast<unsigned int>(std::min(str_len, MAX_SEGMENT_SIZE));
                memcpy(buffer.data(), str.data() + offset, len);
                blob->putSegment(status, len, buffer.data());
                offset += len;
                str_len -= len;
            }
        }
    }

    const unsigned int getSqlDialect(ThrowStatusWrapper* status, IAttachment* att)
    {
        unsigned int sql_dialect = 1;
        const unsigned char info_options[] = { isc_info_db_sql_dialect, isc_info_end };
        ISC_UCHAR buffer[256];
        att->getInfo(status, sizeof(info_options), info_options, sizeof(buffer), buffer);
        /* Extract the values returned in the result buffer. */
        for (ISC_UCHAR* p = buffer; *p != isc_info_end; ) {
            const unsigned char item = *p++;
            const ISC_SHORT length = static_cast<ISC_SHORT>(portable_integer(p, 2));
            p += 2;
            switch (item) {
            case isc_info_db_sql_dialect:
                sql_dialect = static_cast<unsigned int>(portable_integer(p, length));
                break;
            default:
                break;
            }
            p += length;
        };
        return sql_dialect;
    }
    
    IscRandomStatus IscRandomStatus::createFmtStatus(const char* message, ...)
    {
        char buffer[BUFFER_LARGE];

        va_list ptr;
        va_start(ptr, message);
        vsnprintf(buffer, BUFFER_LARGE, message, ptr);
        va_end(ptr);

        return IscRandomStatus(buffer);
    }

    void throwException(Firebird::ThrowStatusWrapper* const status, const char* message, ...)
    {
        char buffer[BUFFER_LARGE];

        va_list ptr;
        va_start(ptr, message);
        vsnprintf(buffer, BUFFER_LARGE, message, ptr);
        va_end(ptr);

        ISC_STATUS statusVector[] = {
            isc_arg_gds, isc_random,
            isc_arg_string, (ISC_STATUS)buffer,
            isc_arg_end
        };
        throw Firebird::FbException(status, statusVector);
    }

    IMessageMetadata* prepareTextMetaData(ThrowStatusWrapper* status, IMessageMetadata* meta)
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
                builder->setType(status, i, SQL_VARYING);
                builder->setLength(status, i, 12);
                builder->setCharSet(status, i, 0);
                break;
            case SQL_INT64:
                builder->setType(status, i, SQL_VARYING);
                builder->setLength(status, i, 20);
                builder->setCharSet(status, i, 0);
                break;
#if FB_API_VER >= 40
            case SQL_INT128:
                builder->setType(status, i, SQL_VARYING);
                builder->setLength(status, i, Firebird::IInt128::STRING_SIZE);
                builder->setCharSet(status, i, 0);
                break;
#endif
            case SQL_FLOAT:
            case SQL_D_FLOAT:
            case SQL_DOUBLE:
                builder->setType(status, i, SQL_VARYING);
                builder->setLength(status, i, 50 * 4);
                builder->setCharSet(status, i, 0);
                break;
            case SQL_BOOLEAN:
                builder->setType(status, i, SQL_VARYING);
                builder->setLength(status, i, 5);
                builder->setCharSet(status, i, 0);
                break;
            case SQL_TYPE_DATE:
                builder->setType(status, i, SQL_VARYING);
                builder->setLength(status, i, 10);
                builder->setCharSet(status, i, 0);
                break;
            case SQL_TYPE_TIME:
            case SQL_TIMESTAMP:
                builder->setType(status, i, SQL_VARYING);
                builder->setLength(status, i, 35 * 4);
                builder->setCharSet(status, i, 0);
                break;
#if FB_API_VER >= 40
            case SQL_TIME_TZ:
            case SQL_TIMESTAMP_TZ:
                builder->setType(status, i, SQL_VARYING);
                builder->setLength(status, i, 42 * 4);
                builder->setCharSet(status, i, 0);
                break;
            case SQL_DEC16:
                builder->setType(status, i, SQL_VARYING);
                builder->setLength(status, i, Firebird::IDecFloat16::STRING_SIZE);
                builder->setCharSet(status, i, 0);
                break;
            case SQL_DEC34:
                builder->setType(status, i, SQL_VARYING);
                builder->setLength(status, i, Firebird::IDecFloat34::STRING_SIZE);
                builder->setCharSet(status, i, 0);
                break;
#endif
            }
        }
        return builder->getMetadata(status);
    }
}