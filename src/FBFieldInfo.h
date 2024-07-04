#ifndef FB_FIELD_INFO_H
#define FB_FIELD_INFO_H

/**
 *  Utilities for getting information about query fields and their values.
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

#include <string>
#include <vector>

#include "LuceneUdr.h"


namespace FTSMetadata
{

    class FbFieldInfo {
    public:
        std::string fieldName;
        std::string relationName;
        std::string owner;
        std::string alias;
        
        unsigned fieldIndex = 0;
        unsigned dataType = 0;
        int subType = 0;
        unsigned length = 0;
        int scale = 0;
        unsigned charSet = 0;
        unsigned offset = 0;
        unsigned nullOffset = 0;
        // lucene specific
        std::wstring ftsFieldName;
        double ftsBoost = 1.0;
        bool ftsBoostNull = true;
        bool ftsKey = false;

        bool nullable = false;

        FbFieldInfo() = default;
        FbFieldInfo(Firebird::ThrowStatusWrapper* status, Firebird::IMessageMetadata* const meta, unsigned index);

        // non-copyable
        FbFieldInfo(const FbFieldInfo& rhs) = delete;
        FbFieldInfo& operator=(const FbFieldInfo& rhs) = delete;
        // movable
        FbFieldInfo(FbFieldInfo&& rhs) noexcept = default;
        FbFieldInfo& operator=(FbFieldInfo&& rhs) noexcept = default;


        bool isNull(unsigned char* buffer) const {
            return *reinterpret_cast<short*>(buffer + nullOffset);
        }

        FB_BOOLEAN getBooleanValue(unsigned char* buffer) const {
            return *reinterpret_cast<FB_BOOLEAN*>(buffer + offset);
        }

        ISC_SHORT getShortValue(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_SHORT*>(buffer + offset);
        }

        ISC_LONG getLongValue(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_LONG*>(buffer + offset);
        }

        ISC_INT64 getInt64Value(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_INT64*>(buffer + offset);
        }

        float getFloatValue(unsigned char* buffer) const {
            return *reinterpret_cast<float*>(buffer + offset);
        }

        double getDoubleValue(unsigned char* buffer) const {
            return *reinterpret_cast<double*>(buffer + offset);
        }

        ISC_DATE getDateValue(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_DATE*>(buffer + offset);
        }

        ISC_TIME getTimeValue(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_TIME*>(buffer + offset);
        }

        ISC_TIMESTAMP getTimestampValue(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_TIMESTAMP*>(buffer + offset);
        }

        ISC_QUAD getQuadValue(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_QUAD*>(buffer + offset);
        }

        ISC_QUAD* getQuadPtr(unsigned char* buffer) const
        {
            return reinterpret_cast<ISC_QUAD*>(buffer + offset);
        }

#if FB_API_VER >= 40
        FB_I128 getInt128Value(unsigned char* buffer) const {
            return *reinterpret_cast<FB_I128*>(buffer + offset);
        }

        FB_DEC16 getDecFloat16Value(unsigned char* buffer) const {
            return *reinterpret_cast<FB_DEC16*>(buffer + offset);
        }

        FB_DEC34 getDecFloat34Value(unsigned char* buffer) const {
            return *reinterpret_cast<FB_DEC34*>(buffer + offset);
        }

        ISC_TIME_TZ getTimeTzValue(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_TIME_TZ*>(buffer + offset);
        }

        ISC_TIME_TZ_EX getTimeTzExValue(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_TIME_TZ_EX*>(buffer + offset);
        }

        ISC_TIMESTAMP_TZ getTimestampTzValue(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_TIMESTAMP_TZ*>(buffer + offset);
        }

        ISC_TIMESTAMP_TZ_EX getTimestampTzExValue(unsigned char* buffer) const {
            return *reinterpret_cast<ISC_TIMESTAMP_TZ_EX*>(buffer + offset);
        }
#endif

        unsigned short getOctetsLength(unsigned char* buffer) const {
            switch (dataType)
            {
            case SQL_TEXT:
                return static_cast<unsigned short>(length);
            case SQL_VARYING:
                return *reinterpret_cast<unsigned short*>(buffer + offset);
            default:
                return 0;
            }
        }

        char* getCharValue(unsigned char* buffer) const {
            switch (dataType)
            {
            case SQL_TEXT:
                return reinterpret_cast<char*>(buffer + offset);
            case SQL_VARYING:
                return reinterpret_cast<char*>(buffer + offset + sizeof(short));
            default:
                return nullptr;
            }
        }

        unsigned char* getBinaryValue(unsigned char* buffer) const {
            switch (dataType)
            {
            case SQL_TEXT:
                return buffer + offset;
            case SQL_VARYING:
                return buffer + offset + sizeof(short);
            default:
                return nullptr;
            }
        }

        bool isBlob() const {
            return dataType == SQL_BLOB;
        }

        bool isBinary() const {
            switch (dataType) {
            case SQL_TEXT:
            case SQL_VARYING:
            {
                return (charSet == CS_BINARY);
            }
            case SQL_BLOB:
            {
                return (subType == 0);
            }
            default:
                return false;
            }
        }

        bool isInt() const {
            switch (dataType) {
            case SQL_SHORT:
            case SQL_LONG:
            case SQL_INT64:
            {
                return (scale == 0);
            }
            default:
                return false;
            }
        }

        std::string getStringValue(
            Firebird::ThrowStatusWrapper* status, 
            Firebird::IAttachment* att, 
            Firebird::ITransaction* tra, 
            unsigned char* buffer) const;
    };



    using FbFieldsInfo = std::vector<FbFieldInfo>;

    FbFieldsInfo makeFbFieldsInfo(Firebird::ThrowStatusWrapper* status, Firebird::IMessageMetadata* meta);
}


#endif	// FB_FIELD_INFO_H
