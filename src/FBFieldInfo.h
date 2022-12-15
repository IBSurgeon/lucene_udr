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
#include <map>
#include <memory>
#include "LuceneUdr.h"

using namespace std;
using namespace Firebird;

namespace FTSMetadata
{

    template <typename T>
    inline T as(unsigned char* ptr)
    {
        return *((T*)ptr);
    }

    class FbFieldInfo {
    public:
        unsigned fieldIndex = 0;

        string fieldName{""};
        string relationName{""};
        string owner{""};
        string alias{""};
        bool nullable = false;
        unsigned dataType = 0;
        int subType = 0;
        unsigned length = 0;
        int scale = 0;
        unsigned charSet = 0;
        unsigned offset = 0;
        unsigned nullOffset = 0;

        wstring ftsFieldName{L""};
        bool ftsKey = false;
        double ftsBoost = 1.0;
        bool ftsBoostNull = true;

        FbFieldInfo() = default;

        inline bool isNull(unsigned char* buffer) {
            return as<short>(buffer + nullOffset);
        }

        inline FB_BOOLEAN getBooleanValue(unsigned char* buffer) {
            return as<FB_BOOLEAN>(buffer + offset);
        }

        inline ISC_SHORT getShortValue(unsigned char* buffer) {
            return as<ISC_SHORT>(buffer + offset);
        }

        inline ISC_LONG getLongValue(unsigned char* buffer) {
            return as<ISC_LONG>(buffer + offset);
        }

        inline ISC_INT64 getInt64Value(unsigned char* buffer) {
            return as<ISC_INT64>(buffer + offset);
        }

        inline float getFloatValue(unsigned char* buffer) {
            return as<float>(buffer + offset);
        }

        inline double getDoubleValue(unsigned char* buffer) {
            return as<double>(buffer + offset);
        }

        inline ISC_DATE getDateValue(unsigned char* buffer) {
            return as<ISC_DATE>(buffer + offset);
        }

        inline ISC_TIME getTimeValue(unsigned char* buffer) {
            return as<ISC_TIME>(buffer + offset);
        }

        inline ISC_TIMESTAMP getTimestampValue(unsigned char* buffer) {
            return as<ISC_TIMESTAMP>(buffer + offset);
        }

        inline ISC_QUAD getQuadValue(unsigned char* buffer) {
            return as<ISC_QUAD>(buffer + offset);
        }

#if FB_API_VER >= 40
        inline FB_I128 getInt128Value(unsigned char* buffer) {
            return as<FB_I128>(buffer + offset);
        }

        inline FB_DEC16 getDecFloat16Value(unsigned char* buffer) {
            return as<FB_DEC16>(buffer + offset);
        }

        inline FB_DEC34 getDecFloat34Value(unsigned char* buffer) {
            return as<FB_DEC34>(buffer + offset);
        }

        inline ISC_TIME_TZ getTimeTzValue(unsigned char* buffer) {
            return as<ISC_TIME_TZ>(buffer + offset);
        }

        inline ISC_TIME_TZ_EX getTimeTzExValue(unsigned char* buffer) {
            return as<ISC_TIME_TZ_EX>(buffer + offset);
        }

        inline ISC_TIMESTAMP_TZ getTimestampTzValue(unsigned char* buffer) {
            return as<ISC_TIMESTAMP_TZ>(buffer + offset);
        }

        inline ISC_TIMESTAMP_TZ_EX getTimestampTzExValue(unsigned char* buffer) {
            return as<ISC_TIMESTAMP_TZ_EX>(buffer + offset);
        }
#endif

        inline short getOctetsLength(unsigned char* buffer) {
            switch (dataType)
            {
            case SQL_TEXT:
                return length;
            case SQL_VARYING:
                return as<short>(buffer + offset);
            default:
                return 0;
            }
        }

        inline char* getCharValue(unsigned char* buffer) {
            switch (dataType)
            {
            case SQL_TEXT:
                return (char*)(buffer + offset);
            case SQL_VARYING:
                return (char*)(buffer + offset + sizeof(short));
            default:
                return nullptr;
            }
        }

        inline bool isBinary() {
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

        inline bool isInt() {
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

        string getStringValue(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned char* buffer);
    };



    using FbFieldInfoPtr = unique_ptr<FbFieldInfo>;
    using FbFieldInfoVector = vector<FbFieldInfoPtr>;

    class FbFieldsInfo : public FbFieldInfoVector
    {
    private:
        map<string, unsigned> m_fieldByNameMap;
    public:
        FbFieldsInfo() = delete;

        FbFieldsInfo(ThrowStatusWrapper* status, IMessageMetadata* const meta);

        int findFieldByName(const string& fieldName);
    };

    using FbFieldsInfoPtr = unique_ptr<FbFieldsInfo>;

}


#endif	// FB_FIELD_INFO_H