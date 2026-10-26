#ifndef FB_BLOB_UTILS_H
#define FB_BLOB_UTILS_H

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

#include <string>
#include <string_view>
#include <vector>

#include "LuceneUdr.h"



namespace LuceneUDR
{

    std::vector<unsigned char> readBinaryFromBlob(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att,
        Firebird::ITransaction* tra, ISC_QUAD* blobIdPtr);

    std::string readStringFromBlob(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att, 
        Firebird::ITransaction* tra, ISC_QUAD* blobIdPtr);

    void writeStringToBlob(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att, 
        Firebird::ITransaction* tra, ISC_QUAD* blobIdPtr, std::string_view str);


    unsigned int getSqlDialect(Firebird::ThrowStatusWrapper* status, Firebird::IAttachment* att);

    /// <summary>
    /// Escapes the name of the metadata object depending on the SQL dialect. 
    /// </summary>
    /// 
    /// <param name="sqlDialect">SQL dialect</param>
    /// <param name="name">Metadata object name</param>
    /// 
    /// <returns>Returns the escaped name of the metadata object.</returns>
    inline std::string escapeMetaName(const unsigned int sqlDialect, const std::string& name)
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

    [[noreturn]]
    void throwException(Firebird::ThrowStatusWrapper* const status, const char* message, ...);

    struct IscRandomStatus {
    public:
        explicit IscRandomStatus(const std::string& message)
            : message(message)
        {
        }

        explicit IscRandomStatus(const char* message)
            : message(message)
        {
        }

        explicit IscRandomStatus(const std::exception& e)
            : message(e.what())
        {
        }

        // The message is owned, so the vector stays valid after createFmtStatus() returns or the object is copied
        operator const ISC_STATUS* () const
        {
            statusVector[3] = (ISC_STATUS)message.c_str();
            return statusVector;
        }

        static IscRandomStatus createFmtStatus(const char* message, ...);

    private:
        std::string message;
        mutable ISC_STATUS statusVector[5] = { isc_arg_gds, isc_random, isc_arg_string, 0, isc_arg_end };
    };

    Firebird::IMessageMetadata* prepareTextMetaData(Firebird::ThrowStatusWrapper* status, Firebird::IMessageMetadata* meta);

    std::string binary_to_hex(const unsigned char* data, size_t size);

    std::vector<unsigned char> hex_to_binary(std::string_view input);
}

#endif // FB_BLOB_UTILS_H
