#ifndef FTS_UTILS_H
#define FTS_UTILS_H

/**
 *  Various utilities to support full-text indexes.
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

#include <filesystem> 

#include "LuceneUdr.h"

namespace fs = std::filesystem;

namespace LuceneUDR
{

    /// <summary>
    /// Returns the directory where full-text indexes are located.
    /// </summary>
    /// 
    /// <param name="context">The context of the external routine.</param>
    /// 
    /// <returns>Full path to full-text index directory</returns>
    fs::path getFtsDirectory(Firebird::ThrowStatusWrapper* status, Firebird::IExternalContext* context);

    // error_code overloads: a thrown filesystem_error would surface as "Unrecognized C++ exception"
    inline bool createIndexDirectory(const fs::path& indexDir, std::error_code& ec)
    {
        if (!fs::is_directory(indexDir, ec)) {
            return fs::create_directory(indexDir, ec);
        }
        return true;
    }

    inline bool removeIndexDirectory(const fs::path& indexDir, std::error_code& ec)
    {
        if (fs::is_directory(indexDir, ec)) {
            const auto removed = fs::remove_all(indexDir, ec);
            return !ec && removed > 0;
        }
        return !ec;
    }

}

#endif // FTS_UTILS_H
