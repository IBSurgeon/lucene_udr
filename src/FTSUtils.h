#ifndef FTS_UTILS_H
#define FTS_UTILS_H

/**
 *  Various utilities to support full-text indexes.
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
#include <filesystem> 

using namespace Firebird;
using namespace std;

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
	const fs::path getFtsDirectory(IExternalContext* const context);

	inline bool createIndexDirectory(const fs::path& indexDir)
	{
		if (!fs::is_directory(indexDir)) {
			return fs::create_directory(indexDir);
		}
		return true;
	}

	inline bool removeIndexDirectory(const fs::path& indexDir)
	{
		if (fs::is_directory(indexDir)) {
			return (fs::remove_all(indexDir) > 0);
		}
		return true;
	}

}

#endif // FTS_UTILS_H