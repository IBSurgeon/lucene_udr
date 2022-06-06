#include "FTSUtils.h"
#include "inicpp.h"

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

namespace LuceneUDR
{

	/// <summary>
	/// Returns the directory where full-text indexes are located.
	/// </summary>
	/// 
	/// <param name="context">The context of the external routine.</param>
	/// 
	/// <returns>Full path to full-text index directory</returns>
	const fs::path getFtsDirectory(IExternalContext* const context) {
		IConfigManager* configManager = context->getMaster()->getConfigManager();
		const string databaseName(context->getDatabaseName());
		const string rootDir(configManager->getRootDirectory());
		const fs::path rootDirPath = rootDir;
		const fs::path iniFilePath = rootDirPath / "fts.ini";
		ini::IniFile iniFile;
		iniFile.load(iniFilePath.u8string());
		auto section = iniFile[databaseName];
		const auto ftsDirectory = section["ftsDirectory"].as<string>();
		fs::path ftsDirectoryPath = ftsDirectory;
		return ftsDirectoryPath;
	}

}
