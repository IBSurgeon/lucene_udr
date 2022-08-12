#include "FTSUtils.h"
#include "inicpp.h"

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

namespace LuceneUDR
{

	/// <summary>
	/// Returns the directory where full-text indexes are located.
	/// </summary>
	/// 
	/// <param name="status">Status. </param>
	/// <param name="context">The context of the external routine.</param>
	/// 
	/// <returns>Full path to full-text index directory</returns>
	const fs::path getFtsDirectory(ThrowStatusWrapper* const status, IExternalContext* const context) {
		const auto pluginManager = context->getMaster()->getPluginManager();
		IConfigManager* configManager = context->getMaster()->getConfigManager();

		const string databaseName(context->getDatabaseName());
		const string rootDir(configManager->getRootDirectory());
		const fs::path rootDirPath = rootDir;

		const fs::path confFilePath = rootDirPath / "fts.conf";
		AutoRelease<IConfig> conf = pluginManager->getConfig(status, confFilePath.string().c_str());
		if (conf) {
			AutoRelease<IConfigEntry> ftsEntry(conf->findValue(status, "database", context->getDatabaseName()));
			if (ftsEntry) {
				AutoRelease<IConfig> subConf(ftsEntry->getSubConfig(status));
				if (subConf) {
					AutoRelease<IConfigEntry> dirEntry(subConf->find(status, "ftsDirectory"));
					if (dirEntry) {
						fs::path ftsDirectoryPath(dirEntry->getValue());
						return ftsDirectoryPath;
					}
				}
			}
		}

		const fs::path iniFilePath = rootDirPath / "fts.ini";
#ifdef WIN32_LEAN_AND_MEAN
		ini::IniFileCaseInsensitive iniFile;
#else
		ini::IniFile iniFile;
#endif
		iniFile.load(iniFilePath.u8string());
		auto section = iniFile[databaseName];
		const auto ftsDirectory = section["ftsDirectory"].as<string>();
		fs::path ftsDirectoryPath = ftsDirectory;
		return ftsDirectoryPath;
	}

}
