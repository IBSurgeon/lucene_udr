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

#include "FTSUtils.h"

#include <string>

#include "FBUtils.h"
#include "inicpp.h"

using namespace Firebird;

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
    fs::path getFtsDirectory(ThrowStatusWrapper* status, IExternalContext* context) 
    try {
        const auto pluginManager = context->getMaster()->getPluginManager();
        IConfigManager* configManager = context->getMaster()->getConfigManager();

        const std::string databaseName(context->getDatabaseName());
        const std::string rootDir(configManager->getRootDirectory());
        const fs::path rootDirPath = rootDir;

        const fs::path confFilePath = rootDirPath / "fts.conf";
        if (fs::exists(confFilePath)) {
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
                        else {
                            IscRandomStatus statusVector = IscRandomStatus::createFmtStatus(R"(Key ftsDirectory not found in entry "database = %s" file fts.conf)", databaseName.c_str());
                            throw Firebird::FbException(status, statusVector);
                        }
                    }
                    else {
                        IscRandomStatus statusVector = IscRandomStatus::createFmtStatus(R"(Key ftsDirectory not found in entry "database = %s" file fts.conf)", databaseName.c_str());
                        throw Firebird::FbException(status, statusVector);
                    }
                }
                else {
                    IscRandomStatus statusVector = IscRandomStatus::createFmtStatus(R"(Entry "database = %s" not found in file fts.conf)", databaseName.c_str());
                    throw Firebird::FbException(status, statusVector);
                }
            }
        }

        const fs::path iniFilePath = rootDirPath / "fts.ini";
        if (fs::exists(iniFilePath)) {
#ifdef WIN32_LEAN_AND_MEAN
            ini::IniFileCaseInsensitive iniFile;
#else
            ini::IniFile iniFile;
#endif
            iniFile.load(iniFilePath.u8string());
            auto secIt = iniFile.find(databaseName);
            if (secIt == iniFile.end()) {
                IscRandomStatus statusVector = IscRandomStatus::createFmtStatus(R"(Section "%s" not found in file fts.ini)", databaseName.c_str());
                throw Firebird::FbException(status, statusVector);
            }
            auto&& section = secIt->second;
            auto keyIt = section.find("ftsDirectory");
            if (keyIt == section.end()) {
                IscRandomStatus statusVector = IscRandomStatus::createFmtStatus(R"(Key ftsDirectory not found in section "%s" file fts.ini)", databaseName.c_str());
                throw Firebird::FbException(status, statusVector);
            }
            auto&& key = keyIt->second;
            const auto ftsDirectory = key.as<std::string>();
            return { ftsDirectory };
        }
        else {
            IscRandomStatus statusVector("Settings file fts.ini or fts.conf not found");
            throw Firebird::FbException(status, statusVector);
        }
    }
    catch (const std::exception& e) {
        IscRandomStatus statusVector(e);
        throw Firebird::FbException(status, statusVector);
    }
}
