#ifndef LUCENE_FILES_H
#define LUCENE_FILES_H

/**
 *  Lucene full-text index file helper.
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

#include "LuceneHeaders.h"
#include <list>

namespace LuceneUDR 
{

    class LuceneFileHelper final
    {
    public:
        LuceneFileHelper()
            : m_ftsIndexDir(nullptr)
        {}

        LuceneFileHelper(Lucene::FSDirectoryPtr fsDirectory)
            : m_ftsIndexDir(fsDirectory)
        {
        }

        void setDirectory(Lucene::FSDirectoryPtr fsDirectory)
        {
            m_ftsIndexDir = fsDirectory;
        }

        std::list<Lucene::String> getIndexFileNames();

        size_t getIndexSize()
        {
            size_t size = 0;
            for (const auto& fileName : getIndexFileNames())
            {
                size += m_ftsIndexDir->fileLength(fileName);
            }
            return size;
        }

        size_t getFileSize(const Lucene::String& fileName)
        {
            return m_ftsIndexDir->fileLength(fileName);
        }

        static std::string getIndexFileType(const Lucene::String& fileName);

    private:
        Lucene::FSDirectoryPtr m_ftsIndexDir;

        static bool endsWith(const Lucene::String& str, const Lucene::String& suffix)
        {
            return str.size() >= suffix.size() && 0 == str.compare(str.size() - suffix.size(), suffix.size(), suffix);
        }
    };

}

#endif // LUCENE_FILES_H
