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

using namespace Lucene;
using namespace std;

namespace LuceneUDR 
{

	class LuceneFileHelper final
	{
	public:
		LuceneFileHelper()
			: m_ftsIndexDir(nullptr)
		{}

		LuceneFileHelper(FSDirectoryPtr fsDirectory)
			: m_ftsIndexDir(fsDirectory)
		{
		}

		void setDirectory(FSDirectoryPtr fsDirectory)
		{
			m_ftsIndexDir = fsDirectory;
		}

		list<String> getIndexFileNames();

		size_t getIndexSize()
		{
			size_t size = 0;
			for (const auto& fileName : getIndexFileNames())
			{
				size += m_ftsIndexDir->fileLength(fileName);
			}
			return size;
		}

		size_t getFileSize(const String& fileName)
		{
			return m_ftsIndexDir->fileLength(fileName);
		}

		string getIndexFileType(const String& fileName);

	private:
		FSDirectoryPtr m_ftsIndexDir;

		inline bool endsWith(const String& str, const String& suffix)
		{
			return str.size() >= suffix.size() && 0 == str.compare(str.size() - suffix.size(), suffix.size(), suffix);
		}
	};

}

#endif // LUCENE_FILES_H
