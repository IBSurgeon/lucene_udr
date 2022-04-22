#ifndef LUCENE_FILES_H
#define LUCENE_FILES_H

/**
 *  Lucene full-text index file helper.
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

#include "lucene++/LuceneHeaders.h"
#include "lucene++/FileUtils.h"
#include "lucene++/IndexFileNameFilter.h"
#include "lucene++/IndexFileNames.h"
#include <list>

using namespace Lucene;
using namespace std;

namespace LuceneUDR 
{

	class LuceneFileHelper final
	{
	public:
		LuceneFileHelper()
			: ftsIndexDir(nullptr)
		{}

		LuceneFileHelper(FSDirectoryPtr fsDirectory)
			: ftsIndexDir(fsDirectory)
		{
		}

		void setDirectory(FSDirectoryPtr fsDirectory)
		{
			ftsIndexDir = fsDirectory;
		}

		list<String> getIndexFileNames()
		{
			auto allFileNames = ftsIndexDir->listAll();
			list<String> fileNames(allFileNames.begin(), allFileNames.end());
			fileNames.remove_if([this](auto& fileName) {
				return !IndexFileNameFilter::getFilter()->accept(ftsIndexDir->getFile(), fileName);
			});
			return fileNames;
		}

		size_t getIndexSize()
		{
			size_t size = 0;
			for (const auto& fileName : getIndexFileNames())
			{
				size += ftsIndexDir->fileLength(fileName);
			}
			return size;
		}

		size_t getFileSize(const String& fileName)
		{
			return ftsIndexDir->fileLength(fileName);
		}

		string getIndexFileType(const String &fileName)
		{			
			if (fileName == IndexFileNames::SEGMENTS()) {
				// index segment file
				return "SEGMENTS";
			}
			if (fileName == IndexFileNames::SEGMENTS_GEN()) {
				// generation reference file name
				return "SEGMENTS_GEN";
			}
			if (fileName == IndexFileNames::DELETABLE()) {
				// index deletable file (only used in pre-lockless indices)
				return "DELETABLE";
			}
			if (endsWith(fileName, IndexFileNames::NORMS_EXTENSION())) {
				// norms file
				return "NORMS";
			}
			if (endsWith(fileName, IndexFileNames::FREQ_EXTENSION())) {
				// freq postings file
				return "FREQ";
			}
			if (endsWith(fileName, IndexFileNames::PROX_EXTENSION())) {
				// prox postings file
				return "PROX";
			}
			if (endsWith(fileName, IndexFileNames::TERMS_EXTENSION())) {
				// terms file
				return "TERMS";
			}
			if (endsWith(fileName, IndexFileNames::TERMS_INDEX_EXTENSION())) {
				// terms index file
				return "TERMS_INDEX";
			}
			if (endsWith(fileName, IndexFileNames::FIELDS_INDEX_EXTENSION())) {
				// stored field index
				return "FIELDS_INDEX";
			}
			if (endsWith(fileName, IndexFileNames::FIELDS_EXTENSION())) {
				// stored field data
				return "FIELDS";
			}
			if (endsWith(fileName, IndexFileNames::VECTORS_FIELDS_EXTENSION())) {
				// Term Vector Fields
				return "VECTORS_FIELDS";
			}
			if (endsWith(fileName, IndexFileNames::VECTORS_DOCUMENTS_EXTENSION())) {
				// Term Vector Documents
				return "VECTORS_DOCUMENTS";
			}
			if (endsWith(fileName, IndexFileNames::VECTORS_INDEX_EXTENSION())) {
				// Term Vector Index
				return "VECTORS_INDEX";
			}
			if (endsWith(fileName, IndexFileNames::COMPOUND_FILE_EXTENSION())) {
				// Compound File
				return "COMPOUND_FILE";
			}
			if (endsWith(fileName, IndexFileNames::COMPOUND_FILE_STORE_EXTENSION())) {
				// Compound File for doc store files
				return "COMPOUND_FILE_STORE";
			}
			if (endsWith(fileName, IndexFileNames::DELETES_EXTENSION())) {
				// Deletes
				return "DELETES";
			}
			if (endsWith(fileName, IndexFileNames::FIELD_INFOS_EXTENSION())) {
				// field infos
				return "FIELD_INFOS";
			}
			if (endsWith(fileName, IndexFileNames::PLAIN_NORMS_EXTENSION())) {
				// plain norms
				return "PLAIN_NORMS";
			}
			if (endsWith(fileName, IndexFileNames::SEPARATE_NORMS_EXTENSION())) {
				// separate norms
				return "SEPARATE_NORMS";
			}
			if (endsWith(fileName, IndexFileNames::GEN_EXTENSION())) {
				// gen file
				return "SEGMENTS_GEN";
			}
			
			return "";
		}

	private:
		FSDirectoryPtr ftsIndexDir;

		bool endsWith(const String& str, const String& suffix)
		{
			return str.size() >= suffix.size() && 0 == str.compare(str.size() - suffix.size(), suffix.size(), suffix);
		}
	};

}

#endif // LUCENE_FILES_H
