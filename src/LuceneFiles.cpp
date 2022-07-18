#include "LuceneFiles.h"
#include "IndexFileNameFilter.h"
#include "IndexFileNames.h"
#include "FileUtils.h"

using namespace Lucene;
using namespace std;

namespace LuceneUDR
{
	list<String> LuceneFileHelper::getIndexFileNames()
	{
		auto allFileNames = m_ftsIndexDir->listAll();
		list<String> fileNames(allFileNames.begin(), allFileNames.end());
		fileNames.remove_if([this](auto& fileName) {
			return !IndexFileNameFilter::getFilter()->accept(m_ftsIndexDir->getFile(), fileName);
			});
		return fileNames;
	}

	string LuceneFileHelper::getIndexFileType(const String& fileName)
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
}