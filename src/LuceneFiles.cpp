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

#include "LuceneFiles.h"

#include "IndexFileNameFilter.h"
#include "IndexFileNames.h"

namespace LuceneUDR
{
    std::list<Lucene::String> LuceneFileHelper::getIndexFileNames()
    {
        auto allFileNames = m_ftsIndexDir->listAll();
        std::list<Lucene::String> fileNames(allFileNames.begin(), allFileNames.end());
        fileNames.remove_if([this](auto& fileName) {
            return !Lucene::IndexFileNameFilter::getFilter()->accept(m_ftsIndexDir->getFile(), fileName);
        });
        return fileNames;
    }

    std::string LuceneFileHelper::getIndexFileType(const Lucene::String& fileName)
    {
        if (fileName == Lucene::IndexFileNames::SEGMENTS()) {
            // index segment file
            return "SEGMENTS";
        }
        if (fileName == Lucene::IndexFileNames::SEGMENTS_GEN()) {
            // generation reference file name
            return "SEGMENTS_GEN";
        }
        if (fileName == Lucene::IndexFileNames::DELETABLE()) {
            // index deletable file (only used in pre-lockless indices)
            return "DELETABLE";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::NORMS_EXTENSION())) {
            // norms file
            return "NORMS";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::FREQ_EXTENSION())) {
            // freq postings file
            return "FREQ";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::PROX_EXTENSION())) {
            // prox postings file
            return "PROX";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::TERMS_EXTENSION())) {
            // terms file
            return "TERMS";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::TERMS_INDEX_EXTENSION())) {
            // terms index file
            return "TERMS_INDEX";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::FIELDS_INDEX_EXTENSION())) {
            // stored field index
            return "FIELDS_INDEX";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::FIELDS_EXTENSION())) {
            // stored field data
            return "FIELDS";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::VECTORS_FIELDS_EXTENSION())) {
            // Term Vector Fields
            return "VECTORS_FIELDS";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::VECTORS_DOCUMENTS_EXTENSION())) {
            // Term Vector Documents
            return "VECTORS_DOCUMENTS";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::VECTORS_INDEX_EXTENSION())) {
            // Term Vector Index
            return "VECTORS_INDEX";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::COMPOUND_FILE_EXTENSION())) {
            // Compound File
            return "COMPOUND_FILE";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::COMPOUND_FILE_STORE_EXTENSION())) {
            // Compound File for doc store files
            return "COMPOUND_FILE_STORE";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::DELETES_EXTENSION())) {
            // Deletes
            return "DELETES";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::FIELD_INFOS_EXTENSION())) {
            // field infos
            return "FIELD_INFOS";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::PLAIN_NORMS_EXTENSION())) {
            // plain norms
            return "PLAIN_NORMS";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::SEPARATE_NORMS_EXTENSION())) {
            // separate norms
            return "SEPARATE_NORMS";
        }
        if (endsWith(fileName, Lucene::IndexFileNames::GEN_EXTENSION())) {
            // gen file
            return "SEGMENTS_GEN";
        }

        return "";
    }
}