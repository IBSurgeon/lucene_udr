/**
 *  Implementation of procedures and functions of the FTS$STATISTICS package.
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
#include "FTSIndex.h"
#include "FTSUtils.h"
#include "FBUtils.h"
#include "LuceneFiles.h"
#include "LuceneHeaders.h"
#include "FileUtils.h"
#include "IndexFileNames.h"
#include "SegmentInfos.h"
#include "SegmentInfo.h"
#include "IndexFileNameFilter.h"
#include "FieldInfos.h"
#include "FieldInfo.h"
#include "CompoundFileReader.h"
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;

/***
FUNCTION FTS$LUCENE_VERSION ()
RETURNS VARCHAR(20) CHARACTER SET UTF8
DETERMINISTIC
EXTERNAL NAME 'luceneudr!getLuceneVersion'
ENGINE UDR;
***/
FB_UDR_BEGIN_FUNCTION(getLuceneVersion)
	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(80, CS_UTF8), luceneVersion)
	);


	FB_UDR_EXECUTE_FUNCTION
	{
		const string luceneVersion = StringUtils::toUTF8(Constants::LUCENE_VERSION);

		out->luceneVersionNull = false;
		out->luceneVersion.length = static_cast<ISC_USHORT>(luceneVersion.length());
		luceneVersion.copy(out->luceneVersion.str, out->luceneVersion.length);
	}
FB_UDR_END_FUNCTION

/***
PROCEDURE FTS$INDEX_STATISTICS (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
  FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
  FTS$INDEX_STATUS TYPE OF FTS$D_INDEX_STATUS,
  FTS$INDEX_DIRECTORY VARCHAR(255) CHARACTER SET UTF8,
  FTS$INDEX_EXISTS BOOLEAN,
  FTS$INDEX_OPTIMIZED BOOLEAN,
  FTS$HAS_DELETIONS BOOLEAN,
  FTS$NUM_DOCS INTEGER,
  FTS$NUM_DELETED_DOCS INTEGER,
  FTS$NUM_FIELDS SMALLINT,
  FTS$INDEX_SIZE BIGINT
)
EXTERNAL NAME 'luceneudr!getIndexStatistics'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getIndexStatistics)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
	);


	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
		(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
		(FB_INTL_VARCHAR(1020, CS_UTF8), indexDir)
		(FB_BOOLEAN, indexExists)
		(FB_BOOLEAN, isOptimized)
		(FB_BOOLEAN, hasDeletions)
		(FB_INTEGER, numDocs)
		(FB_INTEGER, numDeletedDocs)
		(FB_SMALLINT, numFields)
		(FB_BIGINT, indexSize)
	);

	FB_UDR_CONSTRUCTOR
	, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
		char* name, unsigned nameSize)
	{
		// Forced internal request encoding to UTF8
		memset(name, 0, nameSize);

		const string charset = "UTF8";
		charset.copy(name, charset.length());
	}

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->index_name.str, in->index_name.length);

		const auto& ftsDirectoryPath = getFtsDirectory(context);
		// check if there is a directory for full-text indexes
		if (!fs::is_directory(ftsDirectoryPath)) {
			const string error_message = string_format(R"(Fts directory "%s" not exists)"s, ftsDirectoryPath.u8string());
			throwException(status, error_message.c_str());
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		out->analyzerNameNull = true;
		out->indexStatusNull = true;
		out->indexDirNull = true;
		out->indexExistsNull = true;
		out->isOptimizedNull = true;
		out->hasDeletionsNull = true;
		out->numDocsNull = true;
		out->numDeletedDocsNull = true;
		out->numFieldsNull = true;
		out->indexSizeNull = true;

		try {
			// check for index existence
			auto ftsIndex = make_unique<FTSIndex>();
			procedure->indexRepository->getIndex(status, att, tra, sqlDialect, ftsIndex, indexName);

			out->analyzerNameNull = false;
			out->analyzerName.length = static_cast<ISC_USHORT>(ftsIndex->analyzer.length());
			ftsIndex->analyzer.copy(out->analyzerName.str, out->analyzerName.length);

			const auto& indexDirectoryPath = ftsDirectoryPath / indexName;

			const string indexDir = indexDirectoryPath.u8string();
			out->indexDirNull = false;
			out->indexDir.length = static_cast<ISC_USHORT>(indexDir.length());
			indexDir.copy(out->indexDir.str, out->indexDir.length);

			out->indexExistsNull = false;
			out->indexExists = true;
			// Check if the index directory exists
			if (!fs::is_directory(indexDirectoryPath)) {
				// index created, but not build
				ftsIndex->status = "N";
				out->indexExists = false;
			}
			else {
				const auto& ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
				if (!IndexReader::indexExists(ftsIndexDir)) {
					// index created, but not build
					ftsIndex->status = "N";
					out->indexExists = false;
				}
				else {
					const auto& reader = IndexReader::open(ftsIndexDir, true);
					LuceneFileHelper luceneFileHelper(ftsIndexDir);

					out->isOptimizedNull = false;
					out->isOptimized = reader->isOptimized();

					out->hasDeletionsNull = false;
					out->hasDeletions = reader->hasDeletions();

					out->numDocsNull = false;
					out->numDocs = reader->numDocs();

					out->numDeletedDocsNull = false;
					out->numDeletedDocs = reader->numDeletedDocs();

					//reader->getUniqueTermCount();

					out->numFieldsNull = false;
					auto fieldNames = reader->getFieldNames(IndexReader::FIELD_OPTION_ALL);
					out->numFields = fieldNames.size();


					// calculate index size
					out->indexSizeNull = false;
					out->indexSize = luceneFileHelper.getIndexSize();

					reader->close();
				}
				ftsIndexDir->close();
			}
			out->indexStatusNull = false;
			out->indexStatus.length = static_cast<ISC_USHORT>(ftsIndex->status.length());
			ftsIndex->status.copy(out->indexStatus.str, out->indexStatus.length);
		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	AutoRelease<IAttachment> att{nullptr};
	AutoRelease<ITransaction> tra{nullptr};
	bool fetched = false;

	FB_UDR_FETCH_PROCEDURE
	{
		if (fetched) {
			return false;
		}
		fetched = !fetched;
		return true;
	}
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$INDEX_FIELDS (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
  FTS$FIELD_NAME VARCHAR(127) CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!getIndexFields'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getIndexFields)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
	);


	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(508, CS_UTF8), fieldName)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
		char* name, unsigned nameSize)
	{
		// Forced internal request encoding to UTF8
		memset(name, 0, nameSize);

		const string charset = "UTF8";
		charset.copy(name, charset.length());
	}

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->index_name.str, in->index_name.length);

		const auto& ftsDirectoryPath = getFtsDirectory(context);
		// check if there is a directory for full-text indexes
		if (!fs::is_directory(ftsDirectoryPath)) {
			const string error_message = string_format(R"(Fts directory "%s" not exists)"s, ftsDirectoryPath.u8string());
			throwException(status, error_message.c_str());
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		out->fieldNameNull = true;

		try {
			// check for index existence
			if (!procedure->indexRepository->hasIndex(status, att, tra, sqlDialect, indexName)) {
				const string error_message = string_format(R"(Index "%s" not exists)"s, indexName);
				throwException(status, error_message.c_str());
			}


			const auto& indexDirectoryPath = ftsDirectoryPath / indexName;

			// Check if the index directory exists
			if (!fs::is_directory(indexDirectoryPath)) {
				const string error_message = string_format(R"(Index directory "%s" not exists.)"s, indexDirectoryPath.u8string());
				throwException(status, error_message.c_str());
			}

			const auto& ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
			if (!IndexReader::indexExists(ftsIndexDir)) {
				const string error_message = string_format(R"(Index "%s" not build.)"s, indexName);
				throwException(status, error_message.c_str());
			}
				
			const auto& reader = IndexReader::open(ftsIndexDir, true);

			fieldNames = reader->getFieldNames(IndexReader::FIELD_OPTION_ALL);
			it = fieldNames.begin();

			reader->close();	
			ftsIndexDir->close();

		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	HashSet<String> fieldNames;
	HashSet<String>::const_iterator it;

	FB_UDR_FETCH_PROCEDURE
	{
		if (it == fieldNames.end()) {
			return false;
		}
	    const auto unicodeFieldName = *it;
		const string fieldName = StringUtils::toUTF8(unicodeFieldName);
	    out->fieldNameNull = false;
	    out->fieldName.length = static_cast<ISC_USHORT>(fieldName.length());
		fieldName.copy(out->fieldName.str, out->fieldName.length);

	    ++it;
		return true;
	}
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$INDEX_FILES (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
   FTS$FILE_NAME VARCHAR(127) CHARACTER SET UTF8,
   FTS$FILE_TYPE VARCHAR(63) CHARACTER SET UTF8,
   FTS$FILE_SIZE BIGINT
)
EXTERNAL NAME 'luceneudr!getIndexFiles'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getIndexFiles)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
	);


	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(508, CS_UTF8), fileName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fileType)
		(FB_BIGINT, fileSize)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
		char* name, unsigned nameSize)
	{
		// Forced internal request encoding to UTF8
		memset(name, 0, nameSize);

		const string charset = "UTF8";
		charset.copy(name, charset.length());
	}

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->index_name.str, in->index_name.length);

		const auto& ftsDirectoryPath = getFtsDirectory(context);
		// check if there is a directory for full-text indexes
		if (!fs::is_directory(ftsDirectoryPath)) {
			const string error_message = string_format(R"(Fts directory "%s" not exists)"s, ftsDirectoryPath.u8string());
			throwException(status, error_message.c_str());
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		out->fileNameNull = true;
		out->fileTypeNull = true;
		out->fileSizeNull = true;

		try {
			// check for index existence
			if (!procedure->indexRepository->hasIndex(status, att, tra, sqlDialect, indexName)) {
				const string error_message = string_format(R"(Index "%s" not exists)"s, indexName);
				throwException(status, error_message.c_str());
			}


			const auto& indexDirectoryPath = ftsDirectoryPath / indexName;

			// Check if the index directory exists
			if (!fs::is_directory(indexDirectoryPath)) {
				const string error_message = string_format(R"(Index directory "%s" not exists.)"s, indexDirectoryPath.u8string());
				throwException(status, error_message.c_str());
			}

			const auto& unicodeIndexDir = indexDirectoryPath.wstring();
			const auto& ftsIndexDir = FSDirectory::open(unicodeIndexDir);
			luceneFileHelper.setDirectory(ftsIndexDir);

			auto allFileNames = ftsIndexDir->listAll();
			fileNames.assign(allFileNames.begin(), allFileNames.end());
			fileNames.remove_if([&unicodeIndexDir](const auto &fileName) {
				return !IndexFileNameFilter::getFilter()->accept(unicodeIndexDir, fileName);
			});
			it = fileNames.begin();

		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	LuceneFileHelper luceneFileHelper;
	list<String> fileNames;
	list<String>::const_iterator it;

	FB_UDR_FETCH_PROCEDURE
	{
		if (it == fileNames.end()) {
			return false;
		}
		const auto unicodeFileName = *it;
		const string fileName = StringUtils::toUTF8(unicodeFileName);
		out->fileNameNull = false;
		out->fileName.length = static_cast<ISC_USHORT>(fileName.length());
		fileName.copy(out->fileName.str, out->fileName.length);

		out->fileTypeNull = false;
		const string fileType = luceneFileHelper.getIndexFileType(unicodeFileName);
		out->fileType.length = static_cast<ISC_USHORT>(fileType.length());
		fileType.copy(out->fileType.str, out->fileType.length);


		out->fileSizeNull = false;
		out->fileSize = luceneFileHelper.getFileSize(unicodeFileName);

		++it;
		return true;
	}
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$INDEX_SEGMENT_INFOS (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
   FTS$SEGMENT_NAME VARCHAR(63) CHARACTER SET UTF8,
   FTS$DOC_COUNT INTEGER,
   FTS$SEGMENT_SIZE BIGINT,
   FTS$USE_COMPOUND_FILE BOOLEAN,
   FTS$HAS_DELETIONS BOOLEAN,
   FTS$DEL_COUNT INTEGER,
   FTS$DEL_FILENAME VARCHAR(255) CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!getIndexSegments'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getIndexSegments)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
	);


	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), segmentName)
		(FB_INTEGER, docCount)
		(FB_BIGINT, segmentSize)
		(FB_BOOLEAN, useCompoundFile)
		(FB_BOOLEAN, hasDeletions)
		(FB_INTEGER, delCount)
		(FB_INTL_VARCHAR(1020, CS_UTF8), delFileName)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
		char* name, unsigned nameSize)
	{
		// Forced internal request encoding to UTF8
		memset(name, 0, nameSize);

		const string charset = "UTF8";
		charset.copy(name, charset.length());
	}

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->index_name.str, in->index_name.length);

		const auto& ftsDirectoryPath = getFtsDirectory(context);
		// check if there is a directory for full-text indexes
		if (!fs::is_directory(ftsDirectoryPath)) {
			const string error_message = string_format(R"(Fts directory "%s" not exists)"s, ftsDirectoryPath.u8string());
			throwException(status, error_message.c_str());
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		out->segmentNameNull = true;
		out->docCountNull = true;
		out->segmentSizeNull = true;
		out->useCompoundFileNull = true;
		out->hasDeletionsNull = true;
		out->delCountNull = true;
		out->delFileNameNull = true;

		try {
			// check for index existence
			if (!procedure->indexRepository->hasIndex(status, att, tra, sqlDialect, indexName)) {
				const string error_message = string_format(R"(Index "%s" not exists)"s, indexName);
				throwException(status, error_message.c_str());
			}

			const auto& indexDirectoryPath = ftsDirectoryPath / indexName;

			// Check if the index directory exists
			if (!fs::is_directory(indexDirectoryPath)) {
				const string error_message = string_format(R"(Index directory "%s" not exists.)"s, indexDirectoryPath.u8string());
				throwException(status, error_message.c_str());
			}
			
			const auto& ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
			segmentInfos = newLucene<SegmentInfos>();
			segmentInfos->read(ftsIndexDir);
			

		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	AutoRelease<IAttachment> att{nullptr};
	AutoRelease<ITransaction> tra{nullptr};
	SegmentInfosPtr segmentInfos;
	int32_t segNo = 0;

	FB_UDR_FETCH_PROCEDURE
	{
		if (segNo >= segmentInfos->size()) {
			return false;
		}
	    auto segmentInfo = segmentInfos->info(segNo);

	 	const string segmentName = StringUtils::toUTF8(segmentInfo->name);

	    out->segmentNameNull = false;
		out->segmentName.length = static_cast<ISC_USHORT>(segmentName.length());
		segmentName.copy(out->segmentName.str, out->segmentName.length);

		out->docCountNull = false;
		out->docCount = segmentInfo->docCount;

		out->segmentSizeNull = false;
		out->segmentSize = segmentInfo->sizeInBytes();

		out->useCompoundFileNull = false;
		out->useCompoundFile = segmentInfo->getUseCompoundFile();

		out->hasDeletionsNull = false;
		out->hasDeletions = segmentInfo->hasDeletions();

		out->delCountNull = false;
		out->delCount = segmentInfo->getDelCount();

		out->delFileNameNull = true;
		auto unicodeDelFileName = segmentInfo->getDelFileName();
		if (!unicodeDelFileName.empty()) {	
			string delFileName = StringUtils::toUTF8(unicodeDelFileName);
			out->delFileNameNull = false;
			out->delFileName.length = static_cast<ISC_USHORT>(delFileName.length());
			delFileName.copy(out->delFileName.str, out->delFileName.length);
		}

	    segNo++;
		return true;
	}
FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$INDEX_FIELD_INFOS (
   FTS$INDEX_NAME   VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
   FTS$SEGMENT_NAME VARCHAR(63) CHARACTER SET UTF8
)
RETURNS (
   FTS$FIELD_NAME VARCHAR(127) CHARACTER SET UTF8,
   FTS$FIELD_NUMBER SMALLINT,
   FTS$IS_INDEXED BOOLEAN,
   FTS$STORE_TERM_VECTOR BOOLEAN,
   FTS$STORE_OFFSET_WITH_TERM_VECTOR BOOLEAN,
   FTS$STORE_POSITION_WITH_TERM_VECTOR BOOLEAN,
   FTS$OMIT_NORMS BOOLEAN,
   FTS$OMIT_TERM_FREQ_AND_POSITIONS BOOLEAN,
   FTS$STORE_PAYLOADS BOOLEAN
)
EXTERNAL NAME 'luceneudr!getFieldInfos'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getFieldInfos)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), segmentName)
	);


	FB_UDR_MESSAGE(OutMessage,		
		(FB_INTL_VARCHAR(508, CS_UTF8), fieldName)
		(FB_SMALLINT, fieldNumber)
		(FB_BOOLEAN, isIndexed)
		(FB_BOOLEAN, storeTermVector)
		(FB_BOOLEAN, storeOffsetWithTermVector)
		(FB_BOOLEAN, storePositionWithTermVector)
		(FB_BOOLEAN, omitNorms)
		(FB_BOOLEAN, omitTermFreqAndPositions)
		(FB_BOOLEAN, storePayloads)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	void getCharSet(ThrowStatusWrapper* status, IExternalContext* context,
		char* name, unsigned nameSize)
	{
		// Forced internal request encoding to UTF8
		memset(name, 0, nameSize);

		const string charset = "UTF8";
		charset.copy(name, charset.length());
	}

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->indexNameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->indexName.str, in->indexName.length);

		String unicodeSegmentName;
		string segmentName;
		if (!in->segmentNameNull) {
			segmentName.assign(in->segmentName.str, in->segmentName.length);
			unicodeSegmentName = StringUtils::toUnicode(segmentName);
		}

		const auto& ftsDirectoryPath = getFtsDirectory(context);
		// check if there is a directory for full-text indexes
		if (!fs::is_directory(ftsDirectoryPath)) {
			const string error_message = string_format(R"(Fts directory "%s" not exists)"s, ftsDirectoryPath.u8string());
			throwException(status, error_message.c_str());
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);



		try {
			// check for index existence
			if (!procedure->indexRepository->hasIndex(status, att, tra, sqlDialect, indexName)) {
				const string error_message = string_format(R"(Index "%s" not exists)"s, indexName);
				throwException(status, error_message.c_str());
			}

			const auto& indexDirectoryPath = ftsDirectoryPath / indexName;

			// Check if the index directory exists
			if (!fs::is_directory(indexDirectoryPath)) {
				const string error_message = string_format(R"(Index directory "%s" not exists.)"s, indexDirectoryPath.u8string());
				throwException(status, error_message.c_str());
			}

			const auto& ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
			const auto& segmentInfos = newLucene<SegmentInfos>();
			segmentInfos->read(ftsIndexDir);
		
			SegmentInfoPtr segmentInfo = nullptr;
			if (unicodeSegmentName.empty()) {
				int32_t segNo = segmentInfos->size() - 1;
				segmentInfo = segmentInfos->info(segNo);
			}
			else {
				for (int32_t segNo = 0; segNo < segmentInfos->size(); segNo++) {
					const auto curSegmentInfo = segmentInfos->info(segNo);
					if (curSegmentInfo->name == unicodeSegmentName) {
						segmentInfo = curSegmentInfo;
						break;
					}
				}
			}
			
			if (segmentInfo == nullptr) {
				const string error_message = string_format(R"(Segment "%s" not found)"s, segmentName);
				throwException(status, error_message.c_str());
			}
			
			DirectoryPtr ftsFieldDir(ftsIndexDir);
			if (segmentInfo->getUseCompoundFile()) {
				ftsFieldDir = newLucene<CompoundFileReader>(ftsIndexDir, segmentInfo->name + L"." + IndexFileNames::COMPOUND_FILE_EXTENSION());
			}
			fieldInfos = newLucene<FieldInfos>(ftsFieldDir, segmentInfo->name + L"." + IndexFileNames::FIELD_INFOS_EXTENSION());

		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	AutoRelease<IAttachment> att{nullptr};
	AutoRelease<ITransaction> tra{nullptr};
	FieldInfosPtr fieldInfos;
	int32_t fieldNo = 0;

	FB_UDR_FETCH_PROCEDURE
	{
		if (fieldNo >= fieldInfos->size()) {
			return false;
		}
	    const auto fieldInfo = fieldInfos->fieldInfo(fieldNo);

		const string fieldName = StringUtils::toUTF8(fieldInfo->name);
		out->fieldNameNull = false;
		out->fieldName.length = static_cast<ISC_USHORT>(fieldName.length());
		fieldName.copy(out->fieldName.str, out->fieldName.length);

		out->fieldNumberNull = false;
		out->fieldNumber = static_cast<ISC_SHORT>(fieldInfo->number);

		out->isIndexedNull = false;
		out->isIndexed = fieldInfo->isIndexed;

		out->storeTermVectorNull = false;
		out->storeTermVector = fieldInfo->storeTermVector;

		out->storeOffsetWithTermVectorNull = false;
		out->storeOffsetWithTermVector = fieldInfo->storeOffsetWithTermVector;

		out->storePositionWithTermVectorNull = false;
		out->storePositionWithTermVector = fieldInfo->storePositionWithTermVector;

		out->omitNormsNull = false;
		out->omitNorms = fieldInfo->omitNorms;

		out->omitTermFreqAndPositionsNull = false;
		out->omitTermFreqAndPositions = fieldInfo->omitTermFreqAndPositions;

		out->storePayloadsNull = false;
		out->storePayloads = fieldInfo->storePayloads;

		fieldNo++;
		return true;
	}
FB_UDR_END_PROCEDURE
