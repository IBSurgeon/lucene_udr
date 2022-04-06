#include "LuceneUdr.h"
#include "FTSIndex.h"
#include "FBUtils.h"
#include "lucene++/LuceneHeaders.h"
#include "lucene++/FileUtils.h"
#include "lucene++/SegmentInfos.h"
#include "lucene++/SegmentInfo.h"
#include <sstream>
#include <vector>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;


/***
PROCEDURE FTS$INDEX_STATISITCS (
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
	, indexRepository(context->getMaster())
	{
	}

	LuceneFTS::FTSIndexRepository indexRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Index name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string indexName(in->index_name.str, in->index_name.length);

		const string ftsDirectory = LuceneFTS::getFtsDirectory(context);
		// check if there is a directory for full-text indexes
		if (!FileUtils::isDirectory(StringUtils::toUnicode(ftsDirectory))) {
			const string error_message = string_format("Fts directory \"%s\" not exists", ftsDirectory);
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
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
			auto ftsIndex = procedure->indexRepository.getIndex(status, att, tra, sqlDialect, indexName);

			out->analyzerNameNull = false;
			out->analyzerName.length = static_cast<ISC_USHORT>(ftsIndex.analyzer.length());
			ftsIndex.analyzer.copy(out->analyzerName.str, out->analyzerName.length);

			const auto unicodeIndexDir = FileUtils::joinPath(StringUtils::toUnicode(ftsDirectory), StringUtils::toUnicode(indexName));
			const string indexDir = StringUtils::toUTF8(unicodeIndexDir);
			out->indexDirNull = false;
			out->indexDir.length = static_cast<ISC_USHORT>(indexDir.length());
			indexDir.copy(out->indexDir.str, out->indexDir.length);

			out->indexExistsNull = false;
			out->indexExists = true;
			// Check if the index directory exists
			if (!FileUtils::isDirectory(unicodeIndexDir)) {
				// index created, but not build
				ftsIndex.status = "N";
				out->indexExists = false;
			}
			else {
				auto ftsIndexDir = FSDirectory::open(unicodeIndexDir);
				if (!IndexReader::indexExists(ftsIndexDir)) {
					// index created, but not build
					ftsIndex.status = "N";
					out->indexExists = false;
				}
				else {
					IndexReaderPtr reader = IndexReader::open(ftsIndexDir, true);

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
					out->indexSize = 0;
					auto indexFileNames = ftsIndexDir->listAll();
					for (const auto indexFileName : indexFileNames) {
						out->indexSize += ftsIndexDir->fileLength(indexFileName);
					}

					reader->close();
				}
				ftsIndexDir->close();
			}
			out->indexStatusNull = false;
			out->indexStatus.length = static_cast<ISC_USHORT>(ftsIndex.status.length());
			ftsIndex.status.copy(out->indexStatus.str, out->indexStatus.length);
		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
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
		, indexRepository(context->getMaster())
	{
	}

	LuceneFTS::FTSIndexRepository indexRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Index name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string indexName(in->index_name.str, in->index_name.length);

		const string ftsDirectory = LuceneFTS::getFtsDirectory(context);
		// check if there is a directory for full-text indexes
		if (!FileUtils::isDirectory(StringUtils::toUnicode(ftsDirectory))) {
			const string error_message = string_format("Fts directory \"%s\" not exists", ftsDirectory);
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		out->fieldNameNull = true;

		try {
			// check for index existence
			auto ftsIndex = procedure->indexRepository.getIndex(status, att, tra, sqlDialect, indexName);


			const auto unicodeIndexDir = FileUtils::joinPath(StringUtils::toUnicode(ftsDirectory), StringUtils::toUnicode(indexName));
			const string indexDir = StringUtils::toUTF8(unicodeIndexDir);

			// Check if the index directory exists
			if (!FileUtils::isDirectory(unicodeIndexDir)) {
				const string indexDir = StringUtils::toUTF8(unicodeIndexDir);
				const string error_message = string_format("Index directory \"%s\" not exists.", indexDir);
				ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS)error_message.c_str(),
					isc_arg_end
				};
				throw FbException(status, statusVector);
			}

			auto ftsIndexDir = FSDirectory::open(unicodeIndexDir);
			if (!IndexReader::indexExists(ftsIndexDir)) {
				const string error_message = string_format("Index \"%s\" not build.", indexName);
				ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS)error_message.c_str(),
					isc_arg_end
				};
				throw FbException(status, statusVector);
			}
				
			IndexReaderPtr reader = IndexReader::open(ftsIndexDir, true);

			fieldNames = reader->getFieldNames(IndexReader::FIELD_OPTION_ALL);
			it = fieldNames.begin();

			reader->close();	
			ftsIndexDir->close();

		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
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
PROCEDURE FTS$INDEX_SEGMENTS_INFO (
   FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
RETURNS (
   FTS$SEGMENT_NAME VARCHAR(63) CHARACTER SET UTF8,
   FTS$DOC_COUNT INTEGER,
   FTS$SEGMENT_SIZE BIGINT,
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
		(FB_BOOLEAN, hasDeletions)
		(FB_INTEGER, delCount)
		(FB_INTL_VARCHAR(1020, CS_UTF8), delFileName)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
	{
	}

	LuceneFTS::FTSIndexRepository indexRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Index name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string indexName(in->index_name.str, in->index_name.length);

		const string ftsDirectory = LuceneFTS::getFtsDirectory(context);
		// check if there is a directory for full-text indexes
		if (!FileUtils::isDirectory(StringUtils::toUnicode(ftsDirectory))) {
			const string error_message = string_format("Fts directory \"%s\" not exists", ftsDirectory);
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		out->segmentNameNull = true;
		out->docCountNull = true;
		out->segmentSizeNull = true;
		out->hasDeletionsNull = true;
		out->delCountNull = true;
		out->delFileNameNull = true;

		try {
			// check for index existence
			auto ftsIndex = procedure->indexRepository.getIndex(status, att, tra, sqlDialect, indexName);

			const auto unicodeIndexDir = FileUtils::joinPath(StringUtils::toUnicode(ftsDirectory), StringUtils::toUnicode(indexName));
			const string indexDir = StringUtils::toUTF8(unicodeIndexDir);

			// Check if the index directory exists
			if (!FileUtils::isDirectory(unicodeIndexDir)) {
				const string indexDir = StringUtils::toUTF8(unicodeIndexDir);
				const string error_message = string_format("Index directory \"%s\" not exists.", indexDir);
				ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS)error_message.c_str(),
					isc_arg_end
				};
				throw FbException(status, statusVector);
			}
			
			auto ftsIndexDir = FSDirectory::open(unicodeIndexDir);
			segmentInfos = newLucene<SegmentInfos>();
			segmentInfos->read(ftsIndexDir);
			

		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
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