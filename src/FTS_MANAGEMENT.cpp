/**
 *  Implementation of procedures and functions of the FTS$MANAGEMENT package.
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
#include "Relations.h"
#include "FBUtils.h"
#include "EncodeUtils.h"
#include "FBFieldInfo.h"
#include "lucene++/LuceneHeaders.h"
#include "lucene++/FileUtils.h"
#include "lucene++/QueryScorer.h"
#include "LuceneAnalyzerFactory.h"
#include <sstream>
#include <vector>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;

/***
FUNCTION FTS$GET_DIRECTORY ()
RETURNS VARCHAR(255) CHARACTER SET UTF8
DETERMINISTIC
EXTERNAL NAME 'luceneudr!getFTSDirectory'
ENGINE UDR;
***/
FB_UDR_BEGIN_FUNCTION(getFTSDirectory)
    FB_UDR_MESSAGE(OutMessage,
	   (FB_INTL_VARCHAR(1020, CS_UTF8), directory)
    );


    FB_UDR_EXECUTE_FUNCTION
    {
	    const string ftsDirectory = getFtsDirectory(context);

	    out->directoryNull = false;
	    out->directory.length = static_cast<ISC_USHORT>(ftsDirectory.length());
	    ftsDirectory.copy(out->directory.str, out->directory.length);
    }
FB_UDR_END_FUNCTION


/***
PROCEDURE FTS$ANALYZERS
EXTERNAL NAME 'luceneudr!getAnalyzers'
RETURNS (
  FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8
)
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(getAnalyzers)

    FB_UDR_MESSAGE(OutMessage,
	    (FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
    );

    FB_UDR_CONSTRUCTOR
       , analyzerFactory()
    {
    }

    LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_PROCEDURE
	{
		analyzerNames = procedure->analyzerFactory.getAnalyzerNames();
		it = analyzerNames.begin();;
	}

	list<string> analyzerNames;
	list<string>::const_iterator it;

	FB_UDR_FETCH_PROCEDURE
	{
		if (it == analyzerNames.end()) {
			return false;
		}
		const string analyzerName = *it;

		out->analyzerNull = false;
		out->analyzer.length = static_cast<ISC_USHORT>(analyzerName.length());
		analyzerName.copy(out->analyzer.str, out->analyzer.length);

		++it;
		return true;
	}
FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$CREATE_INDEX (
	 FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
	 FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!createIndex'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(createIndex)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
		(FB_BLOB, description)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
		, analyzerFactory()
	{
	}

	FTSIndexRepository indexRepository;
	LuceneAnalyzerFactory analyzerFactory;

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

		string analyzerName;
		if (!in->analyzerNull) {
			analyzerName.assign(in->analyzer.str, in->analyzer.length);
		}
		if (analyzerName.empty()) {
			analyzerName = DEFAULT_ANALYZER_NAME;
		}
		else {
			std::transform(analyzerName.begin(), analyzerName.end(), analyzerName.begin(), ::toupper);
		}

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);


		string description;
		if (!in->descriptionNull) {
			AutoRelease<IBlob> blob(att->openBlob(status, tra, &in->description, 0, nullptr));
			description = blob_get_string(status, blob);
			blob->close(status);
		}


		procedure->indexRepository.createIndex(status, att, tra, sqlDialect, indexName, analyzerName, description);

		// Check if the index directory exists, and if it doesn't exist, create it. 
		const string ftsDirectory = getFtsDirectory(context);
		const auto unicodeIndexDir = FileUtils::joinPath(StringUtils::toUnicode(ftsDirectory), StringUtils::toUnicode(indexName));
		if (!createIndexDirectory(unicodeIndexDir)) {
			    const string indexDir = StringUtils::toUTF8(unicodeIndexDir);
				const string error_message = string_format("Cannot create index directory \"%s\".", indexDir);
				ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS)error_message.c_str(),
					isc_arg_end
				};
				throw FbException(status, statusVector);
		}
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$DROP_INDEX (
	 FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!dropIndex'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(dropIndex)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
	{
	}

	FTSIndexRepository indexRepository;

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

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		procedure->indexRepository.dropIndex(status, att, tra, sqlDialect, indexName);

		const string ftsDirectory = getFtsDirectory(context);
		const auto unicodeIndexDir = FileUtils::joinPath(StringUtils::toUnicode(ftsDirectory), StringUtils::toUnicode(indexName));
		// If the directory exists, then delete it.
		if (!removeIndexDirectory(unicodeIndexDir)) {
			const string indexDir = StringUtils::toUTF8(unicodeIndexDir);
			const string error_message = string_format("Cannot delete index directory \"%s\".", indexDir);
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$SET_INDEX_ACTIVE (
	 FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$INDEX_ACTIVE BOOLEAN NOT NULL
)
EXTERNAL NAME 'luceneudr!setIndexActive'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(setIndexActive)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
		(FB_BOOLEAN, index_active)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
	{
	}

	FTSIndexRepository indexRepository;

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
		const bool indexActive = in->index_active;

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		auto ftsIndex = procedure->indexRepository.getIndex(status, att, tra, sqlDialect, indexName);
		if (indexActive) {
			// index is inactive
			if (ftsIndex.status == "I") {
				// index is active but needs to be rebuilt
				procedure->indexRepository.setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
			}
		}
		else {
			// index is active
			if (ftsIndex.isActive()) {
				// make inactive
				procedure->indexRepository.setIndexStatus(status, att, tra, sqlDialect, indexName, "I");
			}
		}
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$ADD_INDEX_FIELD (
	 FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$BOOST DOUBLE PRECISION DEFAULT NULL
)
EXTERNAL NAME 'luceneudr!addIndexField'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(addIndexField)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), field_name)
		(FB_DOUBLE, boost)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
	{
	}

	FTSIndexRepository indexRepository;

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

		if (in->relation_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Relation name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string relationName(in->relation_name.str, in->relation_name.length);

		if (in->field_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Field name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string fieldName(in->field_name.str, in->field_name.length);

		double boost = 1.0;
		if (!in->boostNull) {
			boost = in->boost;
		}

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		// adding a segment
		procedure->indexRepository.addIndexField(status, att, tra, sqlDialect, indexName, relationName, fieldName, boost);
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$DROP_INDEX_FIELD (
	 FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!dropIndexField'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(dropIndexField)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), field_name)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
	{
	}

	FTSIndexRepository indexRepository;

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

		if (in->relation_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Relation name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string relationName(in->relation_name.str, in->relation_name.length);

		if (in->field_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Field name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string fieldName(in->field_name.str, in->field_name.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		// deleting a segment 
		procedure->indexRepository.dropIndexField(status, att, tra, sqlDialect, indexName, relationName, fieldName);
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$REBUILD_INDEX (
	FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!rebuildIndex'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(rebuildIndex)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
		, relationHelper(context->getMaster())
		, analyzerFactory()
	{
	}

	FTSIndexRepository indexRepository;
	RelationHelper relationHelper;
	LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_PROCEDURE
	{
		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		if (in->index_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Index name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string indexName(in->index_name.str, in->index_name.length);

		const string ftsDirectory = getFtsDirectory(context);
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

		const unsigned int sqlDialect = getSqlDialect(status, att);

		try {
			// check for index existence
			auto ftsIndex = procedure->indexRepository.getIndex(status, att, tra, sqlDialect, indexName);
			// Check if the index directory exists, and if it doesn't exist, create it. 
			const auto unicodeIndexDir = FileUtils::joinPath(StringUtils::toUnicode(ftsDirectory), StringUtils::toUnicode(indexName));
			if (!createIndexDirectory(unicodeIndexDir)) {
				const string indexDir = StringUtils::toUTF8(unicodeIndexDir);
				const string error_message = string_format("Cannot create index directory \"%s\".", indexDir);
				ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS)error_message.c_str(),
					isc_arg_end
				};
				throw FbException(status, statusVector);
			}

			// get index segments and group them by table names
			auto segments = procedure->indexRepository.getIndexSegments(status, att, tra, sqlDialect, indexName);
			if (segments.size() == 0) {
				const string error_message = string_format("Cannot rebuild index \"%s\". The index does not contain segments.", indexName);
				ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS)error_message.c_str(),
					isc_arg_end
				};
				throw FbException(status, statusVector);
			}

			auto segmentsByRelation = FTSIndexRepository::groupIndexSegmentsByRelation(segments);

			auto fsIndexDir = FSDirectory::open(unicodeIndexDir);
			auto analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex.analyzer);
			IndexWriterPtr writer = newLucene<IndexWriter>(fsIndexDir, analyzer, true, IndexWriter::MaxFieldLengthLIMITED);

			// clean up index directory
			writer->deleteAll();
			writer->commit();

			const char* fbCharset = context->getClientCharSet();
			const string icuCharset = getICICharset(fbCharset);

			for (const auto& p : segmentsByRelation) {
				const string relationName = p.first;
				if (!procedure->relationHelper.relationExists(status, att, tra, sqlDialect, relationName)) {
					const string error_message = string_format("Cannot rebuild index \"%s\". Table \"%s\" not exists. Please delete the index segments containing it.", indexName, relationName);
					ISC_STATUS statusVector[] = {
						isc_arg_gds, isc_random,
						isc_arg_string, (ISC_STATUS)error_message.c_str(),
						isc_arg_end
					};
					throw FbException(status, statusVector);
				}
				const auto segments = p.second;
				list<string> fieldNames;
				for (const auto& segment : segments) {
					if (!procedure->relationHelper.fieldExists(status, att, tra, sqlDialect, segment.relationName, segment.fieldName)) {
						const string error_message = string_format("Cannot rebuild index \"%s\". Field \"%s\" not exists in table \"%s\". Please delete the index segments containing it.", indexName, segment.fieldName, segment.relationName);
						ISC_STATUS statusVector[] = {
							isc_arg_gds, isc_random,
							isc_arg_string, (ISC_STATUS)error_message.c_str(),
							isc_arg_end
						};
						throw FbException(status, statusVector);
					}
					fieldNames.push_back(segment.fieldName);
				}
				const string sql = RelationHelper::buildSqlSelectFieldValues(sqlDialect, relationName, fieldNames);
				const auto unicodeRelationName = StringUtils::toUnicode(relationName);

				AutoRelease<IStatement> stmt(att->prepare(
					status,
					tra,
					0,
					sql.c_str(),
					sqlDialect,
					IStatement::PREPARE_PREFETCH_METADATA
				));
				AutoRelease<IMessageMetadata> outputMetadata(stmt->getOutputMetadata(status));
				const unsigned colCount = outputMetadata->getCount(status);
				// make all fields of string type except BLOB
				AutoRelease<IMessageMetadata> newMeta(prepareTextMetaData(status, outputMetadata));
				auto fields = FbFieldsInfo(status, newMeta);

				AutoRelease<IResultSet> rs(stmt->openCursor(
					status,
					tra,
					nullptr,
					nullptr,
					newMeta,
					0
				));

				const unsigned msgLength = newMeta->getMessageLength(status);
				{
					// allocate output buffer
					auto b = make_unique<unsigned char[]>(msgLength);
					unsigned char* buffer = b.get();
					while (rs->fetchNext(status, buffer) == IStatus::RESULT_OK) {
						bool emptyFlag = true;
						DocumentPtr doc = newLucene<Document>();
						const string dbKey = fields[0].getStringValue(status, att, tra, buffer);
						// RDB$DB_KEY is in a binary format that cannot be converted to Unicode, 
						// so we will convert the string to a hexadecimal representation.
						const string hexDbKey = string_to_hex(dbKey);
						doc->add(newLucene<Field>(L"RDB$DB_KEY", StringUtils::toUnicode(hexDbKey), Field::STORE_YES, Field::INDEX_NOT_ANALYZED));
						doc->add(newLucene<Field>(L"RDB$RELATION_NAME", unicodeRelationName, Field::STORE_YES, Field::INDEX_NOT_ANALYZED));
						for (unsigned int i = 1; i < colCount; i++) {
							auto field = fields[i];
							bool nullFlag = field.isNull(buffer);
							string value;
							if (!nullFlag) {
								value = field.getStringValue(status, att, tra, buffer);
							}
							auto fieldName = StringUtils::toUnicode(relationName + "." + field.fieldName);
							Lucene::String unicodeValue;
							if (!value.empty()) {
								// re-encode content to Unicode only if the string is non-empty
								unicodeValue = StringUtils::toUnicode(to_utf8(value, icuCharset));
							}

							auto luceneField = newLucene<Field>(fieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED);

							auto iSegment = std::find_if(
								segments.begin(),
								segments.end(),
								[&field](FTSIndexSegment segment) { return segment.fieldName == field.fieldName; }
							);
							if (iSegment != segments.end()) {
								auto segment = *iSegment;
								luceneField->setBoost(segment.boost);
							}
							doc->add(luceneField);
							emptyFlag = emptyFlag && value.empty();
						}
						// if all indexed fields are empty, then it makes no sense to add the document to the index
						if (!emptyFlag) {
							writer->addDocument(doc);
						}
					}
					rs->close(status);
				}
				writer->commit();
			}
			writer->optimize();
			writer->close();

			// if the index building was successful, then set the indexing completion status
			procedure->indexRepository.setIndexStatus(status, att, tra, sqlDialect, indexName, "C");
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

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$OPTIMIZE_INDEX (
	FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!optimizeIndex'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(optimizeIndex)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
		, analyzerFactory()
	{
	}

	FTSIndexRepository indexRepository;
	LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_PROCEDURE
	{
		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		if (in->index_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Index name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		const string indexName(in->index_name.str, in->index_name.length);

		const string ftsDirectory = getFtsDirectory(context);
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

		const unsigned int sqlDialect = getSqlDialect(status, att);

		try {
			// check for index existence
			auto ftsIndex = procedure->indexRepository.getIndex(status, att, tra, sqlDialect, indexName);
			// Check if the index directory exists. 
			const auto unicodeIndexDir = FileUtils::joinPath(StringUtils::toUnicode(ftsDirectory), StringUtils::toUnicode(indexName));
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

			auto fsIndexDir = FSDirectory::open(unicodeIndexDir);
			auto analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex.analyzer);
			IndexWriterPtr writer = newLucene<IndexWriter>(fsIndexDir, analyzer, false, IndexWriter::MaxFieldLengthLIMITED);

			// clean up index directory
			writer->optimize();
			writer->close();
			fsIndexDir->close();
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

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE