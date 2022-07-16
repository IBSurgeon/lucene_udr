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
#include "FTSUtils.h"
#include "Relations.h"
#include "FBUtils.h"
#include "EncodeUtils.h"
#include "FBFieldInfo.h"
#include "LuceneHeaders.h"
#include "FileUtils.h"
#include "QueryScorer.h"
#include "LuceneAnalyzerFactory.h"
#include <sstream>
#include <vector>
#include <memory>
#include <algorithm>
#include <filesystem> 

namespace fs = std::filesystem;

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
	    const auto& ftsDirectoryPath = getFtsDirectory(context);
	    const string ftsDirectory = ftsDirectoryPath.u8string();

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
		it = analyzerNames.begin();
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
	 FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8,
	 FTS$KEY_FIELD_NAME VARCHAR(63) CHARACTER SET UTF8,
	 FTS$DESCRIPTION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!createIndex'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(createIndex)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
		(FB_INTL_VARCHAR(252, CS_UTF8), keyFieldName)
		(FB_BLOB, description)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
		, analyzerFactory()
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};
	LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->indexNameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->indexName.str, in->indexName.length);

		if (in->relationNameNull) {
			throwException(status, "Relation name can not be NULL");
		}
		const string relationName(in->relationName.str, in->relationName.length);

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
			description = BlobUtils::getString(status, blob);
			blob->close(status);
		}

		const auto& relationHelper = procedure->indexRepository->getRelationHelper();
		const auto relationInfo = relationHelper->getRelationInfo(status, att, tra, sqlDialect, relationName);


		procedure->indexRepository->createIndex(status, att, tra, sqlDialect, indexName, relationName, analyzerName, description);

		string keyFieldName;
		if (in->keyFieldNameNull) {
			if (relationInfo->findKeyFieldSupported()) {
				RelationFieldList keyFields;
				relationHelper->fillPrimaryKeyFields(status, att, tra, sqlDialect, relationName, keyFields);
				if (keyFields.size() == 0) {
				   // There is no primary key constraint.
					if (relationInfo->relationType == RelationType::RT_REGULAR) {
						keyFieldName = "RDB$DB_KEY";
					}
					else {
						throwException(status, "The key field is not specified.");
					}
				}
				else if (keyFields.size() == 1) {
				    // OK
					const auto& keyFieldInfo = *keyFields.cbegin();
					keyFieldName = keyFieldInfo->fieldName;
				}
				else {
					throwException(status, 
						"The primary key of the relation is composite. The FTS index does not support composite keys. " 
						"Please specify the key field explicitly.");				
				}
			}
			else {
				throwException(status, "It is not possible to automatically determine the key field for this type of relation. Please specify this explicitly.");
			}
		}
		else {
			keyFieldName.assign(in->keyFieldName.str, in->keyFieldName.length);
		}

		if (keyFieldName == "RDB$DB_KEY") {
			if (relationInfo->relationType != RelationType::RT_REGULAR) {
				throwException(status, R"(Using "RDB$DB_KEY" as a key is supported only for regular tables.)");
			}
		}
		else {
			const auto keyFieldInfo = relationHelper->getField(status, att, tra, sqlDialect, relationName, keyFieldName);
		    // check field type
			// Supported types SMALLINT, INTEGER, BIGINT, CHAR(16) CHARACTER SET OCTETS, BINARY(16) 
			if (!(keyFieldInfo->isInt() || (keyFieldInfo->isFixedChar() && keyFieldInfo->isBinary() && keyFieldInfo->fieldLength == 16))) {
				throwException(status, "Unsupported data type for the key field. Supported data types: SMALLINT, INTEGER, BIGINT, CHAR(16) CHARACTER SET OCTETS, BINARY(16).");
			}
		}

		// Add index key field
		procedure->indexRepository->addIndexField(status, att, tra, sqlDialect, indexName, keyFieldName, true);

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
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->index_name.str, in->index_name.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		procedure->indexRepository->dropIndex(status, att, tra, sqlDialect, indexName);

		const auto& ftsDirectoryPath = getFtsDirectory(context);
		const auto& indexDirectoryPath = ftsDirectoryPath / indexName;
		// If the directory exists, then delete it.
		if (!removeIndexDirectory(indexDirectoryPath)) {
			const string error_message = string_format(R"(Cannot delete index directory "%s".)"s, indexDirectoryPath.u8string());
			throwException(status, error_message.c_str());
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
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->index_name.str, in->index_name.length);
		const bool indexActive = in->index_active;

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		auto ftsIndex = make_unique<FTSIndex>();
		procedure->indexRepository->getIndex(status, att, tra, sqlDialect, ftsIndex, indexName);
		if (indexActive) {
			// index is inactive
			if (ftsIndex->status == "I") {
				// index is active but needs to be rebuilt
				procedure->indexRepository->setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
			}
		}
		else {
			// index is active
			if (ftsIndex->isActive()) {
				// make inactive
				procedure->indexRepository->setIndexStatus(status, att, tra, sqlDialect, indexName, "I");
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
	 FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$BOOST DOUBLE PRECISION DEFAULT NULL
)
EXTERNAL NAME 'luceneudr!addIndexField'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(addIndexField)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), field_name)
		(FB_DOUBLE, boost)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->index_name.str, in->index_name.length);


		if (in->field_nameNull) {
			throwException(status, "Field name can not be NULL");
		}
		const string fieldName(in->field_name.str, in->field_name.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		// Adding a field to the index.
		procedure->indexRepository->addIndexField(status, att, tra, sqlDialect, indexName, fieldName, false, in->boost, in->boostNull);
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$DROP_INDEX_FIELD (
	 FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!dropIndexField'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(dropIndexField)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), index_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), field_name)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->index_name.str, in->index_name.length);

		if (in->field_nameNull) {
			throwException(status, "Field name can not be NULL");
		}
		const string fieldName(in->field_name.str, in->field_name.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		// Deleting a field from the index.
		procedure->indexRepository->dropIndexField(status, att, tra, sqlDialect, indexName, fieldName);
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$SET_INDEX_FIELD_BOOST (
	 FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$BOOST DOUBLE PRECISION
)
EXTERNAL NAME 'luceneudr!setIndexFieldBoost'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(setIndexFieldBoost)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		(FB_DOUBLE, boost)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->indexNameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->indexName.str, in->indexName.length);


		if (in->fieldNameNull) {
			throwException(status, "Field name can not be NULL");
		}
		const string fieldName(in->fieldName.str, in->fieldName.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		procedure->indexRepository->setIndexFieldBoost(status, att, tra, sqlDialect, indexName, fieldName, in->boost, in->boostNull);
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
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
		, analyzerFactory()
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};
	LuceneAnalyzerFactory analyzerFactory;

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
		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

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

		const unsigned int sqlDialect = getSqlDialect(status, att);

		try {
			// check for index existence
			auto ftsIndex = make_unique<FTSIndex>();
			procedure->indexRepository->getIndex(status, att, tra, sqlDialect, ftsIndex, indexName, true);
			// Check if the index directory exists, and if it doesn't exist, create it. 
			const auto& indexDirectoryPath = ftsDirectoryPath / indexName;
			if (!createIndexDirectory(indexDirectoryPath)) {
				const string error_message = string_format(R"(Cannot create index directory "%s".)"s, indexDirectoryPath.u8string());
				throwException(status, error_message.c_str());
			}

			// check relation exists
			const auto& relationHelper = procedure->indexRepository->getRelationHelper();
			if (!relationHelper->relationExists(status, att, tra, sqlDialect, ftsIndex->relationName)) {
				const string error_message = string_format(R"(Cannot rebuild index "%s". Table "%s" not exists.)"s, indexName, ftsIndex->relationName);
				throwException(status, error_message.c_str());
			}

			// check segments exists
			if (ftsIndex->segments.size() == 0) {
				const string error_message = string_format(R"(Cannot rebuild index "%s". The index does not contain fields.)"s, indexName);
				throwException(status, error_message.c_str());
			}

			const auto& fsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
			const auto& analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex->analyzer);
			const auto& writer = newLucene<IndexWriter>(fsIndexDir, analyzer, true, IndexWriter::MaxFieldLengthLIMITED);

			// clean up index directory
			writer->deleteAll();
			writer->commit();

			//const char* fbCharset = context->getClientCharSet();
			//FBStringEncoder fbStringEncoder(fbCharset);

			for (const auto& segment : ftsIndex->segments) {
				if (segment->fieldName != "RDB$DB_KEY") {
					if (!segment->fieldExists) {
						const string error_message = string_format(R"(Cannot rebuild index "%s". Field "%s" not exists in relation "%s".)"s, indexName, segment->fieldName, ftsIndex->relationName);
						throwException(status, error_message.c_str());
					}
				}
			}
				
			const string sql = ftsIndex->buildSqlSelectFieldValues(status, sqlDialect); 

			AutoRelease<IStatement> stmt(att->prepare(
				status,
				tra,
				0,
				sql.c_str(),
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
			AutoRelease<IMessageMetadata> outputMetadata(stmt->getOutputMetadata(status));
			// make all fields of string type except BLOB
			AutoRelease<IMessageMetadata> newMeta(prepareTextMetaData(status, outputMetadata));
			const auto fields = FbFieldsInfo(status, newMeta);

			// initial specific FTS property for fields
			for (unsigned int i = 0; i < fields.size(); i++) {
				const auto& field = fields[i];
				auto iSegment = ftsIndex->findSegment(field->fieldName);
				if (iSegment == ftsIndex->segments.end()) {
					const string error_message = string_format(R"(Cannot rebuild index "%s". Field "%s" not found.)"s, indexName, field->fieldName);
					throwException(status, error_message.c_str());
				}
				auto const& segment = *iSegment;
				field->ftsFieldName = StringUtils::toUnicode(segment->fieldName);
				field->ftsKey = segment->key;
				field->ftsBoost = segment->boost;
				field->ftsBoostNull = segment->boostNull;
			}

			AutoRelease<IResultSet> rs(stmt->openCursor(
				status,
				tra,
				nullptr,
				nullptr,
				newMeta,
				0
			));
				
			const unsigned colCount = newMeta->getCount(status);
			const unsigned msgLength = newMeta->getMessageLength(status);
			{
				// allocate output buffer
				auto b = make_unique<unsigned char[]>(msgLength);
				unsigned char* buffer = b.get();
				memset(buffer, 0, msgLength);
				while (rs->fetchNext(status, buffer) == IStatus::RESULT_OK) {						
					bool emptyFlag = true;
					const auto& doc = newLucene<Document>();
						
					for (unsigned int i = 0; i < colCount; i++) {
						const auto& field = fields[i];

						Lucene::String unicodeValue;	
						if (!field->isNull(buffer)) {
							const string value = field->getStringValue(status, att, tra, buffer);
							if (!value.empty()) {
								// re-encode content to Unicode only if the string is non-binary
								if (!field->isBinary()) {
									unicodeValue = StringUtils::toUnicode(value);
								}
								else {
									// convert the binary string to a hexadecimal representation
									unicodeValue = StringUtils::toUnicode(string_to_hex(value));
								}
							}
						}
                        // add field to document
						if (field->ftsKey) {
							const auto& luceneField = newLucene<Field>(field->ftsFieldName, unicodeValue, Field::STORE_YES, Field::INDEX_NOT_ANALYZED);
							doc->add(luceneField);
						}
						else {
							const auto& luceneField = newLucene<Field>(field->ftsFieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED);
							if (!field->ftsBoostNull) {
								luceneField->setBoost(field->ftsBoost);
							}
							doc->add(luceneField);
							emptyFlag = emptyFlag && unicodeValue.empty();
						}						
						
					}
					// if all indexed fields are empty, then it makes no sense to add the document to the index
					if (!emptyFlag) {
						writer->addDocument(doc);
					}
					memset(buffer, 0, msgLength);
				}
				rs->close(status);
			}
			writer->commit();

			writer->optimize();
			writer->commit();
			writer->close();

			// if the index building was successful, then set the indexing completion status
			procedure->indexRepository->setIndexStatus(status, att, tra, sqlDialect, indexName, "C");
		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
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
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
		, analyzerFactory()
	{
	}

	FTSIndexRepositoryPtr indexRepository{nullptr};
	LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_PROCEDURE
	{
		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

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

		const unsigned int sqlDialect = getSqlDialect(status, att);

		try {
			// check for index existence
			auto ftsIndex = make_unique<FTSIndex>();
			procedure->indexRepository->getIndex(status, att, tra, sqlDialect, ftsIndex, indexName);
			// Check if the index directory exists. 
			const auto& indexDirectoryPath = ftsDirectoryPath / indexName;
			if (!fs::is_directory(indexDirectoryPath)) {
				const string error_message = string_format(R"(Index directory "%s" not exists.)"s, indexDirectoryPath.u8string());
				throwException(status, error_message.c_str());
			}

			const auto& fsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
			const auto& analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex->analyzer);
			const auto& writer = newLucene<IndexWriter>(fsIndexDir, analyzer, false, IndexWriter::MaxFieldLengthLIMITED);

			// clean up index directory
			writer->optimize();
			writer->close();
			fsIndexDir->close();
		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE
