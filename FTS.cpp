
#include "LuceneUdr.h"
#include "FTSIndex.h"
#include "FTSLog.h"
#include "Relations.h"
#include "FBBlobUtils.h"
#include "FbFieldInfo.h"
#include "EncodeUtils.h"
#include "lucene++/LuceneHeaders.h"
#include "lucene++/FileUtils.h"
#include "lucene++/MiscUtils.h"
#include "lucene++/SimpleHTMLFormatter.h"
#include "lucene++/QueryScorer.h"
#include "lucene++/Highlighter.h"
#include "lucene++/SimpleSpanFragmenter.h"
#include "LuceneAnalyzerFactory.h"
#include <sstream>
#include <vector>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;

IMessageMetadata* prepareTextMetaData(ThrowStatusWrapper* status, IMessageMetadata* meta)
{
	unsigned colCount = meta->getCount(status);
	// make all fields of string type except BLOB
	AutoRelease<IMetadataBuilder> builder(meta->getBuilder(status));
	for (unsigned i = 0; i < colCount; i++) {
		unsigned dataType = meta->getType(status, i);
		switch (dataType) {
		case SQL_VARYING:
			break;
		case SQL_TEXT:
			builder->setType(status, i, SQL_VARYING);
			break;
		case SQL_SHORT:
		case SQL_LONG:
		case SQL_INT64:
		case SQL_INT128:
			builder->setType(status, i, SQL_VARYING);
			builder->setLength(status, i, 40 * 4);
			break;
		case SQL_FLOAT:
		case SQL_D_FLOAT:
		case SQL_DOUBLE:
			builder->setType(status, i, SQL_VARYING);
			builder->setLength(status, i, 50 * 4);
			break;
		case SQL_BOOLEAN:
			builder->setType(status, i, SQL_VARYING);
			builder->setLength(status, i, 5 * 4);
			break;
		case SQL_TYPE_DATE:
		case SQL_TYPE_TIME:
		case SQL_TIMESTAMP:
			builder->setType(status, i, SQL_VARYING);
			builder->setLength(status, i, 35 * 4);
			break;
		case SQL_TIME_TZ:
		case SQL_TIMESTAMP_TZ:
			builder->setType(status, i, SQL_VARYING);
			builder->setLength(status, i, 42 * 4);
			break;
		case SQL_DEC16:
		case SQL_DEC34:
			builder->setType(status, i, SQL_VARYING);
			builder->setLength(status, i, 60 * 4);
			break;
		}
	}
	return builder->getMetadata(status);
}

unsigned int getSqlDialect(ThrowStatusWrapper* status, IAttachment* att) {
	unsigned int sql_dialect = 1;
	const unsigned char info_options[] = { isc_info_db_sql_dialect, isc_info_end };
	unsigned char buffer[256];
	int length;
	unsigned char item;
	att->getInfo(status, sizeof(info_options), info_options, sizeof(buffer), buffer);
	/* Extract the values returned in the result buffer. */
	for (unsigned char* p = buffer; *p != isc_info_end; ) {
		item = *p++;
		length = isc_vax_integer((ISC_SCHAR*)p, 2);
		p += 2;
		switch (item) {
		case isc_info_db_sql_dialect:
			sql_dialect = isc_vax_integer((ISC_SCHAR*)p, length);
			break;
		default:
			break;
		}
		p += length;
	};
	return sql_dialect;
}

inline bool createIndexDirectory(string indexDir)
{
	auto indexDirUnicode = StringUtils::toUnicode(indexDir);
	if (!FileUtils::isDirectory(indexDirUnicode)) {
		return FileUtils::createDirectory(indexDirUnicode);
	}
	return true;
}

inline bool removeIndexDirectory(string indexDir)
{
	auto indexDirUnicode = StringUtils::toUnicode(indexDir);
	if (FileUtils::isDirectory(indexDirUnicode)) {
		return FileUtils::removeDirectory(indexDirUnicode);
	}
	return true;
}

/***
FUNCTION FTS$GET_DIRECTORY () 
RETURNS VARCHAR(255) CHARACTER SET UTF8
EXTERNAL NAME 'luceneudr!getFTSDirectory'
ENGINE UDR;
***/
FB_UDR_BEGIN_FUNCTION(getFTSDirectory)
    FB_UDR_MESSAGE(OutMessage,
	    (FB_INTL_VARCHAR(2040, CS_UTF8), directory)
    );


    FB_UDR_EXECUTE_FUNCTION
	{
		string ftsDirectory = LuceneFTS::getFtsDirectory(context);

	    out->directoryNull = false;
	    out->directory.length = ftsDirectory.length();
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

	LuceneFTS::LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_PROCEDURE
	{
		analyzerNames = procedure->analyzerFactory.getAnalyzerNames();
	    it = analyzerNames.begin();;
	}

	list<string> analyzerNames;
	list<string>::iterator it;

	FB_UDR_FETCH_PROCEDURE
	{
		if (it == analyzerNames.end()) {
			return false;
		}
		string analyzerName = *it;

		out->analyzerNull = false;
		out->analyzer.length = analyzerName.length();
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

	LuceneFTS::FTSIndexRepository indexRepository;
	LuceneFTS::LuceneAnalyzerFactory analyzerFactory;

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
		string indexName(in->index_name.str, in->index_name.length);

		string analyzerName;
		if (!in->analyzerNull) {
			analyzerName.assign(in->analyzer.str, in->analyzer.length);
		}

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);


		string description;
		if (!in->descriptionNull) {
		    AutoRelease<IBlob> blob(att->openBlob(status, tra, &in->description, 0, nullptr));
			description = blob_get_string(status, blob);
		    blob->close(status);
	    }


		procedure->indexRepository.createIndex(status, att, tra, sqlDialect, indexName, analyzerName, description);

		// Check if the index directory exists, and if it doesn't exist, create it. 
		string ftsDirectory = LuceneFTS::getFtsDirectory(context);
		string indexDir = ftsDirectory + "/" + indexName;
		if (!createIndexDirectory(indexDir)) {
				string error_message = string_format("Cannot create index directory \"%s\".", indexDir);
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
		string indexName(in->index_name.str, in->index_name.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);

		procedure->indexRepository.dropIndex(status, att, tra, sqlDialect, indexName);

		string ftsDirectory = LuceneFTS::getFtsDirectory(context);
		string indexDir = ftsDirectory + "/" + indexName;
		// If the directory exists, then delete it.
		if (removeIndexDirectory(indexDir)) {
			string error_message = string_format("Cannot delete index directory \"%s\".", indexDir);
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
		string indexName(in->index_name.str, in->index_name.length);
		bool indexActive = in->index_active;

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);

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
		string indexName(in->index_name.str, in->index_name.length);

		if (in->relation_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Relation name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		string relationName(in->relation_name.str, in->relation_name.length);

		if (in->field_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Field name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		string fieldName(in->field_name.str, in->field_name.length);

		double boost = 1.0;
		if (!in->boostNull) {
			boost = in->boost;
		}

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);

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
		string indexName(in->index_name.str, in->index_name.length);

		if (in->relation_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Relation name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		string relationName(in->relation_name.str, in->relation_name.length);

		if (in->field_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Field name can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		string fieldName(in->field_name.str, in->field_name.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);

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

	LuceneFTS::FTSIndexRepository indexRepository;
	LuceneFTS::RelationHelper relationHelper;
	LuceneFTS::LuceneAnalyzerFactory analyzerFactory;
	   
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
		string indexName(in->index_name.str, in->index_name.length);

		string ftsDirectory = LuceneFTS::getFtsDirectory(context);
		// check if there is a directory for full-text indexes
		if (!FileUtils::isDirectory(StringUtils::toUnicode(ftsDirectory))) {
			string error_message = string_format("Fts directory \"%s\" not exists", ftsDirectory);
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		unsigned int sqlDialect = getSqlDialect(status, att);

	    try {
			// check for index existence
			auto ftsIndex = procedure->indexRepository.getIndex(status, att, tra, sqlDialect, indexName);
			// Check if the index directory exists, and if it doesn't exist, create it. 
			string indexDir = ftsDirectory + "/" + indexName;
			if (!createIndexDirectory(indexDir)) {
				string error_message = string_format("Cannot create index directory \"%s\".", indexDir);
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
				string error_message = string_format("Cannot rebuild index \"%s\". The index does not contain segments.", indexName);
				ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS)error_message.c_str(),
					isc_arg_end
				};
				throw FbException(status, statusVector);
			}

			auto segmentsByRelation = LuceneFTS::FTSIndexRepository::groupIndexSegmentsByRelation(segments);

			auto fsIndexDir = FSDirectory::open(StringUtils::toUnicode(indexDir));
			auto analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex.analyzer);
		    IndexWriterPtr writer = newLucene<IndexWriter>(fsIndexDir, analyzer, true, IndexWriter::MaxFieldLengthLIMITED);

			// clean up index directory
		    writer->deleteAll();
		    writer->commit();

			const char* fbCharset = context->getClientCharSet();
			string icuCharset = getICICharset(fbCharset);
					
			for (const auto& p : segmentsByRelation) {
				const string relationName = p.first;
				if (!procedure->relationHelper.relationExists(status, att, tra, sqlDialect, relationName)) {
					string error_message = string_format("Cannot rebuild index \"%s\". Table \"%s\" not exists. Please delete the index segments containing it.", indexName, relationName);
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
						string error_message = string_format("Cannot rebuild index \"%s\". Field \"%s\" not exists in table \"%s\". Please delete the index segments containing it.", indexName, segment.fieldName, segment.relationName);
						ISC_STATUS statusVector[] = {
							isc_arg_gds, isc_random,
							isc_arg_string, (ISC_STATUS)error_message.c_str(),
							isc_arg_end
						};
						throw FbException(status, statusVector);
					}
					fieldNames.push_back(segment.fieldName);
				}
				const string sql = LuceneFTS::RelationHelper::buildSqlSelectFieldValues(sqlDialect, relationName, fieldNames);

				AutoRelease<IStatement> stmt(att->prepare(
					status,
					tra,
					0,
					sql.c_str(),
					sqlDialect,
					IStatement::PREPARE_PREFETCH_METADATA
				));
				AutoRelease<IMessageMetadata> outputMetadata(stmt->getOutputMetadata(status));
				unsigned colCount = outputMetadata->getCount(status);
				// make all fields of string type except BLOB
				AutoRelease<IMessageMetadata> newMeta(prepareTextMetaData(status, outputMetadata));
				auto fields = getFieldsInfo(status, newMeta);

				AutoRelease<IResultSet> rs(stmt->openCursor(
					status, 
					tra, 
					nullptr, 
					nullptr, 
					newMeta, 
					0
				));
				
				unsigned msgLength = newMeta->getMessageLength(status);
				{
					// allocate output buffer
					auto b = make_unique<unsigned char[]>(msgLength);
					unsigned char* buffer = b.get();
					while (rs->fetchNext(status, buffer) == IStatus::RESULT_OK) {
						bool emptyFlag = true;
						DocumentPtr doc = newLucene<Document>();
						string dbKey = fields[0].getStringValue(status, att, tra, buffer); 
						// RDB$DB_KEY is in a binary format that cannot be converted to Unicode, 
						// so we will convert the string to a hexadecimal representation.
						string hexDbKey = string_to_hex(dbKey);
						doc->add(newLucene<Field>(L"RDB$DB_KEY", StringUtils::toUnicode(hexDbKey), Field::STORE_YES, Field::INDEX_NOT_ANALYZED));
						doc->add(newLucene<Field>(L"RDB$RELATION_NAME", StringUtils::toUnicode(relationName), Field::STORE_YES, Field::INDEX_NOT_ANALYZED));
						for (int i = 1; i < colCount; i++) {
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
								[&field](LuceneFTS::FTSIndexSegment segment) { return segment.fieldName == field.fieldName; }
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
		    string error_message = StringUtils::toUTF8(e.getError());
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
PROCEDURE FTS$LOG_CHANGE (
    FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$REC_ID         CHAR(8) CHARACTER SET OCTETS NOT NULL,
	FTS$CHANGE_TYPE    FTS$CHANGE_TYPE NOT NULL
)
EXTERNAL NAME 'luceneudr!ftsLogChange'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsLogChange)
    FB_UDR_MESSAGE(InMessage,
	    (FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
	    (FB_INTL_VARCHAR(8, CS_BINARY), db_key)
		(FB_INTL_VARCHAR(4, CS_UTF8), change_type)
    );

	FB_UDR_CONSTRUCTOR
		, logRepository(context->getMaster())
	{
	}

	LuceneFTS::FTSLogRepository logRepository;

    FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->relation_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$RELATION_NAME can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	    string relationName(in->relation_name.str, in->relation_name.length);

		if (in->db_keyNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$REC_ID can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		string dbKey(in->db_key.str, in->db_key.length);

		if (in->change_typeNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$CHANGE_TYPE can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		string changeType(in->change_type.str, in->change_type.length);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);

		procedure->logRepository.appendLog(status, att, tra, sqlDialect, relationName, dbKey, changeType);
	}

    FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$CLEAR_LOG
EXTERNAL NAME 'luceneudr!ftsClearLog'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsClearLog)
	FB_UDR_CONSTRUCTOR
		, logRepository(context->getMaster())
	{
	}

	LuceneFTS::FTSLogRepository logRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);

		procedure->logRepository.clearLog(status, att, tra, sqlDialect);
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$UPDATE_INDEXES 
EXTERNAL NAME 'luceneudr!updateFtsIndexes'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(updateFtsIndexes)

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
		, relationHelper(context->getMaster())
		, logRepository(context->getMaster())
		, analyzerFactory()
		, prepareStmtMap()
	{
	}

	FB_UDR_DESTRUCTOR
	{
		clearPreparedStatements();
	}

	LuceneFTS::FTSIndexRepository indexRepository;
	LuceneFTS::RelationHelper relationHelper;
	LuceneFTS::FTSLogRepository logRepository;
	LuceneFTS::LuceneAnalyzerFactory analyzerFactory;
	map<string, IStatement*> prepareStmtMap;

	void clearPreparedStatements() {
		for (auto& pStmt : prepareStmtMap) {
			pStmt.second->release();
		}
		prepareStmtMap.clear();
	}

	FB_UDR_EXECUTE_PROCEDURE
	{
		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);

		string ftsDirectory = LuceneFTS::getFtsDirectory(context);

		const char* fbCharset = context->getClientCharSet();
		string icuCharset = getICICharset(fbCharset);

		map<string, LuceneFTS::FTSRelation> relationsByName;
		procedure->clearPreparedStatements();
		
		// get all indexes
		auto allIndexes = procedure->indexRepository.getAllIndexes(status, att, tra, sqlDialect);
		for (auto& ftsIndex : allIndexes) {
			// exclude inactive indexes
			if (!ftsIndex.isActive()) {
				continue;
			}
		    // get index segments
			auto segments = procedure->indexRepository.getIndexSegments(status, att, tra, sqlDialect, ftsIndex.indexName);
			for (auto& ftsSegment : segments) {
			    // looking for a table by name
				auto r = relationsByName.find(ftsSegment.relationName);
				
				if (r != relationsByName.end()) {
					// if the table is found, then add a new index and segment to it, and update the relations map.
					auto ftsRelation = r->second;
					ftsRelation.addIndex(ftsIndex);
					ftsRelation.addSegment(ftsSegment);
					relationsByName.insert_or_assign(ftsSegment.relationName, ftsRelation);
				}
				else {
					// if there is no such table yet, add it
					LuceneFTS::FTSRelation ftsRelation(ftsSegment.relationName);
					// add a new index and segment to it
					ftsRelation.addIndex(ftsIndex);
					ftsRelation.addSegment(ftsSegment);
					relationsByName.insert_or_assign(ftsSegment.relationName, ftsRelation);
				}
			}
		}
		
		// now it is necessary for each table for each index to build queries to extract records
		for (auto& p : relationsByName) {
			string relationName = p.first;
			auto ftsRelation = p.second;
			auto ftsIndexes = ftsRelation.getIndexes();
			for (auto& pIndex : ftsIndexes) {
				auto ftsIndex = pIndex.second;
				// exclude inactive indexes
				if (!ftsIndex.isActive()) {
					continue;
				}
				auto segments = ftsRelation.getSegmentsByIndexName(ftsIndex.indexName);
				list<string> fieldNames;
				for (const auto& segment : segments) {
					if (procedure->relationHelper.fieldExists(status, att, tra, sqlDialect, segment.relationName, segment.fieldName)) {
						fieldNames.push_back(segment.fieldName);
					}
					else {
						// if the field does not exist, then you need to mark the index as requiring updating
						if (ftsIndex.status == "C") {
							ftsIndex.status = "U";
							// this is done in an autonomous transaction
							AutoRelease<ITransaction> aTra(att->startTransaction(status, 0, nullptr));
							procedure->indexRepository.setIndexStatus(status, att, aTra, sqlDialect, ftsIndex.indexName, ftsIndex.status);
							aTra->commit(status);
							ftsRelation.updateIndex(ftsIndex);
						}
					}
				}
				string sql = LuceneFTS::RelationHelper::buildSqlSelectFieldValues(sqlDialect, relationName, fieldNames, true);
				ftsRelation.setSql(ftsIndex.indexName, sql);
			}
			relationsByName.insert_or_assign(relationName, ftsRelation);
		}
		
		FB_MESSAGE(ValInput, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(8, CS_BINARY), db_key)
		) selValInput(status, context->getMaster());
		

		map<string, IndexWriterPtr> indexWriters;

		
		// get the log of changes of records for the index
		AutoRelease<IStatement> logStmt(att->prepare(
			status,
			tra,
			0,
			"SELECT FTS$LOG_ID, FTS$RELATION_NAME, FTS$REC_ID, FTS$CHANGE_TYPE\n"
			"FROM FTS$LOG\n"
			"ORDER BY FTS$LOG_ID",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));

		FB_MESSAGE(LogOutput, ThrowStatusWrapper,
			(FB_BIGINT, id)
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_VARCHAR(8), dbKey)	
			(FB_INTL_VARCHAR(4, CS_UTF8), changeType)

		) logOutput(status, context->getMaster());
		logOutput.clear();

		AutoRelease<IResultSet> logRs(logStmt->openCursor(
			status,
			tra,
			nullptr,
			nullptr,
			logOutput.getMetadata(),
			0
		));
        

		while (logRs->fetchNext(status, logOutput.getData()) == IStatus::RESULT_OK) {
			ISC_INT64 logId = logOutput->id;
			string dbKey(logOutput->dbKey.str, logOutput->dbKey.length);
			string relationName(logOutput->relationName.str, logOutput->relationName.length);
			string changeType(logOutput->changeType.str, logOutput->changeType.length);

			string hexDbKey = string_to_hex(dbKey);

			// looking for a table in the list of indexed tables
			auto r = relationsByName.find(relationName);
			if (r != relationsByName.end()) {
				// for each table we get a list of indexes 
				LuceneFTS::FTSRelation ftsRelation = r->second;
				auto ftsIndexes = ftsRelation.getIndexes();
				
				for (auto& pIndex : ftsIndexes) {
					string indexName = pIndex.first;
					auto ftsIndex = pIndex.second;
					// exclude inactive indexes
					if (!ftsIndex.isActive()) {
						continue;
					}
					auto ftsSegments = ftsRelation.getSegmentsByIndexName(indexName);
					// looking for an IndexWriter
					auto iWriter = indexWriters.find(ftsIndex.indexName);
					// if not found, then open this
					if (iWriter == indexWriters.end()) {
						// Check if the index directory exists, and if it doesn't exist, create it. 
						string indexDir = ftsDirectory + "/" + indexName;
						auto indexDirUnicode = StringUtils::toUnicode(indexDir);
						if (!FileUtils::isDirectory(indexDirUnicode)) {
							if (ftsIndex.status == "C") {
								// If the index directory does not exist, then mark the index as requiring reindexing.
								AutoRelease<ITransaction> aTra(att->startTransaction(status, 0, nullptr));
								procedure->indexRepository.setIndexStatus(status, att, aTra, sqlDialect, ftsIndex.indexName, ftsIndex.status);
								aTra->commit(status);
							}
							// go to next index
							continue;
						}
						auto fsIndexDir = FSDirectory::open(indexDirUnicode);
						auto analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex.analyzer);
						IndexWriterPtr writer = newLucene<IndexWriter>(fsIndexDir, analyzer, IndexWriter::MaxFieldLengthLIMITED);
						indexWriters[indexName] = writer;
					}
					IndexWriterPtr writer = indexWriters[indexName];
					if ((changeType == "I") || (changeType == "U")) {
						string stmtName = relationName + "." + indexName;
						// looking for a prepared statement, and if it is not there, we prepare it
						auto iStmt = procedure->prepareStmtMap.find(stmtName);
						if (iStmt == procedure->prepareStmtMap.end()) {
							IStatement* stmt = att->prepare(
								status,
								tra,
								0,
								ftsRelation.getSql(indexName).c_str(),
								sqlDialect,
								IStatement::PREPARE_PREFETCH_METADATA
							);
							procedure->prepareStmtMap[stmtName] = stmt;
						}
						auto stmt = procedure->prepareStmtMap[stmtName];
						// get the desired field values
						AutoRelease<IMessageMetadata> outputMetadata(stmt->getOutputMetadata(status));
						unsigned colCount = outputMetadata->getCount(status);
						// make all fields of string type except BLOB
						AutoRelease<IMessageMetadata> newMeta(prepareTextMetaData(status, outputMetadata));
						auto fields = getFieldsInfo(status, newMeta);

						selValInput->db_keyNull = false;
						selValInput->db_key.length = dbKey.length();
						dbKey.copy(selValInput->db_key.str, selValInput->db_key.length);

						AutoRelease<IResultSet> rs(stmt->openCursor(
							status,
							tra,
							selValInput.getMetadata(),
							selValInput.getData(),
							newMeta,
							0
						));

						unsigned msgLength = newMeta->getMessageLength(status);
						{
							// allocate output buffer
							auto b = make_unique<unsigned char[]>(msgLength);
							unsigned char* buffer = b.get();
							while (rs->fetchNext(status, buffer) == IStatus::RESULT_OK) {
								bool emptyFlag = true;
								DocumentPtr doc = newLucene<Document>();
								doc->add(newLucene<Field>(L"RDB$DB_KEY", StringUtils::toUnicode(hexDbKey), Field::STORE_YES, Field::INDEX_NOT_ANALYZED));
								doc->add(newLucene<Field>(L"RDB$RELATION_NAME", StringUtils::toUnicode(relationName), Field::STORE_YES, Field::INDEX_NOT_ANALYZED));
								for (int i = 1; i < colCount; i++) {
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
										ftsSegments.begin(), 
										ftsSegments.end(), 
										[&field](LuceneFTS::FTSIndexSegment ftsSegment) { return ftsSegment.fieldName == field.fieldName; }
									);
									if (iSegment != ftsSegments.end()) {
										luceneField->setBoost((*iSegment).boost);
									}
									
									doc->add(luceneField);
									emptyFlag = emptyFlag && value.empty();
								}
								if ((changeType == "I") && !emptyFlag) {
									writer->addDocument(doc);
								}
								if (changeType == "U") {
									TermPtr term = newLucene<Term>(L"RDB$DB_KEY", StringUtils::toUnicode(hexDbKey));
									if (!emptyFlag) {
										writer->updateDocument(term, doc);
									}
									else {
										writer->deleteDocuments(term);
									}
								}
							}
							rs->close(status);
						}
					}
					else if (changeType == "D") {
						TermPtr term = newLucene<Term>(L"RDB$DB_KEY", StringUtils::toUnicode(hexDbKey));
						writer->deleteDocuments(term);
					}
				}
			}
			procedure->logRepository.deleteLog(status, att, tra, sqlDialect, logId);
		}
		logRs->close(status);
		// commit changes for all indexes
		for (auto& pIndexWriter : indexWriters) {
			auto indexWriter = pIndexWriter.second;
			indexWriter->commit();
			indexWriter->close();
		}
		// clean up prepared statements
		procedure->clearPreparedStatements();
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$SEARCH (
	FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$SEARCH_RELATION VARCHAR(63) CHARACTER SET UTF8,
	FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
	FTS$LIMIT INT NOT NULL DEFAULT 1000,
	FTS$EXPLAIN BOOLEAN DEFAULT FALSE
)
RETURNS (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
	FTS$REC_ID CHAR(8) CHARACTER SET OCTETS,
	FTS$SCORE DOUBLE PRECISION,
	FTS$EXPLANATION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!ftsSearch'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsSearch)
    FB_UDR_MESSAGE(InMessage,
	    (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_INTL_VARCHAR(32765, CS_UTF8), query)
		(FB_BIGINT, limit)
		(FB_BOOLEAN, explain)
    );

	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_INTL_VARCHAR(8, CS_BINARY), rec_id)
		(FB_DOUBLE, score)
		(FB_BLOB, explanation)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
		, analyzerFactory()
	{
	}

	LuceneFTS::FTSIndexRepository indexRepository;
	LuceneFTS::LuceneAnalyzerFactory analyzerFactory;

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
	    string indexName(in->index_name.str, in->index_name.length);

		string relationName;
		if (!in->relation_nameNull) {
			relationName.assign(in->relation_name.str, in->relation_name.length);
		}

		string queryStr;
		if (!in->queryNull) {
			queryStr.assign(in->query.str, in->query.length);
		}

		ISC_INT64 limit = in->limit;

		if (!in->explainNull) {
			explainFlag = in->explain;
		}

		string ftsDirectory = LuceneFTS::getFtsDirectory(context);


		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);

		auto ftsIndex = procedure->indexRepository.getIndex(status, att, tra, sqlDialect, indexName);

		// check if directory exists for index
		auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
		if (ftsIndex.status == "N" || !FileUtils::isDirectory(indexDir)) {			
			string error_message = string_format("Index \"%s\" exists, but is not build. Please rebuild index.", indexName);
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		try {
			auto fsIndexDir = FSDirectory::open(indexDir);
			IndexReaderPtr reader = IndexReader::open(fsIndexDir, true);
			searcher = newLucene<IndexSearcher>(reader);
			AnalyzerPtr analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex.analyzer);
			auto segments = procedure->indexRepository.getIndexSegments(status, att, tra, sqlDialect, indexName);
			if (!relationName.empty()) {
				// if a table name is given, then select only segments with this table
				auto segmentsByRelation = LuceneFTS::FTSIndexRepository::groupIndexSegmentsByRelation(segments);
				auto el = segmentsByRelation.find(relationName);
				if (el == segmentsByRelation.end()) {
					string error_message = string_format("Relation \"%s\" not exists in index \"%s\".", relationName, indexName);
					ISC_STATUS statusVector[] = {
						isc_arg_gds, isc_random,
						isc_arg_string, (ISC_STATUS)error_message.c_str(),
						isc_arg_end
					};
					throw FbException(status, statusVector);
				}
				segments = el->second;
			}
			
			auto fields = Collection<String>::newInstance();
			for (auto segment: segments) {
				fields.add(StringUtils::toUnicode(segment.getFullFieldName()));
			}

			MultiFieldQueryParserPtr parser = newLucene<MultiFieldQueryParser>(LuceneVersion::LUCENE_CURRENT, fields, analyzer);
			parser->setDefaultOperator(QueryParser::OR_OPERATOR);
			query = parser->parse(StringUtils::toUnicode(queryStr));
			TopDocsPtr docs = searcher->search(query, limit);

			scoreDocs = docs->scoreDocs;

			it = scoreDocs.begin();
			
			out->relation_nameNull = true;
			out->rec_idNull = true;
			out->scoreNull = true;
		}
		catch (LuceneException& e) {
			string error_message = StringUtils::toUTF8(e.getError());
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	}

	bool explainFlag = false;
	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	QueryPtr query;
	SearcherPtr searcher;
	Collection<ScoreDocPtr> scoreDocs;
	Collection<ScoreDocPtr>::iterator it;

	FB_UDR_FETCH_PROCEDURE
	{
		if (it == scoreDocs.end()) {
			return false;
		}
	    ScoreDocPtr scoreDoc = *it;
		DocumentPtr doc = searcher->doc(scoreDoc->doc);
		string relationName = StringUtils::toUTF8(doc->get(L"RDB$RELATION_NAME"));
		string hexDbKey = StringUtils::toUTF8(doc->get(L"RDB$DB_KEY"));
		// In the Lucene index, the string is stored in hexadecimal form, so let's convert it back to binary format.
		string dbKey = hex_to_string(hexDbKey);
		
        out->relation_nameNull = false;
		out->relation_name.length = relationName.length();
		relationName.copy(out->relation_name.str, out->relation_name.length);
		
		out->rec_idNull = false;
		out->rec_id.length = dbKey.length();
		dbKey.copy(out->rec_id.str, out->rec_id.length);

        out->scoreNull = false;
		out->score = scoreDoc->score;

		if (explainFlag) {
			out->explanationNull = false;
			auto explanation = searcher->explain(query, scoreDoc->doc);
			string explanationStr = StringUtils::toUTF8(explanation->toString());
			AutoRelease<IBlob> blob(att->createBlob(status, tra, &out->explanation, 0, nullptr));
			blob_set_string(status, blob, explanationStr);
			blob->close(status);
		}
		else {
			out->explanationNull = true;
		}

	    ++it;
	    return true;
	}
FB_UDR_END_PROCEDURE


/***
FUNCTION FTS$BEST_FRAGMENT (
	FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
	FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
	FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
	FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
	FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
	FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
	FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>'
)
RETURNS VARCHAR(8191) CHARACTER SET UTF8
EXTERNAL NAME 'luceneudr!bestFragementHighligh'
ENGINE UDR;
***/
FB_UDR_BEGIN_FUNCTION(bestFragementHighligh)
    FB_UDR_MESSAGE(InMessage,
		(FB_BLOB, text)
		(FB_INTL_VARCHAR(32765, CS_UTF8), query)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzer_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), field_name)
		(FB_SMALLINT, fragment_size)
		(FB_INTL_VARCHAR(200, CS_UTF8), left_tag)
		(FB_INTL_VARCHAR(200, CS_UTF8), right_tag)
    );

    FB_UDR_MESSAGE(OutMessage,
	    (FB_INTL_VARCHAR(32765, CS_UTF8), fragment)
    );

	FB_UDR_CONSTRUCTOR
		, analyzerFactory()
	{
	}

	LuceneFTS::LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_FUNCTION
	{
		AutoRelease<IAttachment> att(context->getAttachment(status));
	    AutoRelease<ITransaction> tra(context->getTransaction(status));

		out->fragmentNull = true;

		string text;
		if (!in->textNull) {
			AutoRelease<IBlob> blob(att->openBlob(status, tra, &in->text, 0, nullptr));
			text = blob_get_string(status, blob);
			blob->close(status);
		}
		else {
			return;
		}

		string queryStr;
		if (!in->queryNull) {
			queryStr.assign(in->query.str, in->query.length);
		}

		string analyzerName = "STANDARD";
		if (!in->analyzer_nameNull) {
			analyzerName.assign(in->analyzer_name.str, in->analyzer_name.length);
		}

		string fieldName;
		if (!in->field_nameNull) {
			fieldName.assign(in->field_name.str, in->field_name.length);
		}

		ISC_SHORT fragmentSize = in->fragment_size;

		if (fragmentSize > 8191) {
		    // exceeds Firebird's maximum string size
			string error_message = "Fragment size cannot exceed 8191 characters";
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		if (fragmentSize <= 0) {
			string error_message = "Fragment size must be greater than 0";
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}

		string leftTag;
		if (!in->left_tagNull) {
			leftTag.assign(in->left_tag.str, in->left_tag.length);
		}

		string rightTag;
		if (!in->right_tagNull) {
			rightTag.assign(in->right_tag.str, in->right_tag.length);
		}

		try {
			auto analyzer = analyzerFactory.createAnalyzer(status, analyzerName);
			auto parser = newLucene<QueryParser>(LuceneVersion::LUCENE_CURRENT, StringUtils::toUnicode(fieldName), analyzer);
			auto query = parser->parse(StringUtils::toUnicode(queryStr));
			auto formatter = newLucene<SimpleHTMLFormatter>(StringUtils::toUnicode(leftTag), StringUtils::toUnicode(rightTag));
			auto scorer = newLucene<QueryScorer>(query);
			auto highlighter = newLucene<Highlighter>(formatter, scorer);
			auto fragmenter = newLucene<SimpleSpanFragmenter>(scorer, fragmentSize);
			highlighter->setTextFragmenter(fragmenter);
			auto content = highlighter->getBestFragment(analyzer, StringUtils::toUnicode(fieldName), StringUtils::toUnicode(text));
						
			if (!content.empty()) {
				if (content.length() > 8191) {
					string error_message = "Fragment size exceeds 8191 characters";
					ISC_STATUS statusVector[] = {
						isc_arg_gds, isc_random,
						isc_arg_string, (ISC_STATUS)error_message.c_str(),
						isc_arg_end
					};
					throw FbException(status, statusVector);
				}
				string fragment = StringUtils::toUTF8(content);
				out->fragmentNull = false;
				out->fragment.length = fragment.length();
				fragment.copy(out->fragment.str, out->fragment.length);
			}
		}
		catch (LuceneException& e) {
			string error_message = StringUtils::toUTF8(e.getError());
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	}
FB_UDR_END_FUNCTION

/***
PROCEDURE FTS$BEST_FRAGMENTS (
	FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
	FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
	FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD',
	FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 DEFAULT NULL,
	FTS$FRAGMENT_SIZE SMALLINT NOT NULL DEFAULT 512,
	FTS$MAX_NUM_FRAGMENTS INTEGER NOT NULL DEFAULT 10,
	FTS$LEFT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '<b>',
	FTS$RIGHT_TAG VARCHAR(50) CHARACTER SET UTF8 NOT NULL DEFAULT '</b>'
)
RETURNS (
    FTS$FRAGMENT VARCHAR(8191) CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!bestFragementsHighligh'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(bestFragementsHighligh)
    FB_UDR_MESSAGE(InMessage,
	    (FB_BLOB, text)
	    (FB_INTL_VARCHAR(32765, CS_UTF8), query)
	    (FB_INTL_VARCHAR(252, CS_UTF8), analyzer_name)
	    (FB_INTL_VARCHAR(252, CS_UTF8), field_name)
	    (FB_SMALLINT, fragment_size)
		(FB_INTEGER, maxNumFragments)
	    (FB_INTL_VARCHAR(200, CS_UTF8), left_tag)
	    (FB_INTL_VARCHAR(200, CS_UTF8), right_tag)
    );

    FB_UDR_MESSAGE(OutMessage,
	    (FB_INTL_VARCHAR(32765, CS_UTF8), fragment)
    );

    FB_UDR_CONSTRUCTOR
        , analyzerFactory()
    {
    }

    LuceneFTS::LuceneAnalyzerFactory analyzerFactory;

    FB_UDR_EXECUTE_PROCEDURE
    {
	    att.reset(context->getAttachment(status));
	    tra.reset(context->getTransaction(status));

	    out->fragmentNull = true;

	    string text;
	    if (!in->textNull) {
		    AutoRelease<IBlob> blob(att->openBlob(status, tra, &in->text, 0, nullptr));
		    text = blob_get_string(status, blob);
		    blob->close(status);
	    }

	    string queryStr;
	    if (!in->queryNull) {
		    queryStr.assign(in->query.str, in->query.length);
	    }

	    string analyzerName = "STANDARD";
	    if (!in->analyzer_nameNull) {
		    analyzerName.assign(in->analyzer_name.str, in->analyzer_name.length);
	    }

	    string fieldName;
	    if (!in->field_nameNull) {
		    fieldName.assign(in->field_name.str, in->field_name.length);
	    }

	    ISC_SHORT fragmentSize = in->fragment_size;

	    if (fragmentSize > 8191) {
			// exceeds Firebird's maximum string size
		    string error_message = "Fragment size cannot exceed 8191 characters";
		    ISC_STATUS statusVector[] = {
			    isc_arg_gds, isc_random,
			    isc_arg_string, (ISC_STATUS)error_message.c_str(),
			    isc_arg_end
		    };
		    throw FbException(status, statusVector);
	    }
	    if (fragmentSize <= 0) {
		    string error_message = "Fragment size must be greater than 0";
		    ISC_STATUS statusVector[] = {
			    isc_arg_gds, isc_random,
			    isc_arg_string, (ISC_STATUS)error_message.c_str(),
			    isc_arg_end
		    };
		    throw FbException(status, statusVector);
	    }

		ISC_LONG maxNumFragments = in->maxNumFragments;

	    string leftTag;
	    if (!in->left_tagNull) {
		    leftTag.assign(in->left_tag.str, in->left_tag.length);
	    }

	    string rightTag;
	    if (!in->right_tagNull) {
		    rightTag.assign(in->right_tag.str, in->right_tag.length);
	    }

		try {
			auto analyzer = procedure->analyzerFactory.createAnalyzer(status, analyzerName);
			auto parser = newLucene<QueryParser>(LuceneVersion::LUCENE_CURRENT, StringUtils::toUnicode(fieldName), analyzer);
			auto query = parser->parse(StringUtils::toUnicode(queryStr));
			auto formatter = newLucene<SimpleHTMLFormatter>(StringUtils::toUnicode(leftTag), StringUtils::toUnicode(rightTag));
			auto scorer = newLucene<QueryScorer>(query);
			auto highlighter = newLucene<Highlighter>(formatter, scorer);
			auto fragmenter = newLucene<SimpleSpanFragmenter>(scorer, fragmentSize);
			highlighter->setTextFragmenter(fragmenter);
			
			fragments = highlighter->getBestFragments(analyzer, StringUtils::toUnicode(fieldName), StringUtils::toUnicode(text), maxNumFragments);
			it = fragments.begin();
		}
		catch (LuceneException& e) {
			string error_message = StringUtils::toUTF8(e.getError());
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
	Collection<String> fragments;
	Collection<String>::iterator it;

	FB_UDR_FETCH_PROCEDURE
	{
		out->fragmentNull = true;
		if (it == fragments.end()) {
			return false;
		}
	    auto content = *it;
				
		if (!content.empty()) {
			if (content.length() > 8191) {
				string error_message = "Fragment size exceeds 8191 characters";
				ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS)error_message.c_str(),
					isc_arg_end
				};
				throw FbException(status, statusVector);
			}
			string fragment = StringUtils::toUTF8(content);
			out->fragmentNull = false;
			out->fragment.length = fragment.length();
			fragment.copy(out->fragment.str, out->fragment.length);
		}

		++it;
	    return true;
	}
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$MAKE_TRIGGER (
	FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
	FTS$MULTI_ACTION BOOLEAN DEFAULT TRUE
)
RETURNS (
	FTS$TRIGGER_SOURCE BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!ftsMakeTrigger'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsMakeTrigger)
    FB_UDR_MESSAGE(InMessage,
	   (FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
	   (FB_BOOLEAN, multi_action)
    );

	FB_UDR_MESSAGE(OutMessage,
		(FB_BLOB, triggerSource)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
	{
	}

	LuceneFTS::FTSIndexRepository indexRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->relation_nameNull) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"FTS$RELATION_NAME can not be NULL",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		string relationName(in->relation_name.str, in->relation_name.length);

		bool multiActionFlag = true;
		if (!in->multi_actionNull) {
			multiActionFlag = in->multi_action;
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);


		try {
			// TODO: needs map source of trigger events
			triggerSources = procedure->indexRepository.makeTriggerSourceByRelation(status, att, tra, sqlDialect, relationName, multiActionFlag);
			it = triggerSources.begin();
		}
		catch (LuceneException& e) {
			string error_message = StringUtils::toUTF8(e.getError());
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)error_message.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
	}

	list<string> triggerSources;
	list<string>::iterator it;
	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;


	FB_UDR_FETCH_PROCEDURE
	{
		if (it == triggerSources.end()) {
			out->triggerSourceNull = true;
			return false;
		}
	
	    
		string triggerSource = *it;

		out->triggerSourceNull = false;
	    AutoRelease<IBlob> blob(att->createBlob(status, tra, &out->triggerSource, 0, nullptr));
	    blob_set_string(status, blob, triggerSource);
	    blob->close(status);

	    ++it;
	    return true;
	}
FB_UDR_END_PROCEDURE

/*

// CREATE OR ALTER TRIGGER FTS$TR_HORSE FOR HORSE
// ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 100
// EXTERNAL NAME 'luceneudr!trFtsLog'
// ENGINE UDR;

FB_UDR_BEGIN_TRIGGER(trFtsLog)

    FB_UDR_CONSTRUCTOR
       , triggerTable(metadata->getTriggerTable(status))
       , indexRepository(context->getMaster())
	{
	
		AutoRelease<IMessageMetadata> origTriggerMetadata(metadata->getTriggerMetadata(status));
		AutoRelease<IMetadataBuilder> builder(origTriggerMetadata->getBuilder(status));
		auto fieldIndex = builder->addField(status);
		builder->setField(status, fieldIndex, "RDB$DB_KEY");
		builder->setType(status, fieldIndex, SQL_TEXT);
		builder->setLength(status, fieldIndex, 8);
		builder->setCharSet(status, fieldIndex, CS_BINARY);
		triggerMetadata.reset(builder->getMetadata(status));
	}

	LuceneFTS::FTSIndexRepository indexRepository;

	FB_UDR_EXECUTE_TRIGGER
	{
		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		unsigned int sqlDialect = getSqlDialect(status, att);

		// получаем сегменты FTS индекса
		auto segments = indexRepository.getIndexSegmentsByRelation(status, att, tra, sqlDialect, triggerTable);
		// если нет ни одного сегмента выходим
		if (segments.size() == 0)
			return;
		auto fieldsInfo = getFieldsInfo(status, triggerMetadata);
		int dbKeyIndex = findFieldByName(fieldsInfo, "RDB$DB_KEY");
		

		if (action == IExternalTrigger::ACTION_INSERT) {
			string dbKey = fieldsInfo[dbKeyIndex].getStringValue(status, att, tra, newFields);
			bool changeFlag = false;
			for (auto& segment : segments) {
			    int fieldIndex = findFieldByName(fieldsInfo, segment.fieldName);
				if (fieldIndex < 0) {
					string error_message = string_format("Invalid index segment \"%s\".\"%s\" for index \"%s\".", segment.relationName, segment.fieldName, segment.indexName);
					ISC_STATUS statusVector[] = {
						isc_arg_gds, isc_random,
						isc_arg_string, (ISC_STATUS)error_message.c_str(),
						isc_arg_end
					};
					throw FbException(status, statusVector);
				}
			}
		}
		if (action == IExternalTrigger::ACTION_UPDATE) {
			string dbKey = fieldsInfo[dbKeyIndex].getStringValue(status, att, tra, newFields);
			string hexDbKey = string_to_hex(dbKey);
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)hexDbKey.c_str(),
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		if (action == IExternalTrigger::ACTION_DELETE) {
			string dbKey = fieldsInfo[dbKeyIndex].getStringValue(status, att, tra, newFields);
		}
	}

	string triggerTable;
	AutoRelease<IMessageMetadata> triggerMetadata;
	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
FB_UDR_END_TRIGGER
*/

FB_UDR_IMPLEMENT_ENTRY_POINT