
#include "LuceneUdr.h"
#include "FTSIndex.h"
#include "Relations.h"
#include "FBBlobUtils.h"
#include "FBFieldInfo.h"
#include "EncodeUtils.h"
#include "lucene++\LuceneHeaders.h"
#include "lucene++\FileUtils.h"
#include "lucene++\MiscUtils.h"
#include "inicpp.h"
#include <sstream>
#include <vector>
#include <memory>

using namespace Firebird;
using namespace Lucene;

IMessageMetadata* prepareTextMetaData(ThrowStatusWrapper* status, IMessageMetadata* meta)
{
	unsigned colCount = meta->getCount(status);
	// ������ ��� ���� ���������� ����, ����� BLOB
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


string getFtsDirectory(IExternalContext* context) {
	IConfigManager* configManager = context->getMaster()->getConfigManager();
	const char* databaseName = context->getDatabaseName();

	string rootDir(configManager->getRootDirectory());

	ini::IniFile iniFile;
	iniFile.load(rootDir + "/fts.ini");
	auto section = iniFile[databaseName];
	return section["ftsDirectory"].as<string>();
}


/***
CREATE FUNCTION FTS$GET_DIRECTORY () 
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
		string ftsDirectory = getFtsDirectory(context);

	    out->directory.length = ftsDirectory.length();
		ftsDirectory.copy(out->directory.str, out->directory.length);
	}
FB_UDR_END_FUNCTION

/***
CREATE PROCEDURE FTS$CREATE_INDEX (
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
	{
	}

	LuceneFTS::FTSIndexRepository indexRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwFbException(status, "Index name can not be NULL");
		}
		indexName.assign(in->index_name.str, in->index_name.length);
		if (!in->analyzerNull) {
			analyzerName.assign(in->analyzer.str, in->analyzer.length);
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		// TODO: � ��������� ����� ����������� �� �����������
		// ����� ��� ����� ����������� ���������� �������� �������������

		// �������� ������������� �������
		if (procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message = "";
			error_message += "Index \"" + indexName + "\" already exists";
			throwFbException(status, error_message.c_str());
		}

		if (!in->descriptionNull) {
		    AutoRelease<IBlob> blob(att->openBlob(status, tra, &in->description, 0, nullptr));
			description = blob_get_string(status, blob);
		    blob->close(status);
	    }

		procedure->indexRepository.createIndex(status, att, tra, indexName, analyzerName, description);

		// �������� ������������� ���������� ��� �������
        // � ���� ��� �� ���������� ������ �
		string ftsDirectory = getFtsDirectory(context);
		auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
		if (!FileUtils::isDirectory(indexDir)) {
			bool result = FileUtils::createDirectory(indexDir);
			if (!result) {
				string error_message = "";
				error_message += "Cannot create index directory " + ftsDirectory + "/" + indexName;
				throwFbException(status, error_message.c_str());
			}
		}
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string indexName;
	string analyzerName;
	string description;

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE

/***
CREATE PROCEDURE FTS$DROP_INDEX (
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
			throwFbException(status, "Index name can not be NULL");
		}
		indexName.assign(in->index_name.str, in->index_name.length);

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		// �������� ������������� �������
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message = "";
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		string ftsDirectory = getFtsDirectory(context);
		auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
		if (FileUtils::isDirectory(indexDir)) {
			// ���� ���������� ����, �� ������� �
			FileUtils::removeDirectory(indexDir);
		}

		procedure->indexRepository.dropIndex(status, att, tra, indexName);
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string indexName;

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE

/***
CREATE PROCEDURE FTS$ADD_INDEX_FIELD (
	 FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	 FTS$FIELD_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL
)
EXTERNAL NAME 'luceneudr!addIndexField'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(addIndexField)
    FB_UDR_MESSAGE(InMessage,
	    (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), field_name)
    );

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
		, relationHelper(context->getMaster())
	{
	}

	LuceneFTS::FTSIndexRepository indexRepository;
	LuceneFTS::RelationHelper relationHelper;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwFbException(status, "Index name can not be NULL");
		}
		indexName.assign(in->index_name.str, in->index_name.length);

		if (in->relation_nameNull) {
			throwFbException(status, "Relation name can not be NULL");
		}
		relationName.assign(in->relation_name.str, in->relation_name.length);

		if (in->field_nameNull) {
			throwFbException(status, "Field name can not be NULL");
		}
		fieldName.assign(in->field_name.str, in->field_name.length);

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		// �������� ������������� �������
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message = "";
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		// �������� ������������� �������
		if (!procedure->relationHelper.relationExists(status, att, tra, relationName)) {
			string error_message = "";
			error_message += "Table \"" + relationName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		// �������� ������������� ����
		if (!procedure->relationHelper.fieldExists(status, att, tra, relationName, fieldName)) {
			string error_message = "";
			error_message += "Field \"" + fieldName + "\" not exitsts in table \"" + relationName + "\"";
			throwFbException(status, error_message.c_str());
		}

		// �������� ������������� ��������
		if (procedure->indexRepository.hasIndexSegment(status, att, tra, indexName, relationName, fieldName)) {
			string error_message = "";
			error_message += "Segment for \"" + relationName + "\".\"" + fieldName + "\" already exitsts in index \"" + indexName + "\"";
			throwFbException(status, error_message.c_str());
		}

		// ���������� ��������
		procedure->indexRepository.addIndexField(status, att, tra, indexName, relationName, fieldName);
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string indexName;
	string relationName;
	string fieldName;

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE

/***
CREATE PROCEDURE FTS$DROP_INDEX_FIELD (
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
		, relationHelper(context->getMaster())
	{
	}

	LuceneFTS::FTSIndexRepository indexRepository;
	LuceneFTS::RelationHelper relationHelper;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwFbException(status, "Index name can not be NULL");
		}
		indexName.assign(in->index_name.str, in->index_name.length);

		if (in->relation_nameNull) {
			throwFbException(status, "Relation name can not be NULL");
		}
		relationName.assign(in->relation_name.str, in->relation_name.length);

		if (in->field_nameNull) {
			throwFbException(status, "Field name can not be NULL");
		}
		fieldName.assign(in->field_name.str, in->field_name.length);

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		// �������� ������������� �������
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message = "";
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		// �������� ������������� �������
		if (!procedure->relationHelper.relationExists(status, att, tra, relationName)) {
			string error_message = "";
			error_message += "Table \"" + relationName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		// �������� ������������� ����
		if (!procedure->relationHelper.fieldExists(status, att, tra, relationName, fieldName)) {
			string error_message = "";
			error_message += "Field \"" + fieldName + "\" not exitsts in table \"" + relationName + "\"";
			throwFbException(status, error_message.c_str());
		}

		// �������� ������������� ��������
		if (!procedure->indexRepository.hasIndexSegment(status, att, tra, indexName, relationName, fieldName)) {
			string error_message = "";
			error_message += "Segment for \"" + relationName + "\".\"" + fieldName + "\" not exists in index \"" + indexName + "\"";
			throwFbException(status, error_message.c_str());
		}

		// �������� ��������
		procedure->indexRepository.dropIndexField(status, att, tra, indexName, relationName, fieldName);
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string indexName;
	string relationName;
	string fieldName;

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE

/***
CREATE PROCEDURE FTS$REBUILD_INDEX (
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
    {
    }

	LuceneFTS::FTSIndexRepository indexRepository;
	LuceneFTS::RelationHelper relationHelper;
	   
    FB_UDR_EXECUTE_PROCEDURE
    {		
		if (in->index_nameNull) {
			throwFbException(status, "Index name can not be NULL");
		}
	    indexName.assign(in->index_name.str, in->index_name.length);

	    string ftsDirectory = getFtsDirectory(context);
		// �������� ���� �� ���������� ��� �������������� ��������
	    String ftsUnicodeDirectory = StringUtils::toUnicode(ftsDirectory);
	    if (!FileUtils::isDirectory(ftsUnicodeDirectory)) {
		    string error_message = "";
		    error_message += "Fts directory \"" + ftsDirectory + "\" not exists";
		    throwFbException(status, error_message.c_str());
	    }
	
		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		// �������� ������������� �������
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message = "";
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}
		// �������� ������������� ���������� ��� �������
		// � ���� ��� �� ���������� ������ �
	    auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
        if (!FileUtils::isDirectory(indexDir)) {
		    bool result = FileUtils::createDirectory(indexDir);
		    if (!result) {
			    string error_message = "";
			    error_message += "Cannot create index directory " + ftsDirectory + "/" + indexName;
			    throwFbException(status, error_message.c_str());
		    }
        }

	    try {
			auto fsIndexDir = FSDirectory::open(indexDir);
			auto analyzer = newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT);
		    IndexWriterPtr writer = newLucene<IndexWriter>(fsIndexDir, analyzer, true, IndexWriter::MaxFieldLengthLIMITED);

			// ������� ���������� �������
		    writer->deleteAll();
		    writer->commit();

			const char* fbCharset = context->getClientCharSet();
			string icuCharset = getICICharset(fbCharset);
			
			// �������� �������� ������� � ���������� �� �� ������ ������
			auto segments = procedure->indexRepository.getIndexSegments(status, att, tra, indexName);
			auto segmentsByRelation = LuceneFTS::FTSIndexRepository::groupIndexSegmentsByRelation(segments);
			
			for (const auto& p : segmentsByRelation) {
				const string relationName = p.first;
				if (!procedure->relationHelper.relationExists(status, att, tra, relationName)) {
					// ���� ������� �� ���������� ������ ���������� ���� �������
					continue;
				}
				const auto segments = p.second;
				list<string> fieldNames;
				for (const auto& segment : segments) {
					if (procedure->relationHelper.fieldExists(status, att, tra, segment.relationName, segment.fieldName)) {
						// ���������� �� ������������ ����
						fieldNames.push_back(segment.fieldName);
					}
				}
				const string sql = LuceneFTS::RelationHelper::buildSqlSelectFieldValues(relationName, fieldNames);

				AutoRelease<IStatement> stmt(att->prepare(
					status,
					tra,
					0,
					sql.c_str(),
					UDR_SQL_DIALECT,
					IStatement::PREPARE_PREFETCH_METADATA
				));
				AutoRelease<IMessageMetadata> outputMetadata(stmt->getOutputMetadata(status));
				unsigned colCount = outputMetadata->getCount(status);
				// ������ ��� ���� ���������� ����, ����� BLOB
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
					//char* buffer = new char[msgLength];
					auto b = make_unique<unsigned char[]>(msgLength);
					unsigned char* buffer = b.get();
					while (rs->fetchNext(status, buffer) == IStatus::RESULT_OK) {
						bool emptyFlag = true;
						DocumentPtr doc = newLucene<Document>();
						string dbKey = fields[0].getStringValue(status, att, tra, buffer); 
						// RDB$DB_KEY ����� �������� ������, ������� ���������� �������������� � Unicode
						// ������� �� ����������� ������ � ���������������� �������������
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
							if (!value.empty()) {
								// ������������ ���������� � Unicode ������ ���� ������ �� ������
								auto unicodeValue = StringUtils::toUnicode(to_utf8(value, icuCharset));
								doc->add(newLucene<Field>(fieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED));
							}
							else {
								doc->add(newLucene<Field>(fieldName, L"", Field::STORE_NO, Field::INDEX_ANALYZED));
							}
							emptyFlag = emptyFlag && value.empty();
						}
						// ���� ��� ������������� ���� �����, �� �� ����� ������ ��������� �������� � ������
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
	    }
	    catch (LuceneException& e) {
		    string error_message = StringUtils::toUTF8(e.getError());
		    throwFbException(status, error_message.c_str());
	    }
    }

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string indexName;

    FB_UDR_FETCH_PROCEDURE
    {
	    return false;
    }
FB_UDR_END_PROCEDURE


/***
CREATE PROCEDURE FTS$ADD_RECORD_TO_INDEX (
	FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$DB_KEY CHAR(8) CHARACTER SET OCTETS NOT NULL
)
EXTERNAL NAME 'luceneudr!addRecordToIndex'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(addRecordToIndex)
    FB_UDR_MESSAGE(InMessage,
	   (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
	   (FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
	   (FB_INTL_VARCHAR(8, CS_BINARY), db_key)
    );

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
		, relationHelper(context->getMaster())
	{
	}

	LuceneFTS::FTSIndexRepository indexRepository;
	LuceneFTS::RelationHelper relationHelper;

	FB_UDR_EXECUTE_PROCEDURE
	{
		if (in->index_nameNull) {
			throwFbException(status, "Index name can not be NULL");
		}
		indexName.assign(in->index_name.str, in->index_name.length);
		if (in->relation_nameNull) {
			throwFbException(status, "Relation name can not be NULL");
		}
		relationName.assign(in->relation_name.str, in->relation_name.length);
		if (in->db_keyNull) {
			throwFbException(status, "DB_KEY can not be NULL");
		}
		dbKey.assign(in->db_key.str, in->db_key.length);

		string hexDbKey = string_to_hex(dbKey);

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		// �������� ������������� �������
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message;
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		string ftsDirectory = getFtsDirectory(context);
		// �������� ���� �� ���������� ��� ��������������� �������
		auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
		if (!FileUtils::isDirectory(indexDir)) {
			string error_message;
			error_message += "Index \"" + indexName + "\" exists, but cannot be build. Please run rebuildIndex.";
			throwFbException(status, error_message.c_str());
		}

		// �������� ������������� �������
		if (!procedure->relationHelper.relationExists(status, att, tra, relationName)) {
			string error_message = "";
			error_message += "Table \"" + relationName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		try {
			auto fsIndexDir = FSDirectory::open(indexDir);
			auto analyzer = newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT);
			IndexWriterPtr writer = newLucene<IndexWriter>(fsIndexDir, analyzer, true, IndexWriter::MaxFieldLengthLIMITED);

			const char* fbCharset = context->getClientCharSet();
			string icuCharset = getICICharset(fbCharset);

			// �������� �������� ������� � ���������� �� �� ������ ������
			auto allSegments = procedure->indexRepository.getIndexSegments(status, att, tra, indexName);
			auto segmentsByRelation = LuceneFTS::FTSIndexRepository::groupIndexSegmentsByRelation(allSegments);
			auto s_it = segmentsByRelation.find(relationName);

			if (s_it == segmentsByRelation.end()) {
				string error_message;
				error_message += "Segment with table \"" + relationName + "\" not found in index \"" + indexName + "\".";
				throwFbException(status, error_message.c_str());
			}
			const auto segments = (*s_it).second;
			

			list<string> fieldNames;
			for (const auto& segment : segments) {
				if (procedure->relationHelper.fieldExists(status, att, tra, segment.relationName, segment.fieldName)) {
					// ���������� �� ������������ ����
					fieldNames.push_back(segment.fieldName);
				}
			}
			string sql = LuceneFTS::RelationHelper::buildSqlSelectFieldValues(relationName, fieldNames);
			sql = sql + "\n WHERE RDB$DB_KEY = ?";
			// todo: �� ���� ����� ��� ���������������� ����������
			AutoRelease<IStatement> stmt(att->prepare(
				status,
				tra,
				0,
				sql.c_str(),
				UDR_SQL_DIALECT,
				IStatement::PREPARE_PREFETCH_METADATA
			));

			FB_MESSAGE(Input, ThrowStatusWrapper,
				(FB_INTL_VARCHAR(8, CS_BINARY), db_key)
			) input(status, context->getMaster());

			input.clear();
			input->db_key = in->db_key;

			AutoRelease<IMessageMetadata> outputMetadata(stmt->getOutputMetadata(status));
			unsigned colCount = outputMetadata->getCount(status);
			// ������ ��� ���� ���������� ����, ����� BLOB
			AutoRelease<IMessageMetadata> newMeta(prepareTextMetaData(status, outputMetadata));
			auto fields = getFieldsInfo(status, newMeta);

			AutoRelease<IResultSet> rs(stmt->openCursor(
				status,
				tra,
				input.getMetadata(),
				input.getData(),
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
						if (!value.empty()) {
							// ������������ ���������� � Unicode ������ ���� ������ �� ������
							auto unicodeValue = StringUtils::toUnicode(to_utf8(value, icuCharset));
							doc->add(newLucene<Field>(fieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED));
						}
						else {
							doc->add(newLucene<Field>(fieldName, L"", Field::STORE_NO, Field::INDEX_ANALYZED));
						}
						emptyFlag = emptyFlag && value.empty();
					}
					// ���� ��� ������������� ���� �����, �� �� ����� ������ ��������� �������� � ������
					if (!emptyFlag) {
						writer->addDocument(doc);
					}
				}
				rs->close(status);
			}
			writer->commit();
			writer->optimize();
			writer->close();
		}
		catch (LuceneException& e) {
			string error_message = StringUtils::toUTF8(e.getError());
			throwFbException(status, error_message.c_str());
		}
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string indexName;
	string relationName;
	string dbKey;

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE

/***
CREATE PROCEDURE FTS$SEARCH (
	RDB$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT null,
	RDB$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
	rdb$filter VARCHAR(8191) CHARACTER SET UTF8
)
RETURNS (
    FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
	FTS$DB_KEY CHAR(8) CHARACTER SET OCTETS,
	FTS$SCORE DOUBLE PRECISION
)
EXTERNAL NAME 'luceneudr!ftsSearch'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsSearch)
    FB_UDR_MESSAGE(InMessage,
	    (FB_INTL_VARCHAR(252, CS_UTF8), index_name)
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_INTL_VARCHAR(32765, CS_UTF8), filter)
    );

	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relation_name)
		(FB_INTL_VARCHAR(8, CS_BINARY), db_key)
		(FB_DOUBLE, score)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
	{
	}

	LuceneFTS::FTSIndexRepository indexRepository;

	FB_UDR_EXECUTE_PROCEDURE
	{
	    if (in->index_nameNull) {
		    throwFbException(status, "Index name can not be NULL");
	    }
	    indexName.assign(in->index_name.str, in->index_name.length);
		if (!in->relation_nameNull) {
			relationName.assign(in->relation_name.str, in->relation_name.length);
		}
		if (!in->filterNull) {
			filter.assign(in->filter.str, in->filter.length);
		}

		string ftsDirectory = getFtsDirectory(context);
		// �������� ���� �� ���������� ��� �������������� ��������
		String ftsUnicodeDirectory = StringUtils::toUnicode(ftsDirectory);
		if (!FileUtils::isDirectory(ftsUnicodeDirectory)) {
			string error_message;
			error_message += "Fts directory \"" + ftsDirectory + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		// �������� ������������� �������
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message;
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		// �������� ������������� ���������� ��� �������
		auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
		if (!FileUtils::isDirectory(indexDir)) {
			string error_message;
			error_message += "Index \"" + indexName + "\" exists, but cannot be build. Please run rebuildIndex.";
			throwFbException(status, error_message.c_str());
		}

		try {
			auto fsIndexDir = FSDirectory::open(indexDir);
			IndexReaderPtr reader = IndexReader::open(fsIndexDir, true);
			searcher = newLucene<IndexSearcher>(reader);
			AnalyzerPtr analyzer = newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT);
			auto segments = procedure->indexRepository.getIndexSegments(status, att, tra, indexName);
			if (!relationName.empty()) {
				// ���� ������ ��� �������, �� �������� ������ �������� � ���� ��������
				auto segmentsByRelation = LuceneFTS::FTSIndexRepository::groupIndexSegmentsByRelation(segments);
				auto el = segmentsByRelation.find(relationName);
				if (el == segmentsByRelation.end()) {
					string error_message;
					error_message += "Relation \"" + relationName + "\" not exisit in index \"" + indexName + "\".";
					throwFbException(status, error_message.c_str());
				}
				segments = el->second;
			}
			
			auto fields = Collection<String>::newInstance();
			for (auto segment: segments) {
				fields.add(StringUtils::toUnicode(segment.getFullFieldName()));
			}

			MultiFieldQueryParserPtr parser = newLucene<MultiFieldQueryParser>(LuceneVersion::LUCENE_CURRENT, fields, analyzer);
			parser->setDefaultOperator(QueryParser::OR_OPERATOR);
			QueryPtr query = parser->parse(StringUtils::toUnicode(filter));
			TopDocsPtr docs = searcher->search(query, 10);

			scoreDocs = docs->scoreDocs;

			it = scoreDocs.begin();
			out->relation_nameNull = false;
			out->db_keyNull = false;
			out->scoreNull = false;
		}
		catch (LuceneException& e) {
			string error_message = StringUtils::toUTF8(e.getError());
			throwFbException(status, error_message.c_str());
		}
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string indexName;
	string relationName;
	string filter;
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
		// � Lucene ������� ������ �������� � 16-������ ����
		// ����������� � ������� � �������� ������
		string dbKey = hex_to_string(hexDbKey);

		out->relation_name.length = relationName.length();
		relationName.copy(out->relation_name.str, out->relation_name.length);
		
		out->db_key.length = dbKey.length();
		dbKey.copy(out->db_key.str, out->db_key.length);

		out->score = scoreDoc->score;

	    ++it;
	    return true;
	}
FB_UDR_END_PROCEDURE


FB_UDR_IMPLEMENT_ENTRY_POINT