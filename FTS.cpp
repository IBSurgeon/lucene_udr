
#include "LuceneUdr.h"
#include "FTSIndex.h"
#include "FTSLog.h"
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
	// делаем все поля строкового типа, кроме BLOB
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

	    out->directoryNull = false;
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

		// TODO: В настоящее время анализаторы не учитываются
		// когда они будут учитываться необходима проверка существования

		// проверка существования индекса
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

		// проверка существования директории для индекса
        // и если она не существует создаём её
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

		// проверка существования индекса
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message = "";
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		string ftsDirectory = getFtsDirectory(context);
		auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
		if (FileUtils::isDirectory(indexDir)) {
			// если директория есть, то удаляем её
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

		// проверка существования индекса
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message = "";
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		// проверка существования таблицы
		if (!procedure->relationHelper.relationExists(status, att, tra, relationName)) {
			string error_message = "";
			error_message += "Table \"" + relationName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		// проверка существования поля
		if (!procedure->relationHelper.fieldExists(status, att, tra, relationName, fieldName)) {
			string error_message = "";
			error_message += "Field \"" + fieldName + "\" not exitsts in table \"" + relationName + "\"";
			throwFbException(status, error_message.c_str());
		}

		// проверка существования сегмента
		if (procedure->indexRepository.hasIndexSegment(status, att, tra, indexName, relationName, fieldName)) {
			string error_message = "";
			error_message += "Segment for \"" + relationName + "\".\"" + fieldName + "\" already exitsts in index \"" + indexName + "\"";
			throwFbException(status, error_message.c_str());
		}

		// добавление сегмента
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

		// проверка существования индекса
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message = "";
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		// проверка существования таблицы
		if (!procedure->relationHelper.relationExists(status, att, tra, relationName)) {
			string error_message = "";
			error_message += "Table \"" + relationName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		// проверка существования поля
		if (!procedure->relationHelper.fieldExists(status, att, tra, relationName, fieldName)) {
			string error_message = "";
			error_message += "Field \"" + fieldName + "\" not exitsts in table \"" + relationName + "\"";
			throwFbException(status, error_message.c_str());
		}

		// проверка существования сегмента
		if (!procedure->indexRepository.hasIndexSegment(status, att, tra, indexName, relationName, fieldName)) {
			string error_message = "";
			error_message += "Segment for \"" + relationName + "\".\"" + fieldName + "\" not exists in index \"" + indexName + "\"";
			throwFbException(status, error_message.c_str());
		}

		// удаление сегмента
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
		// проверка есть ли директория для полнотекстовых индексов
	    String ftsUnicodeDirectory = StringUtils::toUnicode(ftsDirectory);
	    if (!FileUtils::isDirectory(ftsUnicodeDirectory)) {
		    string error_message = "";
		    error_message += "Fts directory \"" + ftsDirectory + "\" not exists";
		    throwFbException(status, error_message.c_str());
	    }
	
		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		// проверка существования индекса
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message = "";
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}
		// проверка существования директории для индекса
		// и если она не существует создаём её
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

			// очищаем директорию индекса
		    writer->deleteAll();
		    writer->commit();

			const char* fbCharset = context->getClientCharSet();
			string icuCharset = getICICharset(fbCharset);
			
			// получаем сегменты индекса и группируем их по именам таблиц
			auto segments = procedure->indexRepository.getIndexSegments(status, att, tra, indexName);
			auto segmentsByRelation = LuceneFTS::FTSIndexRepository::groupIndexSegmentsByRelation(segments);
			
			for (const auto& p : segmentsByRelation) {
				const string relationName = p.first;
				if (!procedure->relationHelper.relationExists(status, att, tra, relationName)) {
					// если таблицы не существует просто пропускаем этот сегмент
					continue;
				}
				const auto segments = p.second;
				list<string> fieldNames;
				for (const auto& segment : segments) {
					if (procedure->relationHelper.fieldExists(status, att, tra, segment.relationName, segment.fieldName)) {
						// игнорируем не существующие поля
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
				// делаем все поля строкового типа, кроме BLOB
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
						// RDB$DB_KEY имеет бинарный формат, который невозможно перекодировать в Unicode
						// поэтому мы перобразуем строку в шестнацетиричное представление
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
								// перекодируем содержимое в Unicode только если строка не пустая
								auto unicodeValue = StringUtils::toUnicode(to_utf8(value, icuCharset));
								doc->add(newLucene<Field>(fieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED));
							}
							else {
								doc->add(newLucene<Field>(fieldName, L"", Field::STORE_NO, Field::INDEX_ANALYZED));
							}
							emptyFlag = emptyFlag && value.empty();
						}
						// если все индексируемые поля пусты, то не имеет смысла добавлять документ в индекс
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
CREATE PROCEDURE FTS$INSERT_RECORD (
	FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$DB_KEY CHAR(8) CHARACTER SET OCTETS NOT NULL
)
EXTERNAL NAME 'luceneudr!insertRecord'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(insertRecord)
    FB_UDR_MESSAGE(InMessage,
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

		// проверка существования таблицы
		if (!procedure->relationHelper.relationExists(status, att, tra, relationName)) {
			string error_message = "";
			error_message += "Table \"" + relationName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		string ftsDirectory = getFtsDirectory(context);

		const char* fbCharset = context->getClientCharSet();
		string icuCharset = getICICharset(fbCharset);

		// получаем список сегментов всех индексов по имени таблицы
		auto allSegments = procedure->indexRepository.getIndexSegmentsByRelation(status, att, tra, relationName);
		// группируем их по именам индексов
		auto segmentsByIndex = LuceneFTS::FTSIndexRepository::groupSegmentsByIndex(allSegments);
		// перебираем все индексы
		for (const auto& sPair : segmentsByIndex) {
			const string indexName = sPair.first;
			const auto segments = sPair.second;

			// проверка есть ли директория для полнотекстового индекса
			auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
			if (!FileUtils::isDirectory(indexDir)) {
				string error_message;
				error_message += "Index \"" + indexName + "\" exists, but cannot be build. Please run rebuildIndex.";
				throwFbException(status, error_message.c_str());
			}

			// добавляем запись в каждый индекс
			try {
				auto fsIndexDir = FSDirectory::open(indexDir);
				auto analyzer = newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT);
				IndexWriterPtr writer = newLucene<IndexWriter>(fsIndexDir, analyzer, IndexWriter::MaxFieldLengthLIMITED);
				

				list<string> fieldNames;
				for (const auto& segment : segments) {
					if (procedure->relationHelper.fieldExists(status, att, tra, segment.relationName, segment.fieldName)) {
						// игнорируем не существующие поля
						fieldNames.push_back(segment.fieldName);
					}
				}
				string sql = LuceneFTS::RelationHelper::buildSqlSelectFieldValues(relationName, fieldNames);
				sql = sql + "\n WHERE RDB$DB_KEY = ?";

				// todo: по идее нужен кеш скомпилированных операторов
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
				// делаем все поля строкового типа, кроме BLOB
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
								// перекодируем содержимое в Unicode только если строка не пустая
								auto unicodeValue = StringUtils::toUnicode(to_utf8(value, icuCharset));
								doc->add(newLucene<Field>(fieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED));
							}
							else {
								doc->add(newLucene<Field>(fieldName, L"", Field::STORE_NO, Field::INDEX_ANALYZED));
							}
							emptyFlag = emptyFlag && value.empty();
						}
						// если все индексируемые поля пусты, то не имеет смысла добавлять документ в индекс
						if (!emptyFlag) {
							writer->addDocument(doc);							
						}
					}
					rs->close(status);
				}
				writer->commit();
				writer->close();
			}
			catch (LuceneException& e) {
				string error_message = StringUtils::toUTF8(e.getError());
				throwFbException(status, error_message.c_str());
			}
		}
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string relationName;
	string dbKey;

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE

/***
CREATE PROCEDURE FTS$UPDATE_RECORD (
	FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$DB_KEY CHAR(8) CHARACTER SET OCTETS NOT NULL
)
EXTERNAL NAME 'luceneudr!updateRecord'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(updateRecord)
FB_UDR_MESSAGE(InMessage,
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

		// проверка существования таблицы
		if (!procedure->relationHelper.relationExists(status, att, tra, relationName)) {
			string error_message = "";
			error_message += "Table \"" + relationName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		string ftsDirectory = getFtsDirectory(context);

		const char* fbCharset = context->getClientCharSet();
		string icuCharset = getICICharset(fbCharset);

		// получаем список сегментов всех индексов по имени таблицы
		auto allSegments = procedure->indexRepository.getIndexSegmentsByRelation(status, att, tra, relationName);
		// группируем их по именам индексов
		auto segmentsByIndex = LuceneFTS::FTSIndexRepository::groupSegmentsByIndex(allSegments);
		// перебираем все индексы
		for (const auto& sPair : segmentsByIndex) {
			const string indexName = sPair.first;
			const auto segments = sPair.second;

			// проверка есть ли директория для полнотекстового индекса
			auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
			if (!FileUtils::isDirectory(indexDir)) {
				string error_message;
				error_message += "Index \"" + indexName + "\" exists, but cannot be build. Please run rebuildIndex.";
				throwFbException(status, error_message.c_str());
			}

			// добавляем запись в каждый индекс
			try {
				auto fsIndexDir = FSDirectory::open(indexDir);
				auto analyzer = newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT);
				IndexWriterPtr writer = newLucene<IndexWriter>(fsIndexDir, analyzer, IndexWriter::MaxFieldLengthLIMITED);


				list<string> fieldNames;
				for (const auto& segment : segments) {
					if (procedure->relationHelper.fieldExists(status, att, tra, segment.relationName, segment.fieldName)) {
						// игнорируем не существующие поля
						fieldNames.push_back(segment.fieldName);
					}
				}
				string sql = LuceneFTS::RelationHelper::buildSqlSelectFieldValues(relationName, fieldNames);
				sql = sql + "\n WHERE RDB$DB_KEY = ?";

				// todo: по идее нужен кеш скомпилированных операторов
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
				// делаем все поля строкового типа, кроме BLOB
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
								// перекодируем содержимое в Unicode только если строка не пустая
								auto unicodeValue = StringUtils::toUnicode(to_utf8(value, icuCharset));
								doc->add(newLucene<Field>(fieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED));
							}
							else {
								doc->add(newLucene<Field>(fieldName, L"", Field::STORE_NO, Field::INDEX_ANALYZED));
							}
							emptyFlag = emptyFlag && value.empty();
						}
						TermPtr term = newLucene<Term>(L"RDB$DB_KEY", StringUtils::toUnicode(hexDbKey));
						if (!emptyFlag) {
							writer->updateDocument(term, doc);
						}
						else {
							writer->deleteDocuments(term);
						}
					}
					rs->close(status);
				}
				writer->commit();
				writer->close();
			}
			catch (LuceneException& e) {
				string error_message = StringUtils::toUTF8(e.getError());
				throwFbException(status, error_message.c_str());
			}
		}
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string relationName;
	string dbKey;

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}
FB_UDR_END_PROCEDURE


/***
CREATE PROCEDURE FTS$DELETE_RECORD (
	FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$DB_KEY CHAR(8) CHARACTER SET OCTETS NOT NULL
)
EXTERNAL NAME 'luceneudr!deleteRecord'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(deleteRecord)
    FB_UDR_MESSAGE(InMessage,
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

		// проверка существования таблицы
		if (!procedure->relationHelper.relationExists(status, att, tra, relationName)) {
			string error_message = "";
			error_message += "Table \"" + relationName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		string ftsDirectory = getFtsDirectory(context);

		const char* fbCharset = context->getClientCharSet();
		string icuCharset = getICICharset(fbCharset);

		// получаем список сегментов всех индексов по имени таблицы
		auto allSegments = procedure->indexRepository.getIndexSegmentsByRelation(status, att, tra, relationName);
		// группируем их по именам индексов
		auto segmentsByIndex = LuceneFTS::FTSIndexRepository::groupSegmentsByIndex(allSegments);
		// перебираем все индексы
		for (const auto& sPair : segmentsByIndex) {
			const string indexName = sPair.first;
			const auto segments = sPair.second;

			// проверка есть ли директория для полнотекстового индекса
			auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
			if (!FileUtils::isDirectory(indexDir)) {
				string error_message;
				error_message += "Index \"" + indexName + "\" exists, but cannot be build. Please run rebuildIndex.";
				throwFbException(status, error_message.c_str());
			}

			// добавляем запись в каждый индекс
			try {
				auto fsIndexDir = FSDirectory::open(indexDir);
				IndexReaderPtr reader = IndexReader::open(fsIndexDir, false);
				TermPtr term = newLucene<Term>(L"RDB$DB_KEY", StringUtils::toUnicode(hexDbKey));
				int32_t deleted = reader->deleteDocuments(term);

				reader->close();
				fsIndexDir->close();
			}
			catch (LuceneException& e) {
				string error_message = StringUtils::toUTF8(e.getError());
				throwFbException(status, error_message.c_str());
			}
		}
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string relationName;
	string dbKey;

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE

/***
CREATE PROCEDURE FTS$LOG_CHANGE (
    RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	DB_KEY         CHAR(8) CHARACTER SET OCTETS NOT NULL,
	CHANGE_TYPE    FTS$CHANGE_TYPE NOT NULL
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
			throwFbException(status, "Relation name can not be NULL");
		}
		relationName.assign(in->relation_name.str, in->relation_name.length);
		if (in->db_keyNull) {
			throwFbException(status, "DB_KEY can not be NULL");
		}
		dbKey.assign(in->db_key.str, in->db_key.length);
		if (in->change_typeNull) {
			throwFbException(status, "CHANGE_TYPE can not be NULL");
		}
		changeType.assign(in->change_type.str, in->change_type.length);


		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		procedure->logRepository.appendLog(status, att, tra, relationName, dbKey, changeType);
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;
	string relationName;
	string dbKey;
	string changeType;

    FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE

/***
CREATE PROCEDURE FTS$UPDATE_INDEXES 
EXTERNAL NAME 'luceneudr!updateFtsIndexes'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(updateFtsIndexes)

	FB_UDR_CONSTRUCTOR
		, indexRepository(context->getMaster())
		, relationHelper(context->getMaster())
		, logRepository(context->getMaster())
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
	map<string, IStatement*> prepareStmtMap;

	void clearPreparedStatements() {
		for (auto& pStmt : prepareStmtMap) {
			pStmt.second->release();
		}
		prepareStmtMap.clear();
	}

	FB_UDR_EXECUTE_PROCEDURE
	{
		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		string ftsDirectory = getFtsDirectory(context);

		const char* fbCharset = context->getClientCharSet();
		string icuCharset = getICICharset(fbCharset);

		map<string, LuceneFTS::FTSRelation> relationsByName;
		procedure->clearPreparedStatements();
		
		// получаем все индексы
		auto allIndexes = procedure->indexRepository.getAllIndexes(status, att, tra);		
		for (auto& ftsIndex : allIndexes) {
		    // получаем сегменты индекса
			auto segments = procedure->indexRepository.getIndexSegments(status, att, tra, ftsIndex.indexName);
			for (auto& ftsSegment : segments) {
			    // ищем таблицу по имени
				auto r = relationsByName.find(ftsSegment.relationName);
				
				if (r != relationsByName.end()) {
					auto ftsRelation = r->second;
					ftsRelation.addIndex(ftsIndex);
					ftsRelation.addSegment(ftsSegment);
					relationsByName.insert_or_assign(ftsSegment.relationName, ftsRelation);
				}
				else {
					// такой таблицы ещё не было
					LuceneFTS::FTSRelation ftsRelation(ftsSegment.relationName);
					ftsRelation.addIndex(ftsIndex);
					ftsRelation.addSegment(ftsSegment);
					relationsByName.insert_or_assign(ftsSegment.relationName, ftsRelation);
				}
			}
		}
		
		// теперь необходимо для каждой таблицы по каждому индексу построить запросы для ивлечения записей
		for (auto& p : relationsByName) {
			string relationName = p.first;
			auto ftsRelation = p.second;
			auto ftsIndexes = ftsRelation.getIndexes();
			for (auto& pIndex : ftsIndexes) {
				auto ftsIndex = pIndex.second;
				auto segments = ftsRelation.getSegmentsByIndexName(ftsIndex.indexName);
				list<string> fieldNames;
				for (const auto& segment : segments) {
					if (procedure->relationHelper.fieldExists(status, att, tra, segment.relationName, segment.fieldName)) {
						// игнорируем не существующие поля
						fieldNames.push_back(segment.fieldName);
					}
				}
				string sql = LuceneFTS::RelationHelper::buildSqlSelectFieldValues(relationName, fieldNames, true);
				ftsRelation.setSql(ftsIndex.indexName, sql);
			}
			relationsByName.insert_or_assign(relationName, ftsRelation);
		}
		
		FB_MESSAGE(ValInput, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(8, CS_BINARY), db_key)
		) selValInput(status, context->getMaster());
		

		map<string, IndexWriterPtr> indexWriters;

		
		// получаем лог изменений записей для индекса
		AutoRelease<IStatement> logStmt(att->prepare(
			status,
			tra,
			0,
			"SELECT ID, DB_KEY, RELATION_NAME, CHANGE_TYPE\n"
			"FROM FTS$LOG\n"
			"ORDER BY ID",
			UDR_SQL_DIALECT,
			IStatement::PREPARE_PREFETCH_METADATA
		));

		FB_MESSAGE(LogOutput, ThrowStatusWrapper,
			(FB_BIGINT, id)
			(FB_VARCHAR(8), dbKey)
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
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

			// ищем таблицу в списке индексированных таблиц
			auto r = relationsByName.find(relationName);
			if (r != relationsByName.end()) {
				// для каждой таблицы получаем список индексов
				LuceneFTS::FTSRelation ftsRelation = r->second;
				auto ftsIndexes = ftsRelation.getIndexes();
				// по каждому индексу ищем подготовленный запрос
				for (auto& pIndex : ftsIndexes) {
					string indexName = pIndex.first;
					auto ftsIndex = pIndex.second;
					// ищем IndexWriter
					auto iWriter = indexWriters.find(ftsIndex.indexName);
					// если не найден, то открываем такой
					if (iWriter == indexWriters.end()) {
						// проверка существования директории для индекса
						// и если она не существует создаём её
						auto indexDir = StringUtils::toUnicode(ftsDirectory + "/" + indexName);
						if (!FileUtils::isDirectory(indexDir)) {
							bool result = FileUtils::createDirectory(indexDir);
							if (!result) {
								string error_message = "";
								error_message += "Cannot create index directory " + ftsDirectory + "/" + indexName;
								throwFbException(status, error_message.c_str());
							}
						}
						auto fsIndexDir = FSDirectory::open(indexDir);
						// TODO: пока анализатор всегда стандартный
						auto analyzer = newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT);
						IndexWriterPtr writer = newLucene<IndexWriter>(fsIndexDir, analyzer, IndexWriter::MaxFieldLengthLIMITED);
						indexWriters[indexName] = writer;
					}
					IndexWriterPtr writer = indexWriters[indexName];
					if ((changeType == "I") || (changeType == "U")) {
						string stmtName = relationName + "." + indexName;
						// если его нет, то берём текст запроса и подготовливаем его
						auto iStmt = procedure->prepareStmtMap.find(stmtName);
						if (iStmt == procedure->prepareStmtMap.end()) {
							IStatement* stmt = att->prepare(
								status,
								tra,
								0,
								ftsRelation.getSql(indexName).c_str(),
								UDR_SQL_DIALECT,
								IStatement::PREPARE_PREFETCH_METADATA
							);
							procedure->prepareStmtMap[stmtName] = stmt;
						}
						auto stmt = procedure->prepareStmtMap[stmtName];
						// получаем нужные значения полей
						AutoRelease<IMessageMetadata> outputMetadata(stmt->getOutputMetadata(status));
						unsigned colCount = outputMetadata->getCount(status);
						// делаем все поля строкового типа, кроме BLOB
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
									if (!value.empty()) {
										// перекодируем содержимое в Unicode только если строка не пустая
										auto unicodeValue = StringUtils::toUnicode(to_utf8(value, icuCharset));
										doc->add(newLucene<Field>(fieldName, unicodeValue, Field::STORE_NO, Field::INDEX_ANALYZED));
									}
									else {
										doc->add(newLucene<Field>(fieldName, L"", Field::STORE_NO, Field::INDEX_ANALYZED));
									}
									emptyFlag = emptyFlag && value.empty();
								}
								if (changeType == "I") {
									if (!emptyFlag) {
										writer->addDocument(doc);
									}
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
			procedure->logRepository.deleteLog(status, att, tra, logId);
		}
		logRs->close(status);
		// подтверждаем измения для всех индексов
		for (auto& pIndexWriter : indexWriters) {
			auto indexWriter = pIndexWriter.second;
			indexWriter->commit();
			indexWriter->close();
		}
		// очищаем подготовленные запросы
		procedure->clearPreparedStatements();
	}

	AutoRelease<IAttachment> att;
	AutoRelease<ITransaction> tra;

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE


/***
CREATE PROCEDURE FTS$SEARCH (
	RDB$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT null,
	RDB$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
	RDB$FILTER VARCHAR(8191) CHARACTER SET UTF8,
	FTS$LIMIT BIGINT NOT NULL DEFAULT 1000
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
		(FB_BIGINT, limit)
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
		limit = in->limit;

		string ftsDirectory = getFtsDirectory(context);
		// проверка есть ли директория для полнотекстовых индексов
		String ftsUnicodeDirectory = StringUtils::toUnicode(ftsDirectory);
		if (!FileUtils::isDirectory(ftsUnicodeDirectory)) {
			string error_message;
			error_message += "Fts directory \"" + ftsDirectory + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		// проверка существования индекса
		if (!procedure->indexRepository.hasIndex(status, att, tra, indexName)) {
			string error_message;
			error_message += "Index \"" + indexName + "\" not exists";
			throwFbException(status, error_message.c_str());
		}

		// проверка существования директории для индекса
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
				// если задано имя таблицы, то выбираем только сегменты с этой таблицей
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
			TopDocsPtr docs = searcher->search(query, limit);

			scoreDocs = docs->scoreDocs;

			it = scoreDocs.begin();
			
			out->relation_nameNull = true;
			out->db_keyNull = true;
			out->scoreNull = true;
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
	ISC_INT64 limit;
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
		// в Lucene индексе строка хранится в 16-ричном виде
		// преобразуем её обратно в бинарный формат
		string dbKey = hex_to_string(hexDbKey);
		
        out->relation_nameNull = false;
		out->relation_name.length = relationName.length();
		relationName.copy(out->relation_name.str, out->relation_name.length);
		
		out->db_keyNull = false;
		out->db_key.length = dbKey.length();
		dbKey.copy(out->db_key.str, out->db_key.length);

        out->scoreNull = false;
		out->score = scoreDoc->score;

	    ++it;
	    return true;
	}
FB_UDR_END_PROCEDURE


FB_UDR_IMPLEMENT_ENTRY_POINT