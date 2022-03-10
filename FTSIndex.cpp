
#include "FTSIndex.h"
#include "inicpp.h"
#include "FBBlobUtils.h"
#include "StringFormatter.h"
#include "LuceneAnalyzerFactory.h"

using namespace Firebird;
using namespace std;
using namespace LuceneFTS;

string LuceneFTS::getFtsDirectory(IExternalContext* context) {
	IConfigManager* configManager = context->getMaster()->getConfigManager();
	const string databaseName(context->getDatabaseName());

	string rootDir(configManager->getRootDirectory());

	ini::IniFile iniFile;
	iniFile.load(rootDir + "/fts.ini");
	auto section = iniFile[databaseName];
	return section["ftsDirectory"].as<string>();
}

//
// Создание нового полнотекстового индекса
//
void FTSIndexRepository::createIndex(
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect,
	string indexName,
	string analyzer,
	string description)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
		(FB_BLOB, description)
	) input(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	if (analyzer.empty()) {
		analyzer = LuceneFTS::DEFAULT_ANALYZER_NAME;
	}
	else {
		std::transform(analyzer.begin(), analyzer.end(), analyzer.begin(), ::toupper);
	}

	input->analyzer.length = analyzer.length();
	analyzer.copy(input->analyzer.str, input->analyzer.length);

	if (!description.empty()) {

		AutoRelease<IBlob> blob(att->createBlob(status, tra, &input->description, 0, nullptr));
		blob_set_string(status, blob, description);
		blob->close(status);
	}
	else {
		input->descriptionNull = true;
	}

	// проверка существования индекса
	if (hasIndex(status, att, tra, sqlDialect, indexName)) {
		string error_message = string_format("Index \"%s\" already exists", indexName);
		ISC_STATUS statusVector[] = {
	       isc_arg_gds, isc_random,
	       isc_arg_string, (ISC_STATUS)error_message.c_str(),
	       isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	// проверяем существование анализатора
	LuceneFTS::LuceneAnalyzerFactory analyzerFactory;
	if (!analyzerFactory.hasAnalyzer(analyzer)) {
		string error_message = string_format("Analyzer \"%s\" not exists", analyzer);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	att->execute(
		status,
		tra,
		0,
		"INSERT INTO FTS$INDICES(FTS$INDEX_NAME, FTS$ANALYZER, FTS$DESCRIPTION)\n"
		"VALUES(?, ?, ?)",
		sqlDialect,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
}

//
// Удаление полнотекстового индекса
//
void FTSIndexRepository::dropIndex(
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect,
	string indexName)
{

	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
	) input(status, m_master);

	input.clear();
	
	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	// проверка существования индекса
	if (hasIndex(status, att, tra, sqlDialect, indexName)) {
		string error_message = string_format("Index \"%s\" not exists", indexName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	att->execute(
		status,
		tra,
		0,
		"DELETE FROM FTS$INDICES WHERE FTS$INDEX_NAME = ?",
		sqlDialect,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
}

//
// Возвращает существует ли индекс с заданным именем.
//
bool FTSIndexRepository::hasIndex(
	ThrowStatusWrapper* status, 
	IAttachment* att, 
	ITransaction* tra, 
	unsigned int sqlDialect, 
	string indexName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
	) input(status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTEGER, cnt)
	) output(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	if (!stmt_exists_index.hasData()) {
		stmt_exists_index.reset(att->prepare(
			status,
			tra,
			0,
			"SELECT COUNT(*) AS CNT\n"
			"FROM FTS$INDICES\n"
			"WHERE FTS$INDEX_NAME = ?",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	}
	AutoRelease<IResultSet> rs(stmt_exists_index->openCursor(
		status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	bool foundFlag = false;
	if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
		foundFlag = (output->cnt > 0);
	}
	rs->close(status);

	return foundFlag;
}



//
// Получает информацию об индексе с заданным именем, если он существует.
// Бросает сиключение в случае его отсуствия.
//
FTSIndex FTSIndexRepository::getIndex(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned int sqlDialect, string indexName)
{
	FTSIndex ftsIndex;

	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
	) input(status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
		(FB_BLOB, description)
	) output(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	if (!stmt_get_index.hasData()) {
		stmt_get_index.reset(att->prepare(
			status,
			tra,
			0,
			"SELECT FTS$INDEX_NAME, FTS$ANALYZER, FTS$DESCRIPTION\n"
			"FROM FTS$INDICES\n"
			"WHERE FTS$INDEX_NAME = ?",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	}
	AutoRelease<IResultSet> rs(stmt_get_index->openCursor(
		status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	bool foundFlag = false;
	if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
		foundFlag = true;
		ftsIndex.indexName.assign(output->indexName.str, output->indexName.length);
		if (!output->analyzerNull && output->analyzer.length > 0) {
			ftsIndex.analyzer.assign(output->analyzer.str, output->analyzer.length);
		}
		else {
			ftsIndex.analyzer = LuceneFTS::DEFAULT_ANALYZER_NAME;
		}
		if (!output->descriptionNull) {
			AutoRelease<IBlob> blob(att->openBlob(status, tra, &output->description, 0, nullptr));
			ftsIndex.description = blob_get_string(status, blob);
			blob->close(status);
		}
	}
	rs->close(status);
	if (!foundFlag) {
		string error_message = string_format("Index \"%s\" not exists", indexName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	return ftsIndex;
}

//
// Возвращет список всех индексов
//
list<FTSIndex> FTSIndexRepository::getAllIndexes(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned int sqlDialect)
{

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
		(FB_BLOB, description)
	) output(status, m_master);


	AutoRelease<IStatement> stmt(att->prepare(
			status,
			tra,
			0,
			"SELECT FTS$INDEX_NAME, FTS$ANALYZER, FTS$DESCRIPTION\n"
			"FROM FTS$INDICES\n"
			"ORDER BY FTS$INDEX_NAME",
		    sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	
	AutoRelease<IResultSet> rs(stmt->openCursor(
		status,
		tra,
		nullptr,
		nullptr,
		output.getMetadata(),
		0
	));

	list<FTSIndex> indexes;
	while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
		FTSIndex ftsIndex;
		ftsIndex.indexName.assign(output->indexName.str, output->indexName.length);
		if (!output->analyzerNull && output->analyzer.length > 0) {
			ftsIndex.analyzer.assign(output->analyzer.str, output->analyzer.length);
		}
		else {
			ftsIndex.analyzer = LuceneFTS::DEFAULT_ANALYZER_NAME;
		}
		if (!output->descriptionNull) {
			AutoRelease<IBlob> blob(att->openBlob(status, tra, &output->description, 0, nullptr));
			ftsIndex.description = blob_get_string(status, blob);
			blob->close(status);
		}
		indexes.push_back(ftsIndex);
	}
	rs->close(status);

	return indexes;
}

//
// Возвращает сегменты индекса с заданным именем
//
list<FTSIndexSegment> FTSIndexRepository::getIndexSegments(ThrowStatusWrapper* status, IAttachment* att, ITransaction* tra, unsigned int sqlDialect, string indexName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
	) input(status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
	) output(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	if (!stmt_index_segments.hasData()) {
		stmt_index_segments.reset(att->prepare(
			status,
			tra,
			0,
			"SELECT FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$FIELD_NAME\n"
			"FROM FTS$INDEX_SEGMENTS\n"
			"WHERE FTS$INDEX_NAME = ?",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	}
	AutoRelease<IResultSet> rs(stmt_index_segments->openCursor(
		status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	list<FTSIndexSegment> segments;
	while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
		FTSIndexSegment indexSegment;
		indexSegment.indexName.assign(output->indexName.str, output->indexName.length);
		indexSegment.relationName.assign(output->relationName.str, output->relationName.length);
		indexSegment.fieldName.assign(output->fieldName.str, output->fieldName.length);
		segments.push_back(indexSegment);
	}
	rs->close(status);
	return segments;
}

//
// Возвращает все сегменты всех индексов. Упорядочено по имеи индекса
//
list<FTSIndexSegment> FTSIndexRepository::getAllIndexSegments(
	ThrowStatusWrapper* status, 
	IAttachment* att, 
	ITransaction* tra,
	unsigned int sqlDialect)
{

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
	) output(status, m_master);


	AutoRelease<IStatement> stmt(att->prepare(
			status,
			tra,
			0,
			"SELECT\n"
			"  FTS$INDEX_SEGMENTS.FTS$INDEX_NAME,\n"
			"  FTS$INDEX_SEGMENTS.FTS$RELATION_NAME,\n"
			"  FTS$INDEX_SEGMENTS.FTS$FIELD_NAME,\n"
			"  FTS$INDICES.FTS$ANALYZER\n"
			"FROM FTS$INDEX_SEGMENTS\n"
			"JOIN FTS$INDICES ON FTS$INDEX_SEGMENTS.FTS$INDEX_NAME = FTS$INDICES.FTS$INDEX_NAME\n"
			"ORDER BY FTS$INDEX_SEGMENTS.FTS$INDEX_NAME",
		    sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	
	AutoRelease<IResultSet> rs(stmt->openCursor(
		status,
		tra,
		nullptr,
		nullptr,
		output.getMetadata(),
		0
	));
	list<FTSIndexSegment> segments;
	while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
		FTSIndexSegment indexSegment;
		indexSegment.indexName.assign(output->indexName.str, output->indexName.length);
		indexSegment.relationName.assign(output->relationName.str, output->relationName.length);
		indexSegment.fieldName.assign(output->fieldName.str, output->fieldName.length);
		indexSegment.index.indexName.assign(output->indexName.str, output->indexName.length);
		indexSegment.index.analyzer.assign(output->analyzerName.str, output->analyzerName.length);
		if (!output->analyzerNameNull && output->analyzerName.length > 0) {
			indexSegment.index.analyzer.assign(output->analyzerName.str, output->analyzerName.length);
		}
		else {
			indexSegment.index.analyzer = LuceneFTS::DEFAULT_ANALYZER_NAME;
		}
		// замечание: описание индекса не требуется копировать
		segments.push_back(indexSegment);
	}
	rs->close(status);
	return segments;
}

//
// Возвращает сегменты индексов по имени таблицы
//
list<FTSIndexSegment> FTSIndexRepository::getIndexSegmentsByRelation(
	ThrowStatusWrapper* status, 
	IAttachment* att, 
	ITransaction* tra, 
	unsigned int sqlDialect,
	string relationName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
	) input(status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
	) output(status, m_master);

	input.clear();

	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);

	if (!stmt_rel_segments.hasData()) {
		stmt_rel_segments.reset(att->prepare(
			status,
			tra,
			0,
			"SELECT\n" 
			"  FTS$INDEX_SEGMENTS.FTS$INDEX_NAME,\n" 
			"  FTS$INDEX_SEGMENTS.FTS$RELATION_NAME,\n" 
			"  FTS$INDEX_SEGMENTS.FTS$FIELD_NAME,\n"
			"  FTS$INDICES.FTS$ANALYZER\n"
			"FROM FTS$INDEX_SEGMENTS\n"
			"JOIN FTS$INDICES ON FTS$INDEX_SEGMENTS.FTS$INDEX_NAME = FTS$INDICES.FTS$INDEX_NAME\n"
			"WHERE FTS$INDEX_SEGMENTS.FTS$RELATION_NAME = ?\n"
			"ORDER BY FTS$INDEX_SEGMENTS.FTS$INDEX_NAME",
			sqlDialect,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	}
	AutoRelease<IResultSet> rs(stmt_rel_segments->openCursor(
		status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	list<FTSIndexSegment> segments;
	while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
		FTSIndexSegment indexSegment;
		indexSegment.indexName.assign(output->indexName.str, output->indexName.length);
		indexSegment.relationName.assign(output->relationName.str, output->relationName.length);
		indexSegment.fieldName.assign(output->fieldName.str, output->fieldName.length);
		indexSegment.index.indexName.assign(output->indexName.str, output->indexName.length);
		if (!output->analyzerNameNull && output->analyzerName.length > 0) {
			indexSegment.index.analyzer.assign(output->analyzerName.str, output->analyzerName.length);
		}
		else {
			indexSegment.index.analyzer = LuceneFTS::DEFAULT_ANALYZER_NAME;
		}
		// замечание: описание индекса не требуется копировать
		segments.push_back(indexSegment);
	}
	rs->close(status);
	return segments;
}

//
// Добавление нового поля (сегмента) полнотекстового индекса
//
void FTSIndexRepository::addIndexField(
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect,
	string indexName,
	string relationName,
	string fieldName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
	) input(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);

	input->fieldName.length = fieldName.length();
	fieldName.copy(input->fieldName.str, input->fieldName.length);

	// проверка существования индекса
	if (!hasIndex(status, att, tra, sqlDialect, indexName)) {
		string error_message = string_format("Index \"%s\" not exists", indexName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	// проверка существования сегмента
	if (hasIndexSegment(status, att, tra, sqlDialect, indexName, relationName, fieldName)) {
		string error_message = string_format("Segment for \"%s\".\"%s\" already exists in index \"%s\"", relationName, fieldName, indexName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	// проверка существования таблицы
	if (!relationHelper.relationExists(status, att, tra, sqlDialect, relationName)) {
		string error_message = string_format("Table \"%s\" not exists.", relationName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	// проверка существования поля
	if (!relationHelper.fieldExists(status, att, tra, sqlDialect, relationName, fieldName)) {
		string error_message = string_format("Field \"%s\" not exists in table \"%s\".", fieldName, relationName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	att->execute(
		status,
		tra,
		0,
		"INSERT INTO FTS$INDEX_SEGMENTS(FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$FIELD_NAME)\n"
		"VALUES(?, ?, ?)",
		sqlDialect,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
}

//
// Удаление поля (сегмента) из полнотекстового индекса
//
void FTSIndexRepository::dropIndexField(
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect,
	string indexName,
	string relationName,
	string fieldName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
	) input(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);

	input->fieldName.length = fieldName.length();
	fieldName.copy(input->fieldName.str, input->fieldName.length);

	// проверка существования индекса
	if (!hasIndex(status, att, tra, sqlDialect, indexName)) {
		string error_message = string_format("Index \"%s\" not exists", indexName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	// проверка существования сегмента
	if (!hasIndexSegment(status, att, tra, sqlDialect, indexName, relationName, fieldName)) {
		string error_message = string_format("Segment for \"%s\".\"%s\" not exists in index \"%s\"", relationName, fieldName, indexName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	att->execute(
		status,
		tra,
		0,
		"DELETE FROM FTS$INDEX_SEGMENTS\n"
		"WHERE FTS$INDEX_NAME = ? AND FTS$RELATION_NAME = ? AND FTS$FIELD_NAME = ?",
		sqlDialect,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
}

//
// Проверка существования поля (сегмента) в полнотекстовом индексе
//
bool FTSIndexRepository::hasIndexSegment(
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect,
	string indexName,
	string relationName,
	string fieldName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
	) input(status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTEGER, cnt)
	) output(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);

	input->fieldName.length = fieldName.length();
	fieldName.copy(input->fieldName.str, input->fieldName.length);

	AutoRelease<IStatement> stmt(att->prepare(
		status,
		tra,
		0,
		"SELECT COUNT(*) AS CNT\n"
		"FROM FTS$INDEX_SEGMENTS\n"
		"WHERE FTS$INDEX_NAME = ? AND FTS$RELATION_NAME = ? AND FTS$FIELD_NAME = ?",
		sqlDialect,
		IStatement::PREPARE_PREFETCH_METADATA
	));

	AutoRelease<IResultSet> rs(stmt->openCursor(
		status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	bool foundFlag = false;
	if (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
		foundFlag = (output->cnt > 0);
	}
	rs->close(status);

	return foundFlag;
}
