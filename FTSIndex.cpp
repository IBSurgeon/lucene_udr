
#include "FTSIndex.h"
#include "inicpp.h"
#include "FBBlobUtils.h"
#include "LuceneAnalyzerFactory.h"

using namespace Firebird;
using namespace std;
using namespace LuceneFTS;

/// <summary>
/// Returns the directory where full-text indexes are located.
/// </summary>
/// 
/// <param name="context">The context of the external routine.</param>
/// 
/// <returns>Full path to full-text index directory</returns>
string LuceneFTS::getFtsDirectory(IExternalContext* context) {
	IConfigManager* configManager = context->getMaster()->getConfigManager();
	const string databaseName(context->getDatabaseName());

	string rootDir(configManager->getRootDirectory());

	ini::IniFile iniFile;
	iniFile.load(rootDir + "/fts.ini");
	auto section = iniFile[databaseName];
	return section["ftsDirectory"].as<string>();
}

/// <summary>
/// Create a new full-text index. 
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="indexName">Index name</param>
/// <param name="analyzer">Analyzer name</param>
/// <param name="description">Custom index description</param>
void FTSIndexRepository::createIndex (
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
		(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
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

	string indexStatus = "N";
	input->indexStatus.length = indexStatus.length();
	indexStatus.copy(input->indexStatus.str, input->indexStatus.length);

	// check for index existence
	if (hasIndex(status, att, tra, sqlDialect, indexName)) {
		string error_message = string_format("Index \"%s\" already exists", indexName);
		ISC_STATUS statusVector[] = {
	       isc_arg_gds, isc_random,
	       isc_arg_string, (ISC_STATUS)error_message.c_str(),
	       isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	// checking the existence of the analyzer
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
		"INSERT INTO FTS$INDICES(FTS$INDEX_NAME, FTS$ANALYZER, FTS$DESCRIPTION, FTS$INDEX_STATUS)\n"
		"VALUES(?, ?, ?, ?)",
		sqlDialect,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
}

/// <summary>
/// Remove a full-text index. 
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="indexName">Index name</param>
void FTSIndexRepository::dropIndex (
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

	// check for index existence
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

/// <summary>
/// Set the index status.
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="indexName">Index name</param>
/// <param name="indexStatus">Index Status</param>
void FTSIndexRepository::setIndexStatus (
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect,
	string indexName,
	string indexStatus)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
	) input(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	input->indexStatus.length = indexStatus.length();
	indexStatus.copy(input->indexStatus.str, input->indexStatus.length);

	att->execute(
		status,
		tra,
		0,
		"UPDATE FTS$INDICES SET FTS$INDEX_STATUS = ? WHERE FTS$INDEX_NAME = ?",
		sqlDialect,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
}

/// <summary>
/// Checks if an index with the given name exists.
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="indexName">Index name</param>
/// 
/// <returns>Returns true if the index exists, false otherwise</returns>
bool FTSIndexRepository::hasIndex (
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



/// <summary>
/// Returns index metadata by index name.
/// 
/// Throws an exception if the index does not exist. 
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="indexName">Index name</param>
/// 
/// <returns>Index metadata</returns>
FTSIndex FTSIndexRepository::getIndex (
	ThrowStatusWrapper* status, 
	IAttachment* att, 
	ITransaction* tra, 
	unsigned int sqlDialect, 
	string indexName)
{
	FTSIndex ftsIndex;

	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
	) input(status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
		(FB_BLOB, description)
		(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
	) output(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	if (!stmt_get_index.hasData()) {
		stmt_get_index.reset(att->prepare(
			status,
			tra,
			0,
			"SELECT FTS$INDEX_NAME, FTS$ANALYZER, FTS$DESCRIPTION, FTS$INDEX_STATUS\n"
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
		ftsIndex.analyzer.assign(output->analyzer.str, output->analyzer.length);
		if (!output->descriptionNull) {
			AutoRelease<IBlob> blob(att->openBlob(status, tra, &output->description, 0, nullptr));
			ftsIndex.description = blob_get_string(status, blob);
			blob->close(status);
		}
		ftsIndex.status.assign(output->indexStatus.str, output->indexStatus.length);
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

/// <summary>
/// Returns a list of indexes. 
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// 
/// <returns>List of indexes</returns>
list<FTSIndex> FTSIndexRepository::getAllIndexes (
	ThrowStatusWrapper* status, 
	IAttachment* att, 
	ITransaction* tra, 
	unsigned int sqlDialect)
{

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
		(FB_BLOB, description)
		(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
	) output(status, m_master);


	AutoRelease<IStatement> stmt(att->prepare(
			status,
			tra,
			0,
			"SELECT FTS$INDEX_NAME, FTS$ANALYZER, FTS$DESCRIPTION, FTS$INDEX_STATUS\n"
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
		ftsIndex.analyzer.assign(output->analyzer.str, output->analyzer.length);

		if (!output->descriptionNull) {
			AutoRelease<IBlob> blob(att->openBlob(status, tra, &output->description, 0, nullptr));
			ftsIndex.description = blob_get_string(status, blob);
			blob->close(status);
		}

		ftsIndex.status.assign(output->indexStatus.str, output->indexStatus.length);

		indexes.push_back(ftsIndex);
	}
	rs->close(status);

	return indexes;
}

/// <summary>
/// Returns a list of index segments with the given name.
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="indexName">Index name</param>
/// 
/// <returns>List of index segments</returns>
list<FTSIndexSegment> FTSIndexRepository::getIndexSegments (
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
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		(FB_DOUBLE, boost)
	) output(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	if (!stmt_index_segments.hasData()) {
		stmt_index_segments.reset(att->prepare(
			status,
			tra,
			0,
			"SELECT FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$FIELD_NAME, FTS$BOOST\n"
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
		if (output->boostNull) {
			indexSegment.boost = 1.0;
		}
		else {
			indexSegment.boost = output->boost;
		}
		segments.push_back(indexSegment);
	}
	rs->close(status);
	return segments;
}

/// <summary>
/// Returns all segments of all indexes, ordered by index name. 
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// 
/// <returns>List of index segments</returns>
list<FTSIndexSegment> FTSIndexRepository::getAllIndexSegments (
	ThrowStatusWrapper* status, 
	IAttachment* att, 
	ITransaction* tra,
	unsigned int sqlDialect)
{

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		(FB_DOUBLE, boost)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
		(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
	) output(status, m_master);


	AutoRelease<IStatement> stmt(att->prepare(
			status,
			tra,
			0,
			"SELECT\n"
			"  FTS$INDEX_SEGMENTS.FTS$INDEX_NAME,\n"
			"  FTS$INDEX_SEGMENTS.FTS$RELATION_NAME,\n"
			"  FTS$INDEX_SEGMENTS.FTS$FIELD_NAME,\n"
		    "  FTS$INDEX_SEGMENTS.FTS$BOOST,\n"
			"  FTS$INDICES.FTS$ANALYZER,\n"
		    "  FTS$INDICES.FTS$INDEX_STATUS\n"
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
		if (output->boostNull) {
			indexSegment.boost = 1.0;
		}
		else {
			indexSegment.boost = output->boost;
		}
		indexSegment.index.indexName.assign(output->indexName.str, output->indexName.length);
		indexSegment.index.analyzer.assign(output->analyzerName.str, output->analyzerName.length);
		indexSegment.index.status.assign(output->indexStatus.str, output->indexStatus.length);

		// замечание: описание индекса не требуется копировать
		segments.push_back(indexSegment);
	}
	rs->close(status);
	return segments;
}

/// <summary>
/// Returns index segments by relation name.
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="relationName">Relation name</param>
/// 
/// <returns>List of index segments</returns>
list<FTSIndexSegment> FTSIndexRepository::getIndexSegmentsByRelation (
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
		(FB_DOUBLE, boost)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
		(FB_INTL_VARCHAR(4, CS_UTF8), indexStatus)
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
			"  FTS$INDEX_SEGMENTS.FTS$BOOST,\n"
			"  FTS$INDICES.FTS$ANALYZER,\n"
			"  FTS$INDICES.FTS$INDEX_STATUS\n"
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
		if (output->boostNull) {
			indexSegment.boost = 1.0;
		}
		else {
			indexSegment.boost = output->boost;
		}
		indexSegment.index.indexName.assign(output->indexName.str, output->indexName.length);
		indexSegment.index.analyzer.assign(output->analyzerName.str, output->analyzerName.length);
		indexSegment.index.status.assign(output->indexStatus.str, output->indexStatus.length);

		// note: index description does not need to be copied
		segments.push_back(indexSegment);
	}
	rs->close(status);
	return segments;
}

/// <summary>
/// Adds a new field (segment) to the full-text index.
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="indexName">Index name</param>
/// <param name="relationName">Relation name</param>
/// <param name="fieldName">Field name</param>
/// <param name="boost">Significance multiplier</param>
void FTSIndexRepository::addIndexField(
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect,
	string indexName,
	string relationName,
	string fieldName,
	double boost)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
		(FB_DOUBLE, boost)
	) input(status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);

	input->fieldName.length = fieldName.length();
	fieldName.copy(input->fieldName.str, input->fieldName.length);

	if (boost == 1.0) {
		input->boostNull = true;
	}
	else {
		input->boost = boost;
	}

	// check for index existence
	if (!hasIndex(status, att, tra, sqlDialect, indexName)) {
		string error_message = string_format("Index \"%s\" not exists", indexName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	// segment existence check
	if (hasIndexSegment(status, att, tra, sqlDialect, indexName, relationName, fieldName)) {
		string error_message = string_format("Segment for \"%s\".\"%s\" already exists in index \"%s\"", relationName, fieldName, indexName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	// checking if a table exists
	if (!relationHelper.relationExists(status, att, tra, sqlDialect, relationName)) {
		string error_message = string_format("Table \"%s\" not exists.", relationName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	// field existence check
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
		"INSERT INTO FTS$INDEX_SEGMENTS(FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$FIELD_NAME, FTS$BOOST)\n"
		"VALUES(?, ?, ?, ?)",
		sqlDialect,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
	// set the status that the index metadata has been updated
	setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
}

/// <summary>
/// Removes a field (segment) from the full-text index.
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="indexName">Index name</param>
/// <param name="relationName">Relation name</param>
/// <param name="fieldName">Field name</param>
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

	// check for index existence
	if (!hasIndex(status, att, tra, sqlDialect, indexName)) {
		string error_message = string_format("Index \"%s\" not exists", indexName);
		ISC_STATUS statusVector[] = {
		   isc_arg_gds, isc_random,
		   isc_arg_string, (ISC_STATUS)error_message.c_str(),
		   isc_arg_end
		};
		throw FbException(status, statusVector);
	}

	// segment existence check
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
	// set the status that the index metadata has been updated
	setIndexStatus(status, att, tra, sqlDialect, indexName, "U");
}

/// <summary>
/// Checks for the existence of a field (segment) in a full-text index. 
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="indexName">Index name</param>
/// <param name="relationName">Relation name</param>
/// <param name="fieldName">Field name</param>
/// <returns>Returns true if the field (segment) exists in the index, false otherwise</returns>
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

/// <summary>
/// Returns a list of full-text index field names given the relation name.
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="relationName">Relation name</param>
/// 
/// <returns>List of full-text index field names</returns>
list<string> FTSIndexRepository::getFieldsByRelation (
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
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
	) output(status, m_master);

	input.clear();

	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);

	AutoRelease<IStatement> stmt(att->prepare(
		status,
		tra,
		0,
		"SELECT FTS$FIELD_NAME\n"
		"FROM FTS$INDEX_SEGMENTS\n"
		"WHERE FTS$RELATION_NAME = ?\n"
		"GROUP BY 1",
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

	list<string> fieldNames;
	while (rs->fetchNext(status, output.getData()) == IStatus::RESULT_OK) {
		string fieldName(output->fieldName.str, output->fieldName.length);
		fieldNames.push_back(fieldName);
	}
	rs->close(status);
	return fieldNames;
}

/// <summary>
/// Returns a list of trigger source codes to support full-text indexes by relation name. 
/// </summary>
/// 
/// <param name="status">Firebird status</param>
/// <param name="att">Firebird attachment</param>
/// <param name="tra">Firebird transaction</param>
/// <param name="sqlDialect">SQL dialect</param>
/// <param name="relationName">Relation name</param>
/// <param name="multiAction">Flag for generating multi-event triggers</param>
/// 
/// <returns>Trigger source code list</returns>
list<string> FTSIndexRepository::makeTriggerSourceByRelation (
	ThrowStatusWrapper* status,
	IAttachment* att,
	ITransaction* tra,
	unsigned int sqlDialect,
	string relationName,
	bool multiAction)
{
	list<string> triggerSources;

	list<string> fieldNames = getFieldsByRelation(status, att, tra, sqlDialect, relationName);

	if (fieldNames.size() == 0)
		return triggerSources;

	string insertingCondition;
	string updatingCondition;
	string deletingCondition;
	for (auto fieldName : fieldNames) {
		string metaFieldName = escapeMetaName(sqlDialect, fieldName);

		if (!insertingCondition.empty()) {
			insertingCondition += "\n      OR ";
		}
		insertingCondition += "NEW." + metaFieldName + " IS NOT NULL";

		if (!updatingCondition.empty()) {
			updatingCondition += "\n      OR ";
		}
		updatingCondition += "NEW." + metaFieldName + " IS DISTINCT FROM " + "OLD." + metaFieldName;

		if (!deletingCondition.empty()) {
			deletingCondition += "\n      OR ";
		}
		deletingCondition += "OLD." + metaFieldName + " IS NOT NULL";
	}

	
	if (multiAction) {
		string triggerName = "FTS$" + relationName + "_AIUD";
		string triggerSource =
			"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
			"ACTIVE AFTER INSERT OR UPDATE OR DELETE POSITION 100\n"
			"AS\n"
			"BEGIN\n"
			"  IF (INSERTING AND (" + insertingCondition + ")) THEN\n"
			"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', NEW.RDB$DB_KEY, 'I');\n"
			"  IF (UPDATING AND (" + updatingCondition + ")) THEN\n"
			"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', OLD.RDB$DB_KEY, 'U');\n"
			"  IF (DELETING AND (" + deletingCondition + ")) THEN\n"
			"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', OLD.RDB$DB_KEY, 'D');\n"
			"END";
		triggerSources.push_back(triggerSource);
	}
	else {
		// INSERT
		string triggerName = "FTS$" + relationName + "_AI";
		string triggerSource =
			"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
			"ACTIVE AFTER INSERT POSITION 100\n"
			"AS\n"
			"BEGIN\n"
			"  IF (" + insertingCondition + ") THEN\n"
			"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', NEW.RDB$DB_KEY, 'I');\n"
			"END";
		triggerSources.push_back(triggerSource);
		// UPDATE
		triggerName = "FTS$" + relationName + "_AU";
		triggerSource =
			"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
			"ACTIVE AFTER UPDATE POSITION 100\n"
			"AS\n"
			"BEGIN\n"
			"  IF (" + updatingCondition + ") THEN\n"
			"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', OLD.RDB$DB_KEY, 'U');\n"
			"END";
		triggerSources.push_back(triggerSource);
		// DELETE
		triggerName = "FTS$" + relationName + "_AD";
		triggerSource =
			"CREATE OR ALTER TRIGGER " + escapeMetaName(sqlDialect, triggerName) + " FOR " + escapeMetaName(sqlDialect, relationName) + "\n"
			"ACTIVE AFTER DELETE POSITION 100\n"
			"AS\n"
			"BEGIN\n"
			"  IF (" + deletingCondition + ") THEN\n"
			"    EXECUTE PROCEDURE FTS$LOG_CHANGE('" + relationName + "', OLD.RDB$DB_KEY, 'D');\n"
			"END";
		triggerSources.push_back(triggerSource);
	}
	return triggerSources;
}
