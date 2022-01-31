
#include "FTSIndex.h"
#include "FBBlobUtils.h"

using namespace Firebird;
using namespace std;
using namespace LuceneFTS;

//
// �������� ������ ��������������� �������
//
void FTSIndexRepository::createIndex(
	ThrowStatusWrapper status,
	IAttachment* att,
	ITransaction* tra,
	string indexName,
	string analyzer,
	string description)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
		(FB_BLOB, description)
	) input(&status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	input->analyzer.length = analyzer.length();
	analyzer.copy(input->analyzer.str, input->analyzer.length);

	if (!description.empty()) {

		AutoRelease<IBlob> blob(att->createBlob(&status, tra, &input->description, 0, nullptr));
		blob_set_string(&status, blob, description);
		blob->close(&status);
	}
	else {
		input->descriptionNull = true;
	}

	att->execute(
		&status,
		tra,
		0,
		"INSERT INTO FTS$INDICES(FTS$INDEX_NAME, FTS$ANALYZER, FTS$DESCRIPTION)\n"
		"VALUES(?, ?, ?)",
		UDR_SQL_DIALECT,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
}

//
// �������� ��������������� �������
//
void FTSIndexRepository::dropIndex(
	ThrowStatusWrapper status,
	IAttachment* att,
	ITransaction* tra,
	string indexName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
	) input(&status, m_master);

	input.clear();
	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	att->execute(
		&status,
		tra,
		0,
		"DELETE FROM FTS$INDICES WHERE FTS$INDEX_NAME = ?",
		UDR_SQL_DIALECT,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
}

//
// ���������� ���������� �� ������ � �������� ������.
//
bool FTSIndexRepository::hasIndex(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string indexName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
	) input(&status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTEGER, cnt)
	) output(&status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	if (!stmt_exists_index.hasData()) {
		stmt_exists_index.reset(att->prepare(
			&status,
			tra,
			0,
			"SELECT COUNT(*) AS CNT\n"
			"FROM FTS$INDICES\n"
			"WHERE FTS$INDEX_NAME = ?",
			UDR_SQL_DIALECT,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	}
	AutoRelease<IResultSet> rs(stmt_exists_index->openCursor(
		&status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	bool foundFlag = false;
	if (rs->fetchNext(&status, output.getData()) == IStatus::RESULT_OK) {
		foundFlag = (output->cnt > 0);
	}
	rs->close(&status);

	return foundFlag;
}

//
// �������� ���������� �� ������� � �������� ������, ���� �� ����������.
// ���������� true, ���� ������ ����������, � false - � ��������� ������.
//
bool FTSIndexRepository::getIndex(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string indexName, FTSIndex& ftsIndex)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
	) input(&status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzer)
		(FB_BLOB, description)
	) output(&status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	if (!stmt_get_index.hasData()) {
		stmt_get_index.reset(att->prepare(
			&status,
			tra,
			0,
			"SELECT FTS$INDEX_NAME, FTS$ANALYZER, FTS$DESCRIPTION\n"
			"FROM FTS$INDICES\n"
			"WHERE FTS$INDEX_NAME = ?",
			UDR_SQL_DIALECT,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	}
	AutoRelease<IResultSet> rs(stmt_get_index->openCursor(
		&status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	bool foundFlag = false;
	if (rs->fetchNext(&status, output.getData()) == IStatus::RESULT_OK) {
		foundFlag = true;
		ftsIndex.indexName.assign(output->indexName.str, output->indexName.length);
		ftsIndex.analyzer.assign(output->analyzer.str, output->analyzer.length);
		if (!output->descriptionNull) {
			AutoRelease<IBlob> blob(att->openBlob(&status, tra, &output->description, 0, nullptr));
			ftsIndex.description = blob_get_string(&status, blob);
			blob->close(&status);
		}
	}
	rs->close(&status);

	return foundFlag;
}

//
// ���������� �������� ������� � �������� ������
//
list<FTSIndexSegment> FTSIndexRepository::getIndexSegments(ThrowStatusWrapper status, IAttachment* att, ITransaction* tra, string indexName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
	) input(&status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
	) output(&status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	if (!stmt_index_segments.hasData()) {
		stmt_index_segments.reset(att->prepare(
			&status,
			tra,
			0,
			"SELECT FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$FIELD_NAME\n"
			"FROM FTS$INDEX_SEGMENTS\n"
			"WHERE FTS$INDEX_NAME = ?",
			UDR_SQL_DIALECT,
			IStatement::PREPARE_PREFETCH_METADATA
		));
	}
	AutoRelease<IResultSet> rs(stmt_index_segments->openCursor(
		&status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	list<FTSIndexSegment> segments;
	while (rs->fetchNext(&status, output.getData()) == IStatus::RESULT_OK) {
		FTSIndexSegment indexSegment;
		indexSegment.indexName.assign(output->indexName.str, output->indexName.length);
		indexSegment.relationName.assign(output->relationName.str, output->relationName.length);
		indexSegment.fieldName.assign(output->fieldName.str, output->fieldName.length);
		segments.push_back(indexSegment);
	}
	rs->close(&status);
	return segments;
}

//
// ���������� ������ ���� (��������) ��������������� �������
//
void FTSIndexRepository::addIndexField(
	ThrowStatusWrapper status,
	IAttachment* att,
	ITransaction* tra,
	string indexName,
	string relationName,
	string fieldName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
	) input(&status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);

	input->fieldName.length = fieldName.length();
	fieldName.copy(input->fieldName.str, input->fieldName.length);

	att->execute(
		&status,
		tra,
		0,
		"INSERT INTO FTS$INDEX_SEGMENTS(FTS$INDEX_NAME, FTS$RELATION_NAME, FTS$FIELD_NAME)\n"
		"VALUES(?, ?, ?)",
		UDR_SQL_DIALECT,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
}

//
// �������� ���� (��������) �� ��������������� �������
//
void FTSIndexRepository::dropIndexField(
	ThrowStatusWrapper status,
	IAttachment* att,
	ITransaction* tra,
	string indexName,
	string relationName,
	string fieldName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
	) input(&status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);

	input->fieldName.length = fieldName.length();
	fieldName.copy(input->fieldName.str, input->fieldName.length);

	att->execute(
		&status,
		tra,
		0,
		"DELETE FROM FTS$INDEX_SEGMENTS\n"
		"WHERE FTS$INDEX_NAME = ? AND FTS$RELATION_NAME = ? AND FTS$FIELD_NAME = ?",
		UDR_SQL_DIALECT,
		input.getMetadata(),
		input.getData(),
		nullptr,
		nullptr
	);
}

//
// �������� ������������� ���� (��������) � �������������� �������
//
bool FTSIndexRepository::hasIndexSegment(
	ThrowStatusWrapper status,
	IAttachment* att,
	ITransaction* tra,
	string indexName,
	string relationName,
	string fieldName)
{
	FB_MESSAGE(Input, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), fieldName)
	) input(&status, m_master);

	FB_MESSAGE(Output, ThrowStatusWrapper,
		(FB_INTEGER, cnt)
	) output(&status, m_master);

	input.clear();

	input->indexName.length = indexName.length();
	indexName.copy(input->indexName.str, input->indexName.length);

	input->relationName.length = relationName.length();
	relationName.copy(input->relationName.str, input->relationName.length);

	input->fieldName.length = fieldName.length();
	fieldName.copy(input->fieldName.str, input->fieldName.length);

	AutoRelease<IStatement> stmt(att->prepare(
		&status,
		tra,
		0,
		"SELECT COUNT(*) AS CNT\n"
		"FROM FTS$INDEX_SEGMENTS\n"
		"WHERE FTS$INDEX_NAME = ? AND FTS$RELATION_NAME = ? AND FTS$FIELD_NAME = ?",
		UDR_SQL_DIALECT,
		IStatement::PREPARE_PREFETCH_METADATA
	));

	AutoRelease<IResultSet> rs(stmt->openCursor(
		&status,
		tra,
		input.getMetadata(),
		input.getData(),
		output.getMetadata(),
		0
	));
	bool foundFlag = false;
	if (rs->fetchNext(&status, output.getData()) == IStatus::RESULT_OK) {
		foundFlag = (output->cnt > 0);
	}
	rs->close(&status);

	return foundFlag;
}
