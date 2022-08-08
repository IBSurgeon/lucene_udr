/**
 *  Implementation of basic full-text search procedures.
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
#include "FBFieldInfo.h"
#include "EncodeUtils.h"
#include "LuceneHeaders.h"
#include "FileUtils.h"
#include "TermAttribute.h"
#include "LuceneAnalyzerFactory.h"
#include <sstream>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;

const std::string WHITESPACE = " \n\r\t\f\v";

std::string rtrim(const std::string& s)
{
	size_t end = s.find_last_not_of(WHITESPACE);
	return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

const string queryEscape(const string& query)
{
	stringstream ss;
	for (const auto ch : query) {
		switch (ch) {
		case '+':
		case '-':
		case '!':
		case '^':
		case '"':
		case '~':
		case '*':
		case '?':
		case ':':
		case '\\':
		case '&':
		case '|':
		case '(':
		case ')':
		case '[':
		case ']':
		case '{':
		case '}':
			ss << '\\' << ch;
			break;
		default:
			ss << ch;
		}
	}
	return ss.str();
}

/***
FUNCTION FTS$ESCAPE_QUERY (
    FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8
)
RETURNS VARCHAR(8191) CHARACTER SET UTF8
EXTERNAL NAME 'luceneudr!ftsEscapeQuery'
ENGINE UDR;
***/
FB_UDR_BEGIN_FUNCTION(ftsEscapeQuery)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(32765, CS_UTF8), query)
	);

	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(32765, CS_UTF8), query)
	);

	FB_UDR_EXECUTE_FUNCTION
	{
		if (!in->queryNull) {
			string queryStr;
			queryStr.assign(in->query.str, in->query.length);

			const auto escapedQuery = queryEscape(queryStr);

			out->queryNull = false;
			out->query.length = static_cast<ISC_USHORT>(escapedQuery.length());
			escapedQuery.copy(out->query.str, out->query.length);
		}
		else {
			out->queryNull = true;
		}
	}
FB_UDR_END_FUNCTION

/***
PROCEDURE FTS$SEARCH (
	FTS$INDEX_NAME VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$QUERY VARCHAR(8191) CHARACTER SET UTF8,
	FTS$LIMIT INT NOT NULL DEFAULT 1000,
	FTS$EXPLAIN BOOLEAN DEFAULT FALSE
)
RETURNS (
	FTS$RELATION_NAME VARCHAR(63) CHARACTER SET UTF8,
	FTS$KEY_FIELD_NAME VARCHAR(63) CHARACTER SET UTF8,
	FTS$DB_KEY CHAR(8) CHARACTER SET OCTETS,
	FTS$ID BIGINT,
	FTS$UUID CHAR(16) CHARACTER SET OCTETS,
	FTS$SCORE DOUBLE PRECISION,
	FTS$EXPLANATION BLOB SUB_TYPE TEXT CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!ftsSearch'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsSearch)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), indexName)
		(FB_INTL_VARCHAR(32765, CS_UTF8), query)
		(FB_INTEGER, limit)
		(FB_BOOLEAN, explain)
	);

	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(252, CS_UTF8), keyFieldName)
		(FB_INTL_VARCHAR(8, CS_BINARY), dbKey)
		(FB_BIGINT, id)
		(FB_INTL_VARCHAR(16, CS_BINARY), uuid)
		(FB_DOUBLE, score)
		(FB_BLOB, explanation)
	);

	FB_UDR_CONSTRUCTOR
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
		, analyzerFactory()
	{
	}

	FTSIndexRepositoryPtr indexRepository;
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
		if (in->indexNameNull) {
			throwException(status, "Index name can not be NULL");
		}
		const string indexName(in->indexName.str, in->indexName.length);
		
		string queryStr;
		if (!in->queryNull) {
			queryStr.assign(in->query.str, in->query.length);
		}

		const auto limit = static_cast<int32_t>(in->limit);

		if (!in->explainNull) {
			explainFlag = in->explain;
		}

		const auto& ftsDirectoryPath = getFtsDirectory(context);


		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		const auto& ftsIndex = make_unique<FTSIndex>();
		procedure->indexRepository->getIndex(status, att, tra, sqlDialect, ftsIndex, indexName, true);

		// check if directory exists for index
		const auto& indexDirectoryPath = ftsDirectoryPath / indexName;
		if (ftsIndex->status == "N" || !fs::is_directory(indexDirectoryPath)) {
			throwException(status, R"(Index "%s" exists, but is not build. Please rebuild index.)", indexName.c_str());
		}

		try {
			const auto& ftsIndexDir = FSDirectory::open(indexDirectoryPath.wstring());
			if (!IndexReader::indexExists(ftsIndexDir)) {
				throwException(status, R"(Index "%s" exists, but is not build. Please rebuild index.)", indexName.c_str());
			}

			IndexReaderPtr reader = IndexReader::open(ftsIndexDir, true);
			searcher = newLucene<IndexSearcher>(reader);
			AnalyzerPtr analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex->analyzer);

			string keyFieldName;
			auto fields = Collection<String>::newInstance();
			for (const auto& segment : ftsIndex->segments) {
				if (!segment->key) {
					fields.add(StringUtils::toUnicode(segment->fieldName));
				}
				else {
					keyFieldName = segment->fieldName;
					unicodeKeyFieldName = StringUtils::toUnicode(keyFieldName);
				}
			}

			keyFieldInfo = make_unique<RelationFieldInfo>();
			if (keyFieldName != "RDB$DB_KEY") {
				procedure->indexRepository->getRelationHelper()->getField(status, att, tra, sqlDialect, keyFieldInfo, ftsIndex->relationName, keyFieldName);
			}
			else {
				keyFieldInfo->initDB_KEYField(ftsIndex->relationName);
			}

			MultiFieldQueryParserPtr parser = newLucene<MultiFieldQueryParser>(LuceneVersion::LUCENE_CURRENT, fields, analyzer);
			parser->setDefaultOperator(QueryParser::OR_OPERATOR);
			query = parser->parse(StringUtils::toUnicode(queryStr));
			TopDocsPtr docs = searcher->search(query, limit);

			scoreDocs = docs->scoreDocs;

			it = scoreDocs.begin();

			out->relationNameNull = false;
			out->relationName.length = static_cast<ISC_USHORT>(ftsIndex->relationName.length());
			ftsIndex->relationName.copy(out->relationName.str, out->relationName.length);

			out->keyFieldNameNull = false;
			out->keyFieldName.length = static_cast<ISC_USHORT>(keyFieldName.length());
			keyFieldName.copy(out->keyFieldName.str, out->keyFieldName.length);

			out->dbKeyNull = true;
			out->uuidNull = true;
			out->idNull = true;
			out->scoreNull = true;
		}
		catch (LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	bool explainFlag = false;
	AutoRelease<IAttachment> att{ nullptr };
	AutoRelease<ITransaction> tra{ nullptr };
	RelationFieldInfoPtr keyFieldInfo;
	String unicodeKeyFieldName;
	QueryPtr query;
	SearcherPtr searcher;
	Collection<ScoreDocPtr> scoreDocs;
	Collection<ScoreDocPtr>::iterator it;

	FB_UDR_FETCH_PROCEDURE
	{
		try {
			if (it == scoreDocs.end()) {
				return false;
			}
			ScoreDocPtr scoreDoc = *it;
			DocumentPtr doc = searcher->doc(scoreDoc->doc);

			try {
				const string keyValue = StringUtils::toUTF8(doc->get(unicodeKeyFieldName));
				if (unicodeKeyFieldName == L"RDB$DB_KEY") {
					// In the Lucene index, the string is stored in hexadecimal form, so let's convert it back to binary format.
					const string dbKey = hex_to_string(keyValue);
					out->dbKeyNull = false;
					out->dbKey.length = static_cast<ISC_USHORT>(dbKey.length());
					dbKey.copy(out->dbKey.str, out->dbKey.length);
				}
				else if (keyFieldInfo->isBinary()) {
					// In the Lucene index, the string is stored in hexadecimal form, so let's convert it back to binary format.
					const string uuid = hex_to_string(keyValue);
					out->uuidNull = false;
					out->uuid.length = static_cast<ISC_USHORT>(uuid.length());
					uuid.copy(out->uuid.str, out->uuid.length);
				}
				else if (keyFieldInfo->isInt()) {
					out->idNull = false;
					out->id = std::stoll(keyValue);
				}
			}
			catch (invalid_argument& e) {
				throwException(status, e.what());
			}

			out->scoreNull = false;
			out->score = scoreDoc->score;

			if (explainFlag) {
				out->explanationNull = false;
				const auto explanation = searcher->explain(query, scoreDoc->doc);
				const string explanationStr = StringUtils::toUTF8(explanation->toString());
				AutoRelease<IBlob> blob(att->createBlob(status, tra, &out->explanation, 0, nullptr));
				BlobUtils::setString(status, blob, explanationStr);
				blob->close(status);
			}
			else {
				out->explanationNull = true;
			}

			++it;
		}
		catch (LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
		return true;
	}
FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$ANALYZE (
    FTS$TEXT BLOB SUB_TYPE TEXT CHARACTER SET UTF8,
	FTS$ANALYZER VARCHAR(63) CHARACTER SET UTF8 NOT NULL DEFAULT 'STANDARD'
)
RETURNS (
	FTS$TERM VARCHAR(8191) CHARACTER SET UTF8
)
EXTERNAL NAME 'luceneudr!ftsAnalyze'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsAnalyze)
	FB_UDR_MESSAGE(InMessage,
		(FB_BLOB, text)
		(FB_INTL_VARCHAR(252, CS_UTF8), analyzerName)
	);

	FB_UDR_MESSAGE(OutMessage,
		(FB_INTL_VARCHAR(32765, CS_UTF8), term)
	);

	FB_UDR_CONSTRUCTOR
		, analyzerFactory()
	{
	}

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

		string text;
		if (!in->textNull) {
			AutoRelease<IBlob> blob(att->openBlob(status, tra, &in->text, 0, nullptr));
			text = BlobUtils::getString(status, blob);
			blob->close(status);
		}

		string analyzerName = DEFAULT_ANALYZER_NAME;
		if (!in->analyzerNameNull) {
			analyzerName.assign(in->analyzerName.str, in->analyzerName.length);
		}

		try {
			const auto& analyzer = procedure->analyzerFactory.createAnalyzer(status, analyzerName);
			const auto& stringReader = newLucene<StringReader>(StringUtils::toUnicode(text));

			tokenStream = analyzer->tokenStream(L"", stringReader);
			termAttribute = tokenStream->addAttribute<TermAttribute>();
			tokenStream->reset();
		}
		catch (LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	TokenStreamPtr tokenStream = nullptr;
	TermAttributePtr termAttribute = nullptr;

	FB_UDR_FETCH_PROCEDURE
	{
		if (!tokenStream->incrementToken()) {
			return false;
		}
		const auto uTerm = termAttribute->term();

		if (uTerm.length() > 8191) {
			throwException(status, "Term size exceeds 8191 characters");
		}

		const string term = StringUtils::toUTF8(uTerm);

		out->termNull = false;
		out->term.length = static_cast<ISC_USHORT>(term.length());
		term.copy(out->term.str, out->term.length);

		return true;
	}

FB_UDR_END_PROCEDURE

/***
PROCEDURE FTS$LOG_BY_DBKEY (
    FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$DBKEY          CHAR(8) CHARACTER SET OCTETS NOT NULL,
	FTS$CHANGE_TYPE    FTS$D_CHANGE_TYPE NOT NULL
)
EXTERNAL NAME 'luceneudr!ftsLogByDdKey'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsLogByDdKey)
    FB_UDR_MESSAGE(InMessage,
	    (FB_INTL_VARCHAR(252, CS_UTF8), relationName)
	    (FB_INTL_VARCHAR(8, CS_BINARY), dbKey)
		(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
    );

	FB_UDR_CONSTRUCTOR
		, inputMetadata(metadata->getInputMetadata(status))
		, appendLogStmt(nullptr)
	{
	}

	AutoRelease<IMessageMetadata> inputMetadata;
	AutoRelease<IStatement> appendLogStmt;

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
		if (in->relationNameNull) {
			throwException(status, "FTS$RELATION_NAME can not be NULL");
		}
	    string relationName(in->relationName.str, in->relationName.length);
	    relationName = rtrim(relationName);

		if (in->dbKeyNull) {
			throwException(status, "FTS$DBKEY can not be NULL");
		}
		const string dbKey(in->dbKey.str, in->dbKey.length);

		if (in->changeTypeNull) {
			throwException(status, "FTS$CHANGE_TYPE can not be NULL");
		}
		string changeType(in->changeType.str, in->changeType.length);
		changeType = rtrim(changeType);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		// prepare statement for append record to FTS log
		if (!procedure->appendLogStmt.hasData()) {
			procedure->appendLogStmt.reset(att->prepare(
				status,
				tra,
				0,
				SQL_APPEND_LOG,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(8, CS_BINARY), dbKey)
			(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
		) input(status, context->getMaster());

		input.clear();

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		input->dbKey.length = static_cast<ISC_USHORT>(dbKey.length());
		dbKey.copy(input->dbKey.str, input->dbKey.length);

		input->changeType.length = static_cast<ISC_USHORT>(changeType.length());
		changeType.copy(input->changeType.str, input->changeType.length);

		procedure->appendLogStmt->execute(
			status,
			tra,
			inputMetadata,
			input.getData(),
			nullptr,
			nullptr
		);
	}

	const char* SQL_APPEND_LOG =
		R"SQL(
INSERT INTO FTS$LOG (
  FTS$RELATION_NAME,
  FTS$DB_KEY,
  FTS$REC_UUID,
  FTS$REC_ID,
  FTS$CHANGE_TYPE
)
VALUES(?, ?, NULL, NULL, ?)
)SQL";

    FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$LOG_BY_ID (
	FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$ID             BIGINT NOT NULL,
	FTS$CHANGE_TYPE    FTS$D_CHANGE_TYPE NOT NULL
)
EXTERNAL NAME 'luceneudr!ftsLogById'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsLogById)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_BIGINT, id)
		(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
	);

	FB_UDR_CONSTRUCTOR
		, inputMetadata(metadata->getInputMetadata(status))
		, appendLogStmt(nullptr)
	{
	}

	AutoRelease<IMessageMetadata> inputMetadata;
	AutoRelease<IStatement> appendLogStmt;

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
		if (in->relationNameNull) {
			throwException(status, "FTS$RELATION_NAME can not be NULL");
		}
	    string relationName(in->relationName.str, in->relationName.length);
		relationName = rtrim(relationName);

		if (in->idNull) {
			throwException(status, "FTS$ID can not be NULL");
		}

		if (in->changeTypeNull) {
			throwException(status, "FTS$CHANGE_TYPE can not be NULL");
		}
		string changeType(in->changeType.str, in->changeType.length);
		changeType = rtrim(changeType);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		// prepare statement for append record to FTS log
		if (!procedure->appendLogStmt.hasData()) {
			procedure->appendLogStmt.reset(att->prepare(
				status,
				tra,
				0,
				SQL_APPEND_LOG,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_BIGINT, id)
			(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
		) input(status, context->getMaster());

		input.clear();

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		input->idNull = in->idNull;
		input->id = in->id;

		input->changeType.length = static_cast<ISC_USHORT>(changeType.length());
		changeType.copy(input->changeType.str, input->changeType.length);

		procedure->appendLogStmt->execute(
			status,
			tra,
			inputMetadata,
			input.getData(),
			nullptr,
			nullptr
		);
	}


	const char* SQL_APPEND_LOG =
		R"SQL(
INSERT INTO FTS$LOG (
  FTS$RELATION_NAME,
  FTS$DB_KEY,
  FTS$REC_UUID,
  FTS$REC_ID,
  FTS$CHANGE_TYPE
)
VALUES(?, NULL, NULL, ?, ?)
)SQL";

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE


/***
PROCEDURE FTS$LOG_BY_UUID (
	FTS$RELATION_NAME  VARCHAR(63) CHARACTER SET UTF8 NOT NULL,
	FTS$UUID           CHAR(16) CHARACTER SET OCTETS NOT NULL,
	FTS$CHANGE_TYPE    FTS$D_CHANGE_TYPE NOT NULL
)
EXTERNAL NAME 'luceneudr!ftsLogByUuid'
ENGINE UDR;
***/
FB_UDR_BEGIN_PROCEDURE(ftsLogByUuid)
	FB_UDR_MESSAGE(InMessage,
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_INTL_VARCHAR(16, CS_BINARY), uuid)
		(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
	);

	FB_UDR_CONSTRUCTOR
		, inputMetadata(metadata->getInputMetadata(status))
		, appendLogStmt(nullptr)
	{
	}

	AutoRelease<IMessageMetadata> inputMetadata;
	AutoRelease<IStatement> appendLogStmt;

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
		if (in->relationNameNull) {
			throwException(status, "FTS$RELATION_NAME can not be NULL");
		}
	    string relationName(in->relationName.str, in->relationName.length);
	    relationName = rtrim(relationName);

		if (in->uuidNull) {
			throwException(status, "FTS$UUID can not be NULL");
		}
		const string uuid(in->uuid.str, in->uuid.length);

		if (in->changeTypeNull) {
			throwException(status, "FTS$CHANGE_TYPE can not be NULL");
		}
		string changeType(in->changeType.str, in->changeType.length);
		changeType = rtrim(changeType);

		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		const unsigned int sqlDialect = getSqlDialect(status, att);

		// prepare statement for append record to FTS log
		if (!procedure->appendLogStmt.hasData()) {
			procedure->appendLogStmt.reset(att->prepare(
				status,
				tra,
				0,
				SQL_APPEND_LOG,
				sqlDialect,
				IStatement::PREPARE_PREFETCH_METADATA
			));
		}

		FB_MESSAGE(Input, ThrowStatusWrapper,
			(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
			(FB_INTL_VARCHAR(16, CS_BINARY), uuid)
			(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
		) input(status, context->getMaster());

		input.clear();

		AutoRelease<IMessageMetadata> inputMetadata(input.getMetadata());

		input->relationName.length = static_cast<ISC_USHORT>(relationName.length());
		relationName.copy(input->relationName.str, input->relationName.length);

		input->uuid.length = static_cast<ISC_USHORT>(uuid.length());
		uuid.copy(input->uuid.str, input->uuid.length);

		input->changeType.length = static_cast<ISC_USHORT>(changeType.length());
		changeType.copy(input->changeType.str, input->changeType.length);

		procedure->appendLogStmt->execute(
			status,
			tra,
			inputMetadata,
			input.getData(),
			nullptr,
			nullptr
		);
	}

	const char* SQL_APPEND_LOG =
		R"SQL(
INSERT INTO FTS$LOG (
  FTS$RELATION_NAME,
  FTS$DB_KEY,
  FTS$REC_UUID,
  FTS$REC_ID,
  FTS$CHANGE_TYPE
)
VALUES(?, NULL, ?, NULL, ?)
)SQL";


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

		const unsigned int sqlDialect = getSqlDialect(status, att);

		att->execute(
			status,
			tra,
			0,
			"DELETE FROM FTS$LOG",
			sqlDialect,
			nullptr,
			nullptr,
			nullptr,
			nullptr
		);
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
		, indexRepository(make_unique<FTSIndexRepository>(context->getMaster()))
		, analyzerFactory()
		, logDeleteStmt(nullptr)
		, logStmt(nullptr)
	{
	}


	FTSIndexRepositoryPtr indexRepository{nullptr};
	LuceneAnalyzerFactory analyzerFactory;
	AutoRelease<IStatement> logDeleteStmt{nullptr};
	AutoRelease<IStatement> logStmt{nullptr};

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

		const unsigned int sqlDialect = getSqlDialect(status, att);

		const auto& ftsDirectoryPath = getFtsDirectory(context);
		
		// get all indexes with segments
		FTSIndexMap indexes;
		procedure->indexRepository->fillAllIndexesWithFields(status, att, tra, sqlDialect, indexes);
		// fill map indexes of relationName
		map<string, list<string>> indexesByRelation;
		for (const auto& [indexName, ftsIndex] : indexes) {	
			if (!ftsIndex->isActive()) {
				continue;
			}
			const auto& indexDirectoryPath = ftsDirectoryPath / indexName;
			if (!ftsIndex->checkAllFieldsExists() || !fs::is_directory(indexDirectoryPath)) {
				// index need to rebuild
				setIndexToRebuild(status, att, sqlDialect, ftsIndex);
				// go to next index
				continue;
			}
			ftsIndex->unicodeIndexDir = indexDirectoryPath.wstring();

			// collect and save the SQL query to extract the record by key
			ftsIndex->prepareExtractRecordStmt(status, att, tra, sqlDialect);

			const auto& inMetadata = ftsIndex->getInExtractRecordMetadata();
			if (inMetadata->getCount(status) != 1) {
				// The number of input parameters must be equal to 1.
				// index need to rebuild
				setIndexToRebuild(status, att, sqlDialect, ftsIndex);
				// go to next index
				continue;
			}
			const auto& params = make_unique<FbFieldsInfo>(status, inMetadata);
			const auto& keyParam = params->at(0);
			if (keyParam->isBinary()) {
				switch (keyParam->length) {
				case 8:
					ftsIndex->keyFieldType = FTSKeyType::DB_KEY;
					break;
				case 16:
					ftsIndex->keyFieldType = FTSKeyType::UUID;
					break;
				default:
					// index need to rebuild
					setIndexToRebuild(status, att, sqlDialect, ftsIndex);
					// go to next index
					continue;
				}
			}
			else if (keyParam->isInt()) {
				ftsIndex->keyFieldType = FTSKeyType::INT_ID;
			}
			else {
				// index need to rebuild
				setIndexToRebuild(status, att, sqlDialect, ftsIndex);
				// go to next index
				continue;
			}


			const auto& outMetadata =  ftsIndex->getOutExtractRecordMetadata();
			auto fields = make_unique<FbFieldsInfo>(status, outMetadata);
			// initial specific FTS property for fields
			for (unsigned int i = 0; i < fields->size(); i++) {
				const auto& field = fields->at(i);
				auto iSegment = ftsIndex->findSegment(field->fieldName);
				if (iSegment == ftsIndex->segments.end()) {
					// index need to rebuild
					setIndexToRebuild(status, att, sqlDialect, ftsIndex);
					// go to next index
					continue;
				}
				auto const& segment = *iSegment;
				field->ftsFieldName = StringUtils::toUnicode(segment->fieldName);
				field->ftsKey = segment->key;
				field->ftsBoost = segment->boost;
				field->ftsBoostNull = segment->boostNull;
				if (field->ftsKey) {
					ftsIndex->unicodeKeyFieldName = field->ftsFieldName;
				}
			}
			fieldsInfoMap[indexName] = std::move(fields);

            // put indexName to map
			auto it = indexesByRelation.find(ftsIndex->relationName);
			if (it == indexesByRelation.end()) {
				list<string> indexNames;
				indexNames.push_back(ftsIndex->indexName);
				indexesByRelation[ftsIndex->relationName] = indexNames;
			}
			else {
				it->second.push_back(ftsIndex->indexName);
			}
		}

		try 
		{
			DBKEYInput dbKeyInput(status, context->getMaster());
			UUIDInput uuidInput(status, context->getMaster());
			IDInput idInput(status, context->getMaster());

			AutoRelease<IMessageMetadata> dbKeyInputMetadata(dbKeyInput.getMetadata());
			AutoRelease<IMessageMetadata> uuidInputMetadata(uuidInput.getMetadata());
			AutoRelease<IMessageMetadata> idInputMetadata(idInput.getMetadata());


			// prepare statement for delete record from FTS log
			if (!procedure->logDeleteStmt.hasData()) {
				procedure->logDeleteStmt.reset(att->prepare(
					status,
					tra,
					0,
					SQL_DELETE_FTS_LOG,
					sqlDialect,
					IStatement::PREPARE_PREFETCH_METADATA
				));
			}

			LogDelInput logDelInput(status, context->getMaster());
			AutoRelease<IMessageMetadata> logDelInputMetadata(logDelInput.getMetadata());

			// prepare statement for retrieval record from FTS log 
			if (!procedure->logStmt.hasData()) {
				procedure->logStmt.reset(att->prepare(
					status,
					tra,
					0,
					SQL_SELECT_FTS_LOG,
					sqlDialect,
					IStatement::PREPARE_PREFETCH_METADATA
				));
			}

			LogOutput logOutput(status, context->getMaster());

			AutoRelease<IMessageMetadata> logOutputMetadata(logOutput.getMetadata());

			AutoRelease<IResultSet> logRs(procedure->logStmt->openCursor(
				status,
				tra,
				nullptr,
				nullptr,
				logOutputMetadata,
				0
			));


			while (logRs->fetchNext(status, logOutput.getData()) == IStatus::RESULT_OK) {
				const ISC_INT64 logId = logOutput->id;
				const string relationName(logOutput->relationName.str, logOutput->relationName.length);
				const string changeType(logOutput->changeType.str, logOutput->changeType.length);


				const auto itIndexNames = indexesByRelation.find(relationName);
				if (itIndexNames == indexesByRelation.end()) {
					continue;
				}
				const auto& indexNames = itIndexNames->second;

				// for all indexes for relationName
				for (const auto& indexName : indexNames) {
					const auto& ftsIndex = indexes[indexName];
					const auto& indexWriter = getIndexWriter(status, ftsIndex);

					Lucene::String unicodeKeyValue;
					switch (ftsIndex->keyFieldType) {
					case FTSKeyType::DB_KEY:
						if (!logOutput->dbKeyNull) {
							string dbKey(logOutput->dbKey.str, logOutput->dbKey.length);
							unicodeKeyValue = StringUtils::toUnicode(string_to_hex(dbKey));
						}
						break;
					case FTSKeyType::UUID:
						if (!logOutput->uuidNull) {
							string uuid(logOutput->uuid.str, logOutput->uuid.length);
							unicodeKeyValue = StringUtils::toUnicode(string_to_hex(uuid));
						}
						break;
					case FTSKeyType::INT_ID:
						if (!logOutput->recIdNull) {
							string id = std::to_string(logOutput->recId);
							unicodeKeyValue = StringUtils::toUnicode(id);
						}
						break;
					default:
						continue;
					}

					if (unicodeKeyValue.empty()) {
						continue;
					}

					if (changeType == "D") {						
						TermPtr term = newLucene<Term>(ftsIndex->unicodeKeyFieldName, unicodeKeyValue);
						indexWriter->deleteDocuments(term);		
						continue;
					}

					const auto& stmt = ftsIndex->getPreparedExtractRecordStmt();
					const auto& outMetadata = ftsIndex->getOutExtractRecordMetadata();

					const auto& fields = *fieldsInfoMap[indexName];

					AutoRelease<IResultSet> rs(nullptr);
					switch (ftsIndex->keyFieldType) {
					case FTSKeyType::DB_KEY:
						if (logOutput->dbKeyNull) continue;
						dbKeyInput->dbKeyNull = logOutput->dbKeyNull;
						dbKeyInput->dbKey.length = logOutput->dbKey.length;
						memcpy(dbKeyInput->dbKey.str, logOutput->dbKey.str, logOutput->dbKey.length);
						rs.reset(stmt->openCursor(
							status,
							tra,
							dbKeyInputMetadata,
							dbKeyInput.getData(),
							outMetadata,
							0
						));
						break;
					case FTSKeyType::UUID:
						if (logOutput->uuidNull) continue;
						uuidInput->uuidNull = logOutput->uuidNull;
						uuidInput->uuid.length = logOutput->uuid.length;
						memcpy(uuidInput->uuid.str, logOutput->uuid.str, uuidInput->uuid.length);
						rs.reset(stmt->openCursor(
							status,
							tra,
							uuidInputMetadata,
							uuidInput.getData(),
							outMetadata,
							0
						));
						break;
					case FTSKeyType::INT_ID:
						if (logOutput->recIdNull) continue;
						idInput->idNull = logOutput->recIdNull;
						idInput->id = logOutput->recId;
						rs.reset(stmt->openCursor(
							status,
							tra,
							idInputMetadata,
							idInput.getData(),
							outMetadata,
							0
						));
						break;
					default:
						continue;
					}

					if (!rs.hasData()) {
						continue;
					}

					const unsigned colCount = outMetadata->getCount(status);
					const unsigned msgLength = outMetadata->getMessageLength(status);
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
							if ((changeType == "I") && !emptyFlag) {
								indexWriter->addDocument(doc);
							}
							if (changeType == "U") {
								if (!ftsIndex->unicodeKeyFieldName.empty()) {
									TermPtr term = newLucene<Term>(ftsIndex->unicodeKeyFieldName, unicodeKeyValue);
									if (!emptyFlag) {
										indexWriter->updateDocument(term, doc);
									}
									else {
										indexWriter->deleteDocuments(term);
									}
								}
							}
							memset(buffer, 0, msgLength);
						}
						rs->close(status);
					}
				}
				// delete record from FTS log
				logDelInput->idNull = false;
				logDelInput->id = logId;
				procedure->logDeleteStmt->execute(
					status,
					tra,
					logDelInputMetadata,
					logDelInput.getData(),
					nullptr,
					nullptr
				);
				
			}
			logRs->close(status);
			// commit changes for all indexes
			for (const auto& pIndexWriter : indexWriters) {
				const auto& indexWriter = pIndexWriter.second;
				indexWriter->commit();
				indexWriter->close();
			}
		}
		catch (const LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	// Input message for extracting a record by RDB$DB_KEY
	FB_MESSAGE(DBKEYInput, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(8, CS_BINARY), dbKey)
	);

	// Input message for extracting a record by UUID
	FB_MESSAGE(UUIDInput, ThrowStatusWrapper,
		(FB_INTL_VARCHAR(16, CS_BINARY), uuid)
	);

	// Input message for extracting a record by integer ID
	FB_MESSAGE(IDInput, ThrowStatusWrapper,
		(FB_BIGINT, id)
	);

	// Input message for the FTS log record delete statement
	FB_MESSAGE(LogDelInput, ThrowStatusWrapper,
		(FB_BIGINT, id)
	);

	const char* SQL_DELETE_FTS_LOG =
R"SQL(
DELETE FROM FTS$LOG
WHERE FTS$LOG_ID = ?
)SQL";

	// FTS log output message
	FB_MESSAGE(LogOutput, ThrowStatusWrapper,
		(FB_BIGINT, id)
		(FB_INTL_VARCHAR(252, CS_UTF8), relationName)
		(FB_VARCHAR(8), dbKey)
		(FB_VARCHAR(16), uuid)
		(FB_BIGINT, recId)
		(FB_INTL_VARCHAR(4, CS_UTF8), changeType)
	);

	const char* SQL_SELECT_FTS_LOG =
R"SQL(
SELECT
    FTS$LOG_ID
  , TRIM(FTS$RELATION_NAME) AS FTS$RELATION_NAME
  , FTS$DB_KEY
  , FTS$REC_UUID
  , FTS$REC_ID
  , FTS$CHANGE_TYPE
FROM FTS$LOG
ORDER BY FTS$LOG_ID
)SQL";

	map<string, FbFieldsInfoPtr> fieldsInfoMap;
	map<string, IndexWriterPtr> indexWriters;


	void setIndexToRebuild(ThrowStatusWrapper* const status, IAttachment* const att, const unsigned int sqlDialect, const FTSIndexPtr& ftsIndex)
	{
		ftsIndex->status = "U";
		// this is done in an autonomous transaction				
		AutoRelease<ITransaction> tra(att->startTransaction(status, 0, nullptr));
		procedure->indexRepository->setIndexStatus(status, att, tra, sqlDialect, ftsIndex->indexName, ftsIndex->status);
		tra->commit(status);
	}

	IndexWriterPtr const getIndexWriter(ThrowStatusWrapper* const status, const FTSIndexPtr& ftsIndex)
	{
		const auto it = indexWriters.find(ftsIndex->indexName);
		if (it == indexWriters.end()) {
			const auto& fsIndexDir = FSDirectory::open(ftsIndex->unicodeIndexDir);
			const auto& analyzer = procedure->analyzerFactory.createAnalyzer(status, ftsIndex->analyzer);
			const auto& indexWriter = newLucene<IndexWriter>(fsIndexDir, analyzer, IndexWriter::MaxFieldLengthLIMITED);
			indexWriters[ftsIndex->indexName] = indexWriter;
			return indexWriter;
		}
		else {
			return it->second;
		}
	}

	FB_UDR_FETCH_PROCEDURE
	{
		return false;
	}

FB_UDR_END_PROCEDURE

FB_UDR_IMPLEMENT_ENTRY_POINT
