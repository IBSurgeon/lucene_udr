/**
 *  Implementation of procedures and functions of the FTS$HIGHLIGHTER package.
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
#include "FBUtils.h"
#include "EncodeUtils.h"
#include "LuceneHeaders.h"
#include "FileUtils.h"
#include "LuceneAnalyzerFactory.h"
#include "SimpleHTMLFormatter.h"
#include "QueryScorer.h"
#include "Highlighter.h"
#include "SimpleSpanFragmenter.h"
#include "LuceneAnalyzerFactory.h"
#include <sstream>
#include <vector>
#include <memory>
#include <algorithm>

using namespace Firebird;
using namespace Lucene;
using namespace LuceneUDR;

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

	LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_FUNCTION
	{
		AutoRelease<IAttachment> att(context->getAttachment(status));
		AutoRelease<ITransaction> tra(context->getTransaction(status));

		out->fragmentNull = true;

		string text;
		if (!in->textNull) {
			AutoRelease<IBlob> blob(att->openBlob(status, tra, &in->text, 0, nullptr));
			text = BlobUtils::getString(status, blob);
			blob->close(status);
		}

		string queryStr;
		if (!in->queryNull) {
			queryStr.assign(in->query.str, in->query.length);
		}

		string analyzerName = DEFAULT_ANALYZER_NAME;
		if (!in->analyzer_nameNull) {
			analyzerName.assign(in->analyzer_name.str, in->analyzer_name.length);
		}

		string fieldName;
		if (!in->field_nameNull) {
			fieldName.assign(in->field_name.str, in->field_name.length);
		}

		const ISC_SHORT fragmentSize = in->fragment_size;

		if (fragmentSize > 8191) {
			// exceeds Firebird's maximum string size
			throwException(status, "Fragment size cannot exceeds 8191 characters");
		}
		if (fragmentSize <= 0) {
			throwException(status, "Fragment size must be greater than 0");
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
			const auto& analyzer = analyzerFactory.createAnalyzer(status, analyzerName);
			const auto& parser = newLucene<QueryParser>(LuceneVersion::LUCENE_CURRENT, StringUtils::toUnicode(fieldName), analyzer);
			const auto& query = parser->parse(StringUtils::toUnicode(queryStr));
			const auto& formatter = newLucene<SimpleHTMLFormatter>(StringUtils::toUnicode(leftTag), StringUtils::toUnicode(rightTag));
			const auto& scorer = newLucene<QueryScorer>(query);
			const auto& highlighter = newLucene<Highlighter>(formatter, scorer);
			const auto& fragmenter = newLucene<SimpleSpanFragmenter>(scorer, fragmentSize);
			highlighter->setTextFragmenter(fragmenter);
			const auto content = highlighter->getBestFragment(analyzer, StringUtils::toUnicode(fieldName), StringUtils::toUnicode(text));

			if (!content.empty()) {
				if (content.length() > 8191) {
					throwException(status, "Fragment size exceeds 8191 characters");
				}
				string fragment = StringUtils::toUTF8(content);
				out->fragmentNull = false;
				out->fragment.length = static_cast<ISC_USHORT>(fragment.length());
				fragment.copy(out->fragment.str, out->fragment.length);
			}
		}
		catch (LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
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

	LuceneAnalyzerFactory analyzerFactory;

	FB_UDR_EXECUTE_PROCEDURE
	{
		att.reset(context->getAttachment(status));
		tra.reset(context->getTransaction(status));

		out->fragmentNull = true;

		string text;
		if (!in->textNull) {
			AutoRelease<IBlob> blob(att->openBlob(status, tra, &in->text, 0, nullptr));
			text = BlobUtils::getString(status, blob);
			blob->close(status);
		}

		string queryStr;
		if (!in->queryNull) {
			queryStr.assign(in->query.str, in->query.length);
		}

		string analyzerName = DEFAULT_ANALYZER_NAME;
		if (!in->analyzer_nameNull) {
			analyzerName.assign(in->analyzer_name.str, in->analyzer_name.length);
		}

		string fieldName;
		if (!in->field_nameNull) {
			fieldName.assign(in->field_name.str, in->field_name.length);
		}

		const ISC_SHORT fragmentSize = in->fragment_size;

		if (fragmentSize > 8191) {
			// exceeds Firebird's maximum string size
			throwException(status, "Fragment size cannot exceed 8191 characters");
		}
		if (fragmentSize <= 0) {
			throwException(status, "Fragment size must be greater than 0");
		}

		const ISC_LONG maxNumFragments = in->maxNumFragments;

		string leftTag;
		if (!in->left_tagNull) {
			leftTag.assign(in->left_tag.str, in->left_tag.length);
		}

		string rightTag;
		if (!in->right_tagNull) {
			rightTag.assign(in->right_tag.str, in->right_tag.length);
		}

		try {
			const auto& analyzer = procedure->analyzerFactory.createAnalyzer(status, analyzerName);
			const auto& parser = newLucene<QueryParser>(LuceneVersion::LUCENE_CURRENT, StringUtils::toUnicode(fieldName), analyzer);
			const auto& query = parser->parse(StringUtils::toUnicode(queryStr));
			const auto& formatter = newLucene<SimpleHTMLFormatter>(StringUtils::toUnicode(leftTag), StringUtils::toUnicode(rightTag));
			const auto& scorer = newLucene<QueryScorer>(query);
			const auto& highlighter = newLucene<Highlighter>(formatter, scorer);
			const auto& fragmenter = newLucene<SimpleSpanFragmenter>(scorer, fragmentSize);
			highlighter->setTextFragmenter(fragmenter);

			fragments = highlighter->getBestFragments(analyzer, StringUtils::toUnicode(fieldName), StringUtils::toUnicode(text), maxNumFragments);
			it = fragments.begin();
		}
		catch (LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
			throwException(status, error_message.c_str());
		}
	}

	AutoRelease<IAttachment> att{nullptr};
	AutoRelease<ITransaction> tra{nullptr};
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
				throwException(status, "Fragment size exceeds 8191 characters");
			}
			const string fragment = StringUtils::toUTF8(content);
			out->fragmentNull = false;
			out->fragment.length = static_cast<ISC_USHORT>(fragment.length());
			fragment.copy(out->fragment.str, out->fragment.length);
		}

		++it;
		return true;
	}
FB_UDR_END_PROCEDURE