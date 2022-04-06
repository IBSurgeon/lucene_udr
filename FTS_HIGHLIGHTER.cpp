#include "LuceneUdr.h"
#include "FTSIndex.h"
#include "FBUtils.h"
#include "EncodeUtils.h"
#include "lucene++/LuceneHeaders.h"
#include "lucene++/FileUtils.h"
#include "LuceneAnalyzerFactory.h"
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

		string queryStr;
		if (!in->queryNull) {
			queryStr.assign(in->query.str, in->query.length);
		}

		string analyzerName = LuceneFTS::DEFAULT_ANALYZER_NAME;
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
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Fragment size cannot exceed 8191 characters",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		if (fragmentSize <= 0) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Fragment size must be greater than 0",
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
					ISC_STATUS statusVector[] = {
						isc_arg_gds, isc_random,
						isc_arg_string, (ISC_STATUS)"Fragment size exceeds 8191 characters",
						isc_arg_end
					};
					throw FbException(status, statusVector);
				}
				string fragment = StringUtils::toUTF8(content);
				out->fragmentNull = false;
				out->fragment.length = static_cast<ISC_USHORT>(fragment.length());
				fragment.copy(out->fragment.str, out->fragment.length);
			}
		}
		catch (LuceneException& e) {
			const string error_message = StringUtils::toUTF8(e.getError());
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

		string analyzerName = LuceneFTS::DEFAULT_ANALYZER_NAME;
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
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Fragment size cannot exceed 8191 characters",
				isc_arg_end
			};
			throw FbException(status, statusVector);
		}
		if (fragmentSize <= 0) {
			ISC_STATUS statusVector[] = {
				isc_arg_gds, isc_random,
				isc_arg_string, (ISC_STATUS)"Fragment size must be greater than 0",
				isc_arg_end
			};
			throw FbException(status, statusVector);
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
			const string error_message = StringUtils::toUTF8(e.getError());
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
				ISC_STATUS statusVector[] = {
					isc_arg_gds, isc_random,
					isc_arg_string, (ISC_STATUS)"Fragment size exceeds 8191 characters",
					isc_arg_end
				};
				throw FbException(status, statusVector);
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