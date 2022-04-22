/**
 *  String conversion functions.
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

#include "EncodeUtils.h"
#include "unicode/uchar.h"
#include "unicode/ucnv.h"
#include "unicode/unistr.h"
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <string>

using namespace icu;
using namespace std;

string getICICharset(const unsigned charset) {
	for (unsigned int i = 0; i < sizeof(FBCharsetMap); i++) {
		if (FBCharsetMap[i].charsetID == charset)
			return string(FBCharsetMap[i].icuCharsetName);
	}
	return "";
}

string getICICharset(const char* charset) {
	string fbCharset(charset);
	for (unsigned int i = 0; i < sizeof(FBCharsetMap); i++) {
		if (FBCharsetMap[i].charsetName == fbCharset)
			return string(FBCharsetMap[i].icuCharsetName);
	}
	return "";
}

string to_utf8(const string& source_str, const string& charset)
{
	// if the string is already in utf-8, then it makes no sense to re-encode it
	if (charset == "utf-8") {
		return source_str;
	}
	const auto srclen = static_cast<int32_t>(source_str.size());
	vector<UChar> target(srclen);

	UErrorCode status = U_ZERO_ERROR;
	UConverter* conv = ucnv_open(charset.c_str(), &status);
	if (!U_SUCCESS(status))
		return string();

	int32_t len = ucnv_toUChars(conv, target.data(), srclen, source_str.c_str(), srclen, &status);
	if (!U_SUCCESS(status))
		return string();

	ucnv_close(conv);

	UnicodeString ustr(target.data(), len);

	string retval;
	ustr.toUTF8String(retval);

	return retval;
}

string string_to_hex(const string& input)
{
	static const char* const lut = "0123456789ABCDEF";
	size_t len = input.length();

	string output;
	output.reserve(2 * len);
	for (size_t i = 0; i < len; ++i)
	{
		const unsigned char c = input[i];
		output.push_back(lut[c >> 4]);
		output.push_back(lut[c & 15]);
	}
	return output;
}

string hex_to_string(const string& input)
{
	static const char* const lut = "0123456789ABCDEF";
	size_t len = input.length();
	if (len & 1) throw invalid_argument("odd length");

	string output;
	output.reserve(len / 2);
	for (size_t i = 0; i < len; i += 2)
	{
		char a = input[i];
		const char* p = lower_bound(lut, lut + 16, a);
		if (*p != a) throw invalid_argument("not a hex digit");

		char b = input[i + 1];
		const char* q = lower_bound(lut, lut + 16, b);
		if (*q != b) throw invalid_argument("not a hex digit");

		output.push_back(((p - lut) << 4) | (q - lut));
	}
	return output;
}
