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
#ifndef WIN32_LEAN_AND_MEAN
#include "unicode/uchar.h"
#include "unicode/ucnv.h"
#include "unicode/unistr.h"
#endif
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <string>

#ifndef WIN32_LEAN_AND_MEAN
using namespace icu;
#endif

using namespace std;



string FBStringEncoder::toUtf8(const string& source_str)
{
	// if the string is already in utf-8, then it makes no sense to re-encode it
	if (sourceCharsetInfo.codePage == 65001) {
		return source_str;
	}
#ifdef WIN32_LEAN_AND_MEAN
	return StringUtils::toUTF8(toUnicode(source_str));
#else
	const auto srclen = static_cast<int32_t>(source_str.size());
	vector<UChar> target(srclen);

	UErrorCode status = U_ZERO_ERROR;
	UConverter* conv = ucnv_open(sourceCharsetInfo.charsetName.c_str(), &status);
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
#endif
}

String FBStringEncoder::toUnicode(const string& source_str) 
{
	// if the string is already in utf-8, then it makes no sense to re-encode it
	if (sourceCharsetInfo.codePage == 65001) {
		return StringUtils::toUnicode(source_str);
	}
#ifdef WIN32_LEAN_AND_MEAN
	int res_len = MultiByteToWideChar(sourceCharsetInfo.codePage, 0, source_str.c_str(), source_str.size(), nullptr, 0);

	if (!res_len)
		return String();

	unique_ptr<wchar_t[]> pRes = make_unique<wchar_t[]>(res_len);
	wchar_t* buffer = pRes.get();

	if (!MultiByteToWideChar(sourceCharsetInfo.codePage, 0, source_str.c_str(), source_str.size(), buffer, res_len))
	{
		return String();
	}
	return String(buffer, res_len);
#else
	return StringUtils::toUnicode(toUtf8(source_str));
#endif
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
