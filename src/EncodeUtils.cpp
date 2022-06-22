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
#include "unicode/unistr.h"
#endif

#include <vector>
#include <algorithm>
#include <stdexcept>
#include <string>
#include <sstream>
#include <iomanip>

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
	const auto src_len = static_cast<int32_t>(source_str.size());
	if (src_len == 0) {
		return source_str;
    }
#ifdef WIN32_LEAN_AND_MEAN
	return Lucene::StringUtils::toUTF8(toUnicode(source_str));
#else	
	vector<UChar> target(src_len);

	UErrorCode status = U_ZERO_ERROR;
	if (!uConverter) {
		uConverter = ucnv_open(sourceCharsetInfo.charsetName.c_str(), &status);
		if (!U_SUCCESS(status))
			throw runtime_error("Cannot convert string to UTF8");
	}

	int32_t dest_len = ucnv_toUChars(uConverter, target.data(), src_len * 4, source_str.c_str(), src_len, &status);
	if (!U_SUCCESS(status))
		throw runtime_error("Cannot convert string to UTF8");

	UnicodeString ustr(target.data(), dest_len);

	string retval;
	ustr.toUTF8String(retval);

	return retval;
#endif
}

wstring FBStringEncoder::toUnicode(const string& source_str)
{
	// if the string is already in utf-8, then it makes no sense to re-encode it
	if (sourceCharsetInfo.codePage == 65001) {
		return Lucene::StringUtils::toUnicode(source_str);
	}
#ifdef WIN32_LEAN_AND_MEAN
	int src_len = static_cast<int>(source_str.size());
    int dest_len = MultiByteToWideChar(sourceCharsetInfo.codePage, 0, source_str.c_str(), src_len, nullptr, 0);

	
	if (!dest_len)
		return L"";
	
	wstring result{ L"" };
	unique_ptr<wchar_t[]> pRes = make_unique<wchar_t[]>(dest_len);
	{
		wchar_t* buffer = pRes.get();
		if (!MultiByteToWideChar(sourceCharsetInfo.codePage, 0, source_str.c_str(), src_len, buffer, dest_len))
		{
			throw runtime_error("Cannot convert string to Unicode");
		}
		result.assign(buffer, dest_len);
	}
	return result;
#else
	return Lucene::StringUtils::toUnicode(toUtf8(source_str));
#endif
}

string string_to_hex(const string& input)
{
	stringstream ss;
	ss << std::hex << std::setfill('0');
	for (size_t i = 0; i < input.size(); i++) {
		const auto ch = static_cast<unsigned char>(input[i]);
		ss << std::setw(2) << std::uppercase << static_cast<unsigned int>(ch);
	}
	return ss.str();
}

unsigned char hexval(unsigned char c)
{
	if ('0' <= c && c <= '9')
		return c - '0';
	else if ('a' <= c && c <= 'f')
		return c - 'a' + 10;
	else if ('A' <= c && c <= 'F')
		return c - 'A' + 10;
	else 
		throw invalid_argument("not a hex digit");
}

string hex_to_string(const string& input)
{
	size_t len = input.length();
	if (len & 1) 
		throw invalid_argument("A hexadecimal string has an odd length");

	string output;
	output.reserve(len / 2);
	for (string::const_iterator p = input.begin(); p != input.end(); p++)
	{
		unsigned char c = hexval(*p);
		p++;
		if (p == input.end())
			throw invalid_argument("Incomplete last digit in hex string");
		c = (c << 4) + hexval(*p); // + takes precedence over <<
		output.push_back(c);
	}
	return output;
}
