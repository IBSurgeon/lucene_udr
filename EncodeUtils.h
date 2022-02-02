#ifndef ENCODE_UTILS_H
#define ENCODE_UTILS_H

#include <string>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include "charsets.h"
#include "unicode/uchar.h"
#include "unicode/ucnv.h"
#include "unicode/unistr.h"

using namespace icu;

struct FBCharsetInfo {
	unsigned charsetID;
	std::string charsetName; 
	std::string icuCharsetName;
	unsigned codePage;
};


static const FBCharsetInfo FBCharsetMap[] = {
	{CS_NONE, "NONE", "", 0 /* CP_ACP */},
	{CS_BINARY, "OCTETS", "", 0 /* CP_ACP */},
	{CS_ASCII, "ASCII", "", 0 /* CP_ACP */},
	{CS_UNICODE_FSS, "UNICODE_FSS", "utf-8", 65001 /* CP_UTF8 */},
	{CS_UTF8, "UTF8", "utf-8", 65001 /* CP_UTF8 */},
	{CS_SJIS, "SJIS", "cp932", 932},
	{CS_EUCJ, "EUCJ", "ibm-1350", 932},
	{CS_DOS_737, "DOS737", "cp737", 737},
	{CS_DOS_437, "DOS437", "cp437", 437},
	{CS_DOS_850, "DOS850", "cp850", 850},
	{CS_DOS_865, "DOS865", "cp865", 865},
	{CS_DOS_860, "DOS860", "cp860", 860},
	{CS_DOS_863, "DOS863", "cp863", 863},
	{CS_DOS_775,  "DOS775", "cp775", 775},
	{CS_DOS_858, "DOS858", "cp858", 858},
	{CS_DOS_862,  "DOS862", "cp862", 862},
	{CS_DOS_864,  "DOS864", "cp864",  864},
	{CS_ISO8859_1, "ISO8859_1", "iso-8859-1",  28591},
	{CS_ISO8859_2, "ISO8859_2", "iso-8859-2", 28592},
	{CS_ISO8859_3, "ISO8859_3", "iso-8859-3",  28593},
	{CS_ISO8859_4, "ISO8859_4", "iso-8859-4", 28594},
	{CS_ISO8859_5, "ISO8859_5", "iso-8859-5", 28595},
	{CS_ISO8859_6, "ISO8859_6", "iso-8859-6", 28596},
	{CS_ISO8859_7, "ISO8859_7", "iso-8859-7", 28597},
	{CS_ISO8859_8, "ISO8859_8", "iso-8859-8", 28598},
	{CS_ISO8859_9, "ISO8859_9", "iso-8859-9", 28599},
	{CS_ISO8859_13, "ISO8859_13", "iso-8859-13", 28603},
	{CS_KSC5601, "KSC_5601", "windows-949", 949},
	{CS_DOS_852, "DOS852", "cp852",  852},
	{CS_DOS_857, "DOS857", "cp857", 857},
	{CS_DOS_861, "DOS861", "cp861", 861},
	{CS_DOS_866, "DOS866", "cp866", 866},
	{CS_DOS_869, "DOS869", "cp869", 869},
	{CS_CYRL, "CYRL", "windows-1251", 1251},
	{CS_WIN1250, "WIN1250", "windows-1250", 1250},
	{CS_WIN1251, "WIN1251", "windows-1251", 1251},
	{CS_WIN1252, "WIN1252", "windows-1252", 1252},
	{CS_WIN1253, "WIN1253", "windows-1253", 1253},
	{CS_WIN1254, "WIN1254", "windows-1254", 1254},
	{CS_BIG5, "BIG_5", "windows-950", 950},
	{CS_GB2312, "GB_2312", "ibm-5478", 936},
	{CS_WIN1255, "WIN1255", "windows-1255", 1255},
	{CS_WIN1256, "WIN1256", "windows-1256", 1256},
	{CS_WIN1257, "WIN1257", "windows-1257", 1257},
	{CS_KOI8R, "KOI8R", "KOI8-R", 20866},
	{CS_KOI8U, "KOI8U", "KOI8-U", 21866},
	{CS_WIN1258, "WIN1258", "windows-1258", 1258},
	{CS_TIS620, "TIS620", "TIS-620", 874},
	{CS_GBK, "GBK", "GBK", 936},
	{CS_CP943C, "CP943C", "ibm-943", 943},
	{CS_GB18030, "GB18030", "windows-54936", 54936}
};

std::string getICICharset(unsigned charset) {
	for (int i = 0; i < sizeof(FBCharsetMap); i++) {
		if (FBCharsetMap[i].charsetID == charset)
			return std::string(FBCharsetMap[i].icuCharsetName);
	}
	return "";
}

std::string getICICharset(const char* charset) {
	std::string fbCharset(charset);
	for (int i = 0; i < sizeof(FBCharsetMap); i++) {
		if (FBCharsetMap[i].charsetName == fbCharset)
			return std::string(FBCharsetMap[i].icuCharsetName);
	}
	return "";
}

std::string to_utf8(const std::string& source_str, const std::string& charset)
{
	// если стока уже в utf-8, то нет смысла её перекодировать
	if (charset == "utf-8") {
		return source_str;
	}
	const std::string::size_type srclen = source_str.size();
	std::vector<UChar> target(srclen);

	UErrorCode status = U_ZERO_ERROR;
	UConverter* conv = ucnv_open(charset.c_str(), &status);
	if (!U_SUCCESS(status))
		return std::string();

	int32_t len = ucnv_toUChars(conv, target.data(), srclen, source_str.c_str(), srclen, &status);
	if (!U_SUCCESS(status))
		return std::string();

	ucnv_close(conv);

	UnicodeString ustr(target.data(), len);

	std::string retval;
	ustr.toUTF8String(retval);

	return retval;
}

std::string string_to_hex(const std::string& input)
{
	static const char* const lut = "0123456789ABCDEF";
	size_t len = input.length();

	std::string output;
	output.reserve(2 * len);
	for (size_t i = 0; i < len; ++i)
	{
		const unsigned char c = input[i];
		output.push_back(lut[c >> 4]);
		output.push_back(lut[c & 15]);
	}
	return output;
}

std::string hex_to_string(const std::string& input)
{
	static const char* const lut = "0123456789ABCDEF";
	size_t len = input.length();
	if (len & 1) throw std::invalid_argument("odd length");

	std::string output;
	output.reserve(len / 2);
	for (size_t i = 0; i < len; i += 2)
	{
		char a = input[i];
		const char* p = std::lower_bound(lut, lut + 16, a);
		if (*p != a) throw std::invalid_argument("not a hex digit");

		char b = input[i + 1];
		const char* q = std::lower_bound(lut, lut + 16, b);
		if (*q != b) throw std::invalid_argument("not a hex digit");

		output.push_back(((p - lut) << 4) | (q - lut));
	}
	return output;
}

#endif	// ENCODE_UTILS_H