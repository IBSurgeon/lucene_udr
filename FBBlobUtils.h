#ifndef FB_BLOB_UTILS_H
#define FB_BLOB_UTILS_H

#include "firebird/UdrCppEngine.h"
#include <string>
#include <sstream>

using namespace std;
using namespace Firebird;

const size_t MAX_SEGMENT_SIZE = 65535;

template <class StatusType> string blob_get_string(StatusType* status, IBlob* blob)
{
	std::stringstream ss("");
	bool eof = false;
	auto b = make_unique<char[]>(MAX_SEGMENT_SIZE + 1);
	char* buffer = b.get();
	unsigned int l;
	while (!eof) {
		switch (blob->getSegment(status, MAX_SEGMENT_SIZE, buffer, &l))
		{
		case IStatus::RESULT_OK:
		case IStatus::RESULT_SEGMENT:
			ss.write(buffer, l);
			continue;
		default:
			eof = true;
			break;
		}
	}
	return ss.str();
}

template <class StatusType> void blob_set_string(StatusType* status, IBlob* blob, const string str)
{
	size_t str_len = str.length();
	size_t offset = 0;
	auto b = make_unique<char[]>(MAX_SEGMENT_SIZE + 1);
	char* buffer = b.get();
	while (str_len > 0) {
		size_t len = std::min(str_len, MAX_SEGMENT_SIZE);
		memset(buffer, 0, MAX_SEGMENT_SIZE + 1);
		memcpy(buffer, str.data() + offset, len);
		blob->putSegment(status, len, buffer);
		offset += len;
		str_len -= len;
	}
}

#endif	// FB_BLOB_UTILS_H