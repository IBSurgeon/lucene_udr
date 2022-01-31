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
	char buffer[MAX_SEGMENT_SIZE + 1];
	unsigned int l;
	while (!eof) {
		switch (blob->getSegment(status, MAX_SEGMENT_SIZE, &buffer[0], &l))
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
	while (str_len > 0) {
		char buffer[MAX_SEGMENT_SIZE + 1];
		size_t len = std::min(str_len, MAX_SEGMENT_SIZE);
		memcpy(buffer, str.data() + offset, len);
		blob->putSegment(status, len, buffer);
		offset += len;
		str_len -= len;
	}
}

#endif	// FB_BLOB_UTILS_H