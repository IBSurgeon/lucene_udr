#ifndef ENCODE_UTILS_H
#define ENCODE_UTILS_H

/**
 *  String conversion functions.
 *
 *  The original code was created by Simonov Denis
 *  for the open source project "IBSurgeon Full Text Search UDR".
 *
 *  Copyright (c) 2022 Simonov Denis <sim-mail@list.ru>
 *  and all contributors signed below.
 *
 *  All Rights Reserved.
 *  Contributor(s): ______________________________________.
**/


#include <string>

std::string string_to_hex(const std::string& input);

std::string hex_to_string(const std::string& input);

#endif	// ENCODE_UTILS_H