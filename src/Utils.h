#ifndef COMMON_UTILS_H
#define COMMON_UTILS_H

/**
 *  Common utils.
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
#include <string_view>

std::string ltrim(const std::string& s);

std::string rtrim(const std::string& s);

std::string trim(const std::string& s);

std::string_view ltrim(std::string_view s);

std::string_view rtrim(std::string_view s);

std::string_view trim(std::string_view s);

#endif
