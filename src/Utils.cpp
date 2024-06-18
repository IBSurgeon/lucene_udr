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


#include "Utils.h"

namespace {

    constexpr const char* WHITESPACE = " \n\r\t\f\v";

}

std::string ltrim(const std::string& s)
{
    size_t start = s.find_first_not_of(WHITESPACE);
    return (start == std::string::npos) ? "" : s.substr(start);
}

std::string rtrim(const std::string& s)
{
    size_t end = s.find_last_not_of(WHITESPACE);
    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

std::string trim(const std::string& s)
{
    return rtrim(ltrim(s));
}


std::string_view ltrim(std::string_view s)
{
    size_t start = s.find_first_not_of(WHITESPACE);
    return (start == std::string_view::npos) ? "" : s.substr(start);
}

std::string_view rtrim(std::string_view s)
{
    size_t end = s.find_last_not_of(WHITESPACE);
    return (end == std::string_view::npos) ? "" : s.substr(0, end + 1);
}

std::string_view trim(std::string_view s)
{
    return rtrim(ltrim(s));
}
