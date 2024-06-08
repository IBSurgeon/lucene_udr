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

#include "EncodeUtils.h"


#include <vector>
#include <algorithm>
#include <stdexcept>
#include <sstream>
#include <iomanip>

namespace {
    constexpr const char hex_digits[] = "0123456789ABCDEF";
}


std::string string_to_hex(const std::string& input)
{
    std::string output;
    output.reserve(input.length() * 2);
    for (unsigned char c : input) {
        output.push_back(hex_digits[c >> 4]);
        output.push_back(hex_digits[c & 15]);
    }
    return output;
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
        throw std::invalid_argument("not a hex digit");
}

std::string hex_to_string(const std::string& input)
{
    size_t len = input.length();
    if (len & 1) 
        throw std::invalid_argument("A hexadecimal string has an odd length");

    std::string output;
    output.reserve(len / 2);
    for (std::string::const_iterator p = input.begin(); p != input.end(); p++)
    {
        unsigned char c = hexval(*p);
        p++;
        if (p == input.end())
            throw std::invalid_argument("Incomplete last digit in hex string");
        c = (c << 4) + hexval(*p); // + takes precedence over <<
        output.push_back(c);
    }
    return output;
}
