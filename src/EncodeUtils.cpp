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


using namespace std;


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
