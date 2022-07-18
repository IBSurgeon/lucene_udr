#ifndef ENCODE_UTILS_H
#define ENCODE_UTILS_H

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


#include <string>


using namespace std;


string string_to_hex(const string& input);

string hex_to_string(const string& input);

#endif	// ENCODE_UTILS_H