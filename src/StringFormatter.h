#ifndef STRING_FORMATTER_H
#define STRING_FORMATTER_H

#include <string>
#include <boost/format.hpp>

using namespace std;

template<typename First, typename... Args>
    inline string string_format(const string& formatString,
        First&& firstArg, Args&&... arg)
{
    boost::format formatter{ formatString };
    boost::format* list[] = { &(formatter % firstArg), &(formatter % arg)... };
    (void)list;
    return formatter.str();
}

inline string string_format(const string& string)
{
    return string;
}

inline string string_format()
{
    BOOST_ASSERT_MSG(false, "Format may not be used without arguments");
    return {};
}

#endif	// STRING_FORMATTER_H