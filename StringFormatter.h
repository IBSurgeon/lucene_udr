#ifndef STRING_FORMATTER_H
#define STRING_FORMATTER_H

#include <string>
#include <boost/format.hpp>
//#include <boost/locale/format.hpp>

template<typename First, typename... Args>
    inline std::string string_format(const std::string& formatString,
        First&& firstArg, Args&&... arg)
{
    boost::format formatter{ formatString };
    boost::format* list[] = { &(formatter % firstArg), &(formatter % arg)... };
    (void)list;
    return formatter.str();
}

inline std::string string_format(const std::string& string)
{
    return string;
}

inline std::string string_format()
{
    BOOST_ASSERT_MSG(false, "Format may not be used without arguments");
    return {};
}

#endif	// STRING_FORMATTER_H