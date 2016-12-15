#ifndef _MISC_H_
#define _MISC_H_

#include <chrono>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <string>

std::chrono::milliseconds get_current_timestamp();

std::string date();

std::string replace_string( std::string, const std::string&, const std::string& );

#endif /* _MISC_H_ */