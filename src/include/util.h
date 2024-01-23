#ifndef _util_h_
#define _util_h_

#include <assert.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <stdarg.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace PeterDB {

bool file_exists(const std::string& fileName);
void file_delete(const std::string& fileName);
size_t file_size(const std::string& fileName);

void trace(const char* level, const char* format, va_list& args);
void INFO(const char* format, ...);
void ERROR(const char* format, ...);
void WARNING(const char* format, ...);

}

#endif
