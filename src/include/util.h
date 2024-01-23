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

bool file_exists(const std::string& fileName) {
    struct stat statbuf;
    return (0==stat(fileName.c_str(), &statbuf));
}

void file_delete(const std::string& fileName) {
    remove(fileName.c_str());
}

size_t file_size(const std::string& fileName) {
    std::ifstream in(fileName.c_str(),
                     std::ifstream::in | std::ifstream::binary);
    in.seekg(0, std::ifstream::end);
    return in.tellg();
}

// Sophisticated trace function with format string and variable arguments
void trace(const char* level, const char* format, va_list& args) {
    auto len1 = strlen(level);
    auto len2 = strlen(format);

    char* logstr = new char[len1 + len2 + 2];
    memcpy(logstr, level, len1);
    memcpy(logstr+len1, format, len2);

    logstr[len1+len2] = '\n';
    logstr[len1+len2+1] = '\0';
    vfprintf(stderr, logstr, args);
}

void INFO(const char* format, ...) {
    va_list args;
    va_start(args, format);
    trace("INFO   ", format, args);
    va_end(args);
}

void ERROR(const char* format, ...) {
    va_list args;
    va_start(args, format);
    trace("ERROR  ", format, args);
    va_end(args);
}

void WARNING(const char* format, ...) {
    va_list args;
    va_start(args, format);
    trace("WARNING", format, args);
    va_end(args);
}

#endif
