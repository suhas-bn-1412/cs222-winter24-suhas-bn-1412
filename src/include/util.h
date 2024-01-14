#include <assert.h>
#include <fcntl.h>
#include <iostream>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

bool file_exists(const std::string& fileName) {
    struct stat statbuf;
    return (0==stat(fileName.c_str(), &statbuf));
}

void file_delete(const std::string& fileName) {
    if (unlink(fileName.c_str())) {
    } 
}

// Sophisticated trace function with format string and variable arguments
void trace(const char* level, const char* format, va_list& args) {
    // Determine the size of the buffer needed
    int size = vsnprintf(nullptr, 0, format, args);

    // Allocate a buffer of the appropriate size
    char* buffer = new char[size + 1];

    // Format the message into the buffer
    vsnprintf(buffer, size + 1, format, args);

    // Print the formatted message
    // currently printing it to stdout, we can
    // enhance it to print to file in future
    std::cout << level << buffer << std::endl;

    // Clean up
    delete[] buffer;
    va_end(args);
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
