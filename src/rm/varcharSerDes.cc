#include <cstring>
#include "src/include/varcharSerDes.h"

namespace PeterDB {

    //todo: refactor to use string reference/ ptr instead of copy
    size_t VarcharSerDes::computeSerializedSize(std::string string) {
        return VARCHAR_SIZE_SPECIFIER_SIZE + string.length();
    }

    void VarcharSerDes::serialize(std::string string, void* data) {
        byte *destPtr = (byte *) data;

        uint32_t stringLen = string.length();
        memmove(destPtr, &stringLen, VARCHAR_SIZE_SPECIFIER_SIZE);
        destPtr += VARCHAR_SIZE_SPECIFIER_SIZE;

        memmove(destPtr, string.c_str(), stringLen);
    }
}
