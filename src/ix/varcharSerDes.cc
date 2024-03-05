#include <cstring>
#include "src/include/varcharSerDes.h"

namespace PeterDB {

    size_t VarcharSerDes::computeSerializedSize(const std::string& string) {
        return VARCHAR_SIZE_SPECIFIER_SIZE + string.length();
    }

    void VarcharSerDes::serialize(const std::string& string, void* data) {
        byte *destPtr = (byte *) data;

        uint32_t stringLen = string.length();
        memmove(destPtr, &stringLen, VARCHAR_SIZE_SPECIFIER_SIZE);
        destPtr += VARCHAR_SIZE_SPECIFIER_SIZE;

        memmove(destPtr, string.c_str(), stringLen);
    }

    std::string VarcharSerDes::deserialize(const void *data) {
        uint32_t stringLength;
        byte *readPtr = (byte *) data;
        memcpy(&stringLength, (void *) readPtr, VARCHAR_SIZE_SPECIFIER_SIZE);
        readPtr += VARCHAR_SIZE_SPECIFIER_SIZE;

        std::string value;
        value.resize(stringLength);
        memcpy((void *) value.data(), readPtr, stringLength);
        return value;
    }

    size_t VarcharSerDes::computeDeserializedSize(const void *data) {
        unsigned int stringSize;
        memcpy(&stringSize, data, VARCHAR_SIZE_SPECIFIER_SIZE);
        return stringSize;
    }
}
