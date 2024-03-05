#ifndef _varchar_ser_des_h_
#define _varchar_ser_des_h_

#include <cstddef>
#include <string>

typedef char byte;

namespace PeterDB {
    class VarcharSerDes {
    public:
        static size_t computeSerializedSize(const std::string& string);

        static size_t computeDeserializedSize(const void *data);

        static void serialize(const std::string &string, void *data);

        static std::string deserialize(void *data);

    private:
        static const size_t VARCHAR_SIZE_SPECIFIER_SIZE = sizeof(uint32_t);
    };
}

#endif
