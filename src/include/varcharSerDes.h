#ifndef _varchar_ser_des_h_
#define _varchar_ser_des_h_

#include <cstddef>
#include <string>

typedef char byte;

namespace PeterDB {
    class VarcharSerDes {
    public:
        static size_t computeSerializedSize(std::string string);

        static void serialize(std::string string, void *data);

    private:
        static const size_t VARCHAR_SIZE_SPECIFIER_SIZE = sizeof(uint32_t);
    };
}

#endif
