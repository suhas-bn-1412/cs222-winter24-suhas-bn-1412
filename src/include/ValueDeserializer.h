
#ifndef PETERDB_VALUEDESERIALIZER_H
#define PETERDB_VALUEDESERIALIZER_H

#include <vector>
#include "qe.h"

namespace PeterDB {
    class ValueDeserializer {
    public:
        static void deserialize(const void* data,
                                const std::vector<Attribute>& attributes,
                                std::vector<Value> &values);

        static unsigned int computeNullFlagSizeBytes(unsigned int size);

        static void deserializeNullFlag(const void *data,unsigned int numAttributes, std::vector<bool>& isAttrValueNull);

        static void deserializeValues(byte *data,
                                      const std::vector<Attribute>& attributes,
                                      const std::vector<bool>& isAttrNull,
                                      std::vector<Value> &values);
    };
}

#endif //PETERDB_VALUEDESERIALIZER_H
