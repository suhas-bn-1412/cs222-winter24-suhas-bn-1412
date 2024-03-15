
#ifndef PETERDB_VALUESERIALIZER_H
#define PETERDB_VALUESERIALIZER_H

#include <vector>
#include "qe.h"

namespace PeterDB {
    class ValueSerializer {
    public:
        static unsigned int computeSerializedDataLengthBytes(const std::vector<Value> &values);

        /*
         * Serializes vector<Value> into the memory pointed to by data
         * IMPORTANT: Ensure to subsequently call destory(values)
         * to free the memory malloc'd to each value's data
         */
        static void serialize(const std::vector<Value> &values, void *data);

    private:
        static unsigned int computeNullFlagSizeBytes(const unsigned long numAttributes);

        static unsigned int computeAttributeValuesLengthBytes(const std::vector<Value> &values);

        static unsigned int getAttributeValueSize(const Value &value);

        static void computeAndAddNullFlag(const std::vector<Value> &values, void *data);

        static void concatAttributeValues(const std::vector<Value> &values, byte *data);
    };
}

#endif //PETERDB_VALUESERIALIZER_H
