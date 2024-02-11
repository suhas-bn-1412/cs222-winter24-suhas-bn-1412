#ifndef _serializer_h_
#define _serializer_h_

#include <vector>
#include "src/include/attributeAndValue.h"

namespace PeterDB {
    class Serializer {
    public:
        static unsigned int computeSerializedDataLenBytes(std::vector<AttributeAndValue> *attributeAndValues);

        static void serialize(std::vector<AttributeAndValue> *attributeAndValues, void *data);
    private:

        static unsigned int computeNullFlagSizeBytes(unsigned int numFields);

        static unsigned int computeAttributeValuesLengthBytes(std::vector<AttributeAndValue> *attributeAndValues);

        static void computeAndAddNullFlag(std::vector<AttributeAndValue> *attributesAndValues, void *data);

        static void concatAttributeValues(std::vector<AttributeAndValue> *attributeAndValues, byte *data);

        static unsigned int getAttributeValueSize(const AttributeAndValue &attributeAndValue);
    };
}

#endif