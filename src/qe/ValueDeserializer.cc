#include <cmath>
#include "src/include/ValueDeserializer.h"

#define NUM_BITS_PER_BYTE 8.0

namespace PeterDB {
    void ValueDeserializer::deserialize(const void *data, const std::vector<Attribute> &attributes,
                                        std::vector<Value> &values) {
        std::vector<bool> isAttrValueNull;
        deserializeNullFlag(data, attributes.size(), isAttrValueNull);
        const unsigned int nullFlagSizeBytes = computeNullFlagSizeBytes(attributes.size());
        deserializeValues((byte *) data + nullFlagSizeBytes, attributes, isAttrValueNull, values);
    }

    void ValueDeserializer::deserializeNullFlag(const void *data, unsigned int numAttributes,
                                                std::vector<bool> &isAttrValueNull) {
        for (int fieldNum = 0; fieldNum < numAttributes; ++fieldNum) {
            //todo
            isAttrValueNull.push_back(false);
        }
    }

    void ValueDeserializer::deserializeValues(byte *data,
                                              const std::vector<Attribute> &attributes,
                                              const std::vector<bool> &isAttrNull,
                                              std::vector<Value> &values) {
        const unsigned int numAttributes = attributes.size();
        uint32_t attributeValueSize;
        for (int fieldNum = 0; fieldNum < numAttributes; ++fieldNum) {
            const AttrType &attributeType = attributes.at(fieldNum).type;
            Value value;
            value.type = attributeType;

            if (isAttrNull.at(fieldNum)) {
                value.data = nullptr;
            } else {
                attributeValueSize = 4;
                if (attributeType == TypeVarChar) {
                    const uint32_t stringLength = *((uint32_t*)data);
                    attributeValueSize += stringLength;
                }

                value.data = malloc(attributeValueSize);
                assert(value.data != nullptr);
                memcpy(value.data, data, attributeValueSize);
            }
            values.push_back(value);
            data += attributeValueSize;
        }
    }

    unsigned int ValueDeserializer::computeNullFlagSizeBytes(unsigned int numAttributes) {
        return ceil(numAttributes / NUM_BITS_PER_BYTE);
    }
}
