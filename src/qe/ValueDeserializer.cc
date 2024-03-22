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

    // 0 indexed..
    // in recordData, most significant bit represents the null flag bit
    // of the attr with attrNum=0
    bool checkIfAttrNull(const void *recordData, const uint16_t &attrNum) {
        uint16_t q = (attrNum) / 8;
        uint16_t r = (attrNum) % 8;

        return (0 != (((char*)recordData)[q] & (1 << (7-r))));
    }

    void ValueDeserializer::deserializeNullFlag(const void *data, unsigned int numAttributes,
                                                std::vector<bool> &isAttrValueNull) {
        for (int fieldNum = 0; fieldNum < numAttributes; fieldNum++) {
            isAttrValueNull.push_back(checkIfAttrNull(data, fieldNum));
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

    unsigned int ValueDeserializer::calculateTupleSize(void* data, const std::vector<Attribute>& attrs) {
        unsigned totalSize = 0;

        totalSize += computeNullFlagSizeBytes(attrs.size());

        void* dataPtr = (char*)data + totalSize;

        std::vector<bool> nullFlags;
        deserializeNullFlag(data, attrs.size(), nullFlags);

        unsigned attrSize = 0;
        for (int i=0; i<attrs.size(); i++) {
            if (!nullFlags[i]) {
                attrSize = 4;
                if (TypeVarChar == attrs[i].type) {
                    attrSize += *((uint32_t*)dataPtr);
                }

                dataPtr = (char*)dataPtr + attrSize;
                totalSize += attrSize;
            }
        }

        return totalSize;
    }
}
