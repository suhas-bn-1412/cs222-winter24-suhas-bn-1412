#include "src/include/attributeAndValueSerializer.h"
#include <cmath>

#define NUM_BITS_PER_BYTE 8

namespace PeterDB {

    unsigned int AttributeAndValueSerializer::computeSerializedDataLenBytes(std::vector<AttributeAndValue> *attributeAndValues) {
        unsigned int nullFlagSizeBytes = computeNullFlagSizeBytes(attributeAndValues->size());
        unsigned int attributeValuesLengthBytes = computeAttributeValuesLengthBytes(attributeAndValues);
        return nullFlagSizeBytes + attributeValuesLengthBytes;
    }

    void AttributeAndValueSerializer::serialize(std::vector<AttributeAndValue> *attributeAndValues, void *data) {
        assert(data != nullptr);
        unsigned int nullFlagLenBytes = computeNullFlagSizeBytes(attributeAndValues->size());
        computeAndAddNullFlag(attributeAndValues, data);
        concatAttributeValues(attributeAndValues, (byte *) data + nullFlagLenBytes);
    }

    unsigned int AttributeAndValueSerializer::computeNullFlagSizeBytes(unsigned int numFields) {
        return ceil(numFields / NUM_BITS_PER_BYTE);
    }

    unsigned int AttributeAndValueSerializer::computeAttributeValuesLengthBytes(std::vector<AttributeAndValue> *attributeAndValues) {
        unsigned int attributeValuesLengthBytes = 0;
        for (const auto& attributeAndValue: *attributeAndValues) {
            attributeValuesLengthBytes += getAttributeValueSize(attributeAndValue);
        }
        return attributeValuesLengthBytes;
    }

    void AttributeAndValueSerializer::computeAndAddNullFlag(std::vector<AttributeAndValue> *attributesAndValues, void *data) {
        unsigned int nullFlagLengthBytes = computeNullFlagSizeBytes(attributesAndValues->size());
        memset(data, 0, nullFlagLengthBytes);
        //todo: handle null fields
    }

    void AttributeAndValueSerializer::concatAttributeValues(std::vector<AttributeAndValue> *attributeAndValues, byte *data) {
        byte *dest = data;
        for (const auto &attributeAndValue: *attributeAndValues) {
            uint32_t attrValueSize = getAttributeValueSize(attributeAndValue);
            AttrType attrType = attributeAndValue.getAttribute().type;
            if (attrType == TypeVarChar) {
                // prefix the size
                memcpy(dest, &attrValueSize, sizeof(uint32_t));
                dest += sizeof(uint32_t);
            }
            memcpy(dest, attributeAndValue.getValue(), attrValueSize);
            dest += attrValueSize;
        }
    }

    unsigned int AttributeAndValueSerializer::getAttributeValueSize(const AttributeAndValue &attributeAndValue) {
        unsigned int attributeDataSize = 0;
        AttrType attrType = attributeAndValue.getAttribute().type;
        switch (attrType) {
            //todo: refactor
            case TypeInt:
                attributeDataSize += 4;
                break;
            case TypeReal:
                attributeDataSize += 4;
                break;
            case TypeVarChar:
                uint32_t varcharValSize;
                memcpy(&varcharValSize, attributeAndValue.getValue(), sizeof(varcharValSize));
                attributeDataSize += sizeof(varcharValSize);
                attributeDataSize += varcharValSize;
                break;
        }
        return attributeDataSize;
    }
}
