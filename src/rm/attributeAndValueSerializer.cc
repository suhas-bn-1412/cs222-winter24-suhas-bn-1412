#include "src/include/attributeAndValueSerializer.h"
#include <cmath>

#define NUM_BITS_PER_BYTE 8.0

namespace PeterDB {

    unsigned int AttributeAndValueSerializer::computeSerializedDataLenBytes(std::vector<AttributeAndValue> *attributeAndValues) {
        unsigned int nullFlagSizeBytes = computeNullFlagSizeBytes(attributeAndValues->size());
        unsigned int attributeValuesLengthBytes = computeAttributeValuesLengthBytes(attributeAndValues);
        return nullFlagSizeBytes + attributeValuesLengthBytes;
    }

    void AttributeAndValueSerializer::serialize(const std::vector<AttributeAndValue> &attributeAndValues, void *data) {
        assert(data != nullptr);
        unsigned int nullFlagLenBytes = computeNullFlagSizeBytes(attributeAndValues.size());
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
            if (TypeVarChar == attributeAndValue.getAttribute().type) {
                attributeValuesLengthBytes += 4;
            }
        }
        return attributeValuesLengthBytes;
    }

    void AttributeAndValueSerializer::computeAndAddNullFlag(const std::vector<AttributeAndValue> &attributesAndValues, void *data) {
        unsigned int nullFlagLengthBytes = computeNullFlagSizeBytes(attributesAndValues.size());
        memset(data, 0, nullFlagLengthBytes);

        byte *dataPtr = (byte *) data;
        unsigned int bitNum = 0;
        for (const auto& attributeAndValue: attributesAndValues) {
            if (attributeAndValue.getValue() == nullptr) {
                assert(1);
                //todo: handle null fields, but they shud not happen, as this serializer only used internally
            }

            bitNum++;
            if (bitNum == NUM_BITS_PER_BYTE ) {
                bitNum = 0;
                dataPtr++;
            }
        }
    }

    void AttributeAndValueSerializer::concatAttributeValues(const std::vector<AttributeAndValue> &attributeAndValues, byte *data) {
        byte *dest = data;
        for (const auto& attributeAndValue: attributeAndValues) {
            uint32_t attrValueSize = getAttributeValueSize(attributeAndValue);
            AttrType attrType = attributeAndValue.getAttribute().type;
            if (attrType == TypeVarChar) {
                // prefix the size
                // memmove(dest, &attrValueSize, sizeof(uint32_t));
                // dest += sizeof(uint32_t);
                memmove(dest, attributeAndValue.getValue(), attrValueSize + sizeof(uint32_t));
                dest += attrValueSize + sizeof(uint32_t);
            } else {
                memmove(dest, attributeAndValue.getValue(), attrValueSize);
                dest += attrValueSize;
            }
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
                attributeDataSize += *( (uint32_t*) attributeAndValue.getValue() );
                break;
        }
        return attributeDataSize;
    }
}
