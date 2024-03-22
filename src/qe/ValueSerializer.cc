#include <cmath>
#include "src/include/ValueSerializer.h"

#define NUM_BITS_PER_BYTE 8.0

namespace PeterDB {
    unsigned int ValueSerializer::computeSerializedDataLengthBytes(const std::vector<Value> &values) {
        const unsigned int nullFlagSizeBytes = computeNullFlagSizeBytes(values.size());
        const unsigned int attributeValuesLengthBytes = computeAttributeValuesLengthBytes(values);
        return nullFlagSizeBytes + attributeValuesLengthBytes;
    }

    void ValueSerializer::serialize(const std::vector<Value> &values, void *data) {
        assert(data != nullptr);
        const unsigned int nullFlagLenBytes = computeNullFlagSizeBytes(values.size());
        computeAndAddNullFlag(values, data);
        concatAttributeValues(values, (byte *) data + nullFlagLenBytes);
    }

    unsigned int ValueSerializer::computeNullFlagSizeBytes(const unsigned long numAttributes) {
        return ceil(numAttributes / NUM_BITS_PER_BYTE);
    }

    unsigned int ValueSerializer::computeAttributeValuesLengthBytes(const std::vector<Value> &values) {
        unsigned int attributeValuesLengthBytes = 0;
        for (const auto &value: values) {
            attributeValuesLengthBytes += getAttributeValueSize(value);
            if (value.type == TypeVarChar) {
                attributeValuesLengthBytes += 4;
            }
        }
        return attributeValuesLengthBytes;
    }

    uint32_t ValueSerializer::getAttributeValueSize(const Value &value) {
        const AttrType attrType = value.type;
        switch (attrType) {
            case TypeInt:
                return 4;
            case TypeReal:
                return 4;
            case TypeVarChar:
                return *((uint32_t *) value.data);
        }
        assert(0);
    }

    void ValueSerializer::computeAndAddNullFlag(const std::vector<Value> &values, void *data) {
        const unsigned int nullFlagLengthBytes = computeNullFlagSizeBytes(values.size());
        memset(data, 0, nullFlagLengthBytes);

        byte *dataPtr = (byte *) data;
        unsigned int bitNum = 0;
        for (const auto &value: values) {
            if (value.data == nullptr) {
                //todo: handle null fields
                assert(0);
            }

            bitNum++;
            if (bitNum == NUM_BITS_PER_BYTE) {
                bitNum = 0;
                dataPtr++;
            }
        }
    }

    void ValueSerializer::concatAttributeValues(const std::vector<Value> &values, byte *data) {
        byte *dest = data;
        for (const auto &value: values) {
            const uint32_t attrValueSize = getAttributeValueSize(value);
            const AttrType attrType = value.type;
            if (attrType == TypeVarChar) {
                memcpy(dest, value.data, attrValueSize + sizeof(uint32_t));
                dest += attrValueSize + sizeof(uint32_t);
            } else {
                memcpy(dest, value.data, attrValueSize);
                dest += attrValueSize;
            }
        }
    }
}
