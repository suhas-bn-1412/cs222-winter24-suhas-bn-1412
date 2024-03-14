#include <cmath>
#include "src/include/ValueSerializer.h"

#define NUM_BITS_PER_BYTE 8.0

namespace PeterDB {
    unsigned int ValueSerializer::computeSerializedDataLengthBytes(const std::vector<Value> &values) {
        const unsigned int nullFlagSizeBytes = computeNullFlagSizeBytes(values.size());
        const unsigned int attributeValuesLengthBytes = computeAttributeValuesLengthBytes(values);
        return nullFlagSizeBytes + attributeValuesLengthBytes;
    }

    /*
     * Serializes vector<Value> into the memory pointed to by data
     * IMPORTANT: Ensure to subsequently call destory(values)
     * to free the memory malloc'd to each value's data
     */
    void ValueSerializer::serialize(const std::vector<Value> &values, void *data) {
        assert(data != nullptr);
        unsigned int nullFlagLenBytes = computeNullFlagSizeBytes(values.size());
        computeAndAddNullFlag(values, data);
        concatAttributeValues(values, (byte *) data + nullFlagLenBytes);
    }

    /*
     * Frees up the memory malloc'd to each value's data
     */

    void ValueSerializer::destroy(const std::vector<Value>& values) {
        for (const auto &value: values) {
            assert(value.data != nullptr);
            free(value.data);
        }
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

    unsigned int ValueSerializer::getAttributeValueSize(const Value &value) {
        const AttrType attrType = value.type;
        switch (attrType) {
            case TypeInt:
                return 4;
                break;
            case TypeReal:
                return 4;
                break;
            case TypeVarChar:
                return *((uint32_t *) &value);
                break;
        }
    }

    void ValueSerializer::computeAndAddNullFlag(const std::vector<Value> &values, void *data) {
        const unsigned int nullFlagLengthBytes = computeNullFlagSizeBytes(values.size());
        memset(data, 0, nullFlagLengthBytes);

        byte *dataPtr = (byte *) data;
        unsigned int bitNum = 0;
        for (const auto &value: values) {
            if (value.data == nullptr) {
                //todo: handle null fields
                assert(1);
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
                // prefix the size
                // memmove(dest, &attrValueSize, sizeof(uint32_t));
                // dest += sizeof(uint32_t);
                memcpy(dest, value.data, attrValueSize + sizeof(uint32_t));
                dest += attrValueSize + sizeof(uint32_t);
            } else {
                memcpy(dest, value.data, attrValueSize);
                dest += attrValueSize;
            }
        }
    }
}
