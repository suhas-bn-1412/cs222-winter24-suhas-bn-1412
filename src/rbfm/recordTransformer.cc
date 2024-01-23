#include "src/include/recordTransformer.h"

#define ATTR_OFFSET_SZ sizeof(unsigned short)
#define INT_SZ 4
#define REAL_SZ 4
#define VARCHAR_ATTR_SZ 4

uint32_t PeterDB::RecordTransformer::getSerializedDataLength(const std::vector<Attribute> &recordDescriptor, const void *recordData) {
    return serialize(recordDescriptor, nullptr);
}

inline bool isNthBitSet (unsigned char c, int n) {
    static unsigned char mask[] = {128, 64, 32, 16, 8, 4, 2, 1};
    return ((c & mask[n]) != 0);
}

// given the null flags and the attribute number, returns True if the
// attribute is defined as null in the flag
bool isAttrNull(const void *recordData, const uint16_t &attrNum, const uint16_t &totalCount) {
    uint16_t q = (attrNum-1) / totalCount;
    uint16_t r = (attrNum-1) % totalCount;

    void *flag_ptr = (void*)((char*)recordData + q);
    return isNthBitSet((unsigned char)flag_ptr, r);
}

uint32_t getVarcharAttrSize(void *data) {
    assert(nullptr != data);

    uint32_t *varcharSz = (uint32_t*)data;
    return *varcharSz;
}

uint32_t PeterDB::RecordTransformer::serialize(const std::vector<Attribute> &recordDescriptor, void *recordData,
                                               void *serializedRecord) {
    uint16_t attCount = recordDescriptor.size();
    uint16_t nullFlagSize = attrCount % 8;
    uint16_t offSetSize = attrCount * ATTR_OFFSET_SZ;
    uint32_t dataSize = nullFlagSize;

    auto attrNum = 0;
    bool isNull = false;
    void *dataPtr = recordData + nullFlagSize;

    for (auto attr : recordDescriptor) {
        attrNum++;
        isNull = isAttrNull(recordData, attrNum);

        if (!isNull) {
            switch (attr.AttrType) {
                case TypeInt:
                    dataSize += INT_SZ;
                    dataPtr = dataPtr + INT_SZ;
                    if (nullptr == serializedRecord) continue;
                case TypeReal:
                    dataSize += REAL_SZ;
                    dataPtr = dataPtr + REAL_SZ;
                    if (nullptr == serializedRecord) continue;
                case TypeVarChar:
                    uint32_t attrSize = getVarcharAttrSize(dataPtr);
                    dataSize += attrSize;

                    // we have to move the data pointer past two data
                    // one is the data which contains size of the varchar attribute
                    // other is the actual varchar data
                    dataPtr = dataPtr + (attrSize + VARCHAR_ATTR_SZ);
                    if (nullptr == serializedRecord) continue;
                default:
                    continue;
            }
        }
    }
    return dataSize;
}

void
PeterDB::RecordTransformer::deserialize(const std::vector<Attribute> &recordDescriptor, const void *serializedRecord, void* deserializedRecord) {
    //todo:
}
