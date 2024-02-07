#include "src/include/recordTransformer.h"

#include <assert.h>
#include <string.h>
#include <sstream>

#define ATTR_COUNT_FIELD_SZ sizeof(uint16_t)
#define ATTR_OFFSET_SZ sizeof(uint16_t)

// given the null flags and the attribute number, returns True if the
// attribute is defined as null in the flag
bool isAttrNull(const void *recordData, const uint16_t &attrNum, const uint16_t &totalCount) {
    uint16_t q = (attrNum-1) / 8;
    uint16_t r = (attrNum-1) % 8;

    return (0 != (((char*)recordData)[q] & (1 << (7-r))));
}

uint32_t getVarcharAttrSize(const void *data) {
    assert(nullptr != data);

    return *((const uint32_t*)data);
}

void writeRecordMetadata(void *serializedRecord, uint16_t &attrCount,
                         const void *unserializedRecordData, uint16_t &nullFlagSize,
                         uint16_t *attrOffsetsInRecord, uint16_t offsetSzInRecord) {
    // write total number of attributes in the record
    memmove(serializedRecord, &attrCount, ATTR_COUNT_FIELD_SZ);

    // write the nullflags of all the attributes
    memmove((void*)((char*)serializedRecord + ATTR_COUNT_FIELD_SZ),
            unserializedRecordData, nullFlagSize);

    // write the offset details of all the attributes
    memmove((void*)((char*)serializedRecord + ATTR_COUNT_FIELD_SZ + nullFlagSize),
            (void*)attrOffsetsInRecord, offsetSzInRecord);

    return;
}

void getNullFlagsFromBoolean(const std::vector<bool>& booleanVector, char *nullFlagsData, unsigned nullFlagsBytes) {
    unsigned sz = booleanVector.size();
    assert(sz <= 8*nullFlagsBytes);

    for (unsigned i=0; i<sz; i++) {
        size_t byteIndex = i / 8;
        size_t bitIndex = 7 - (i % 8);
        // Check if the index is within the char array bounds
        if (byteIndex < sz) {
            // If the bool is true, set the corresponding bit in the char array
            if (booleanVector[i])
                nullFlagsData[byteIndex] |= (1 << bitIndex);
            // If the bool is false, clear the corresponding bit in the char array
            else
                nullFlagsData[byteIndex] &= ~(1 << bitIndex);
        }
    }
}

uint32_t PeterDB::RecordTransformer::serialize(const std::vector<Attribute> &recordDescriptor,
                                               const void *recordData,
                                               void *serializedRecord) {
    uint16_t attrCount = recordDescriptor.size();
    uint32_t serializedDataSz = 0;

    uint16_t nullFlagSize = (attrCount + 7) / 8;
    uint16_t offsetSzInRecord = attrCount * ATTR_OFFSET_SZ;
    serializedDataSz += (ATTR_OFFSET_SZ + nullFlagSize + offsetSzInRecord);

    uint16_t *attrOffsetsInRecord = (uint16_t*)malloc(offsetSzInRecord);
    assert(nullptr != attrOffsetsInRecord);
    memset(attrOffsetsInRecord, 0, offsetSzInRecord);

    bool serializeData = false;
    if (nullptr != serializedRecord)
        serializeData = true;
    /*
     * if we are serializing the data into the memory
     * allocated by the caller, then move the serializedDataPtr
     * by nullflagSize + offsetSzInRecord and the start copying
     * the data into the serialized byte array
     */
    void *serializedDataPtr = nullptr;
    if (serializeData)
        serializedDataPtr = (void *)((char*)serializedRecord + serializedDataSz);

    auto currAttr = 0;
    uint32_t attrSize = 0;
    bool isNull = false;
    void *dataPtr = (void*)((char*)recordData + nullFlagSize);

    for (auto attr : recordDescriptor) {
        currAttr++;

        isNull = isAttrNull(recordData, currAttr, attrCount);
        if (!isNull) {
            switch (attr.type) {
                case TypeInt:
                    if (serializeData) {
                        memmove(serializedDataPtr, dataPtr, INT_SZ);
                        serializedDataPtr = (void*)((char*)serializedDataPtr + INT_SZ);
                    }

                    serializedDataSz += INT_SZ;
                    dataPtr = (void*)((char*)dataPtr + INT_SZ);

                    break;

                case TypeReal:
                    if (serializeData) {
                        memmove(serializedDataPtr, dataPtr, REAL_SZ);
                        serializedDataPtr = (void*)((char*)serializedDataPtr + REAL_SZ);
                    }

                    serializedDataSz += REAL_SZ;
                    dataPtr = (void*)((char*)dataPtr + REAL_SZ);

                    break;

                case TypeVarChar:
                    attrSize = *((uint32_t*)dataPtr);
                    dataPtr = (void*)((char*)dataPtr + VARCHAR_ATTR_LEN_SZ);

                    if (serializeData) {
                        memmove(serializedDataPtr, dataPtr, attrSize);
                        serializedDataPtr = (void*)((char*)serializedDataPtr + attrSize);
                    }

                    // we have to move the data pointer past two data
                    // one is the data which contains size of the varchar attribute
                    // other is the actual varchar data
                    //
                    // but serializedDataSize only moves by how much
                    // ever data is coppied to the serialized data
                    serializedDataSz += attrSize;
                    dataPtr = (void*)((char*)dataPtr + attrSize);

                    break;

                default:
                    continue;
            }
        }

        attrOffsetsInRecord[currAttr-1] = serializedDataSz;
    }

    if (serializeData) {
        /* copy the record metadata to the starting of
         * the record. i,e number of attr, their null flags,
         * and their offsets
         */
        writeRecordMetadata(serializedRecord, attrCount,
                            recordData, nullFlagSize,
                            attrOffsetsInRecord, offsetSzInRecord);
    }

    free(attrOffsetsInRecord);
    return serializedDataSz;
}

void PeterDB::RecordTransformer::deserialize(const std::vector<Attribute> &recordDescriptor,
                                             const std::vector<std::string> &attributeNames,
                                             const void *serializedRecord,
                                             void *recordData) {
    uint16_t attrCount = recordDescriptor.size();
    uint16_t attrCountInSerializedData = *((const uint16_t*)serializedRecord);

    assert(attrCountInSerializedData == attrCount);

    uint16_t nullFlagSize = (attrCount + 7) / 8;
    uint16_t offsetSzInRecord = attrCount * ATTR_OFFSET_SZ;

    const uint16_t *attrOffsetData = nullptr;
    attrOffsetData = (const uint16_t*)((const char*)serializedRecord + (ATTR_COUNT_FIELD_SZ + nullFlagSize));

    uint16_t projectedAttributeCount = attributeNames.size();
    uint16_t projectedAttrsNullFlagSize = (projectedAttributeCount + 7) / 8;
    std::vector<bool> nullFlagsBool;

    // Read the nullflags
    void *dataPtr = recordData;

    // go past the space required to add nullflags for projected attributes
    dataPtr = (void*)((char*)dataPtr + nullFlagSize);

    auto currAttr = 0;
    uint32_t attrStart = ATTR_COUNT_FIELD_SZ + nullFlagSize + offsetSzInRecord;
    uint32_t attrEnd = attrStart;
    uint32_t attrSize = 0;

    for (auto attr : recordDescriptor) {
        currAttr++;

        // Read null flag and offset
        bool isNull = isAttrNull(recordData, currAttr, attrCount);
        attrEnd = attrOffsetData[currAttr-1];

        // only if the attribute name is present in the list of projected
        // attributes, only then write that attribute into the data
        bool projectAttr = (attributeNames.end() != std::find(attributeNames.begin(), attributeNames.end(), attr.name));

        if (projectAttr) nullFlagsBool.emplace_back(isNull);

        if (projectAttr && !isNull) {

            switch (attr.type) {
                case TypeInt:
                    assert(INT_SZ == (attrEnd-attrStart));

                    memmove(dataPtr, (const void*)((const char*)serializedRecord + attrStart), INT_SZ);
                    dataPtr = (void*)((char*)dataPtr + INT_SZ);

                    break;

                case TypeReal:
                    assert(REAL_SZ == (attrEnd-attrStart));

                    memmove(dataPtr, (const void*)((const char*)serializedRecord + attrStart), REAL_SZ);
                    dataPtr = (void*)((char*)dataPtr + REAL_SZ);

                    break;

                case TypeVarChar:
                    attrSize = attrEnd - attrStart;

                    memmove(dataPtr, &attrSize, VARCHAR_ATTR_LEN_SZ);
                    dataPtr = (void*)((char*)dataPtr + VARCHAR_ATTR_LEN_SZ);

                    memmove(dataPtr, (const void*)((const char*)serializedRecord + attrStart), attrSize);
                    dataPtr = (void*)((char*)dataPtr + attrSize);

                    break;

                default:
                    continue;
            }
        }
        attrStart = attrEnd;
    }

    char *nullFlagsData = (char*)malloc(projectedAttrsNullFlagSize);
    assert(nullptr != nullFlagsData);
    memset((void*) nullFlagsData, 0, projectedAttrsNullFlagSize);

    getNullFlagsFromBoolean(nullFlagsBool, nullFlagsData, projectedAttrsNullFlagSize);

    memcpy(recordData, (void*) nullFlagsData, projectedAttrsNullFlagSize);
}

void PeterDB::RecordTransformer::print(const std::vector<Attribute> &recordDescriptor,
                                       const void *recordData,
                                       std::ostream &out) {

    std::stringstream stream;
    uint16_t attrCount = recordDescriptor.size();
    uint16_t nullFlagSize = (attrCount + 7) / 8;


    auto currAttr = 0;
    uint32_t attrSize = 0;
    bool isNull = false;
    void *dataPtr = (void*)((char*)recordData + nullFlagSize);

    for (auto attr : recordDescriptor) {
        currAttr++;

        stream << attr.name << ": ";

        isNull = isAttrNull(recordData, currAttr, attrCount);
        if (!isNull) {
            switch (attr.type) {
                case TypeInt:
                    stream << *((int*)dataPtr);
                    dataPtr = (void*)((char*)dataPtr + INT_SZ);
                    
                    break;

                case TypeReal:
                    stream << *((float*)dataPtr);
                    dataPtr = (void*)((char*)dataPtr + REAL_SZ);

                    break;

                case TypeVarChar:
                    attrSize = *((uint32_t*)dataPtr);
                    if (0 == attrSize) {
                        stream << "";
                    } else {
                        stream.write((const char*)((char*)dataPtr + VARCHAR_ATTR_LEN_SZ), attrSize);
                    }
                    dataPtr = (void*)((char*)dataPtr + (attrSize + VARCHAR_ATTR_LEN_SZ));

                    break;

                default:
                    continue;
            }
        }
        else {
            stream << "NULL";
        }
        stream << ", ";
    }

    std::string result = stream.str();
    if (!result.empty()) {
        result = result.substr(0, result.size()-2);
        out << result;
    }
}
