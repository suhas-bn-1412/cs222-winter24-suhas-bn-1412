#include "src/include/recordTransformer.h"

 unsigned short
PeterDB::RecordTransformer::getSerializedDataLength(const std::vector<Attribute> &recordDescriptor, const void *recordData) {
    //todo
    return 0;
}

void PeterDB::RecordTransformer::serialize(const std::vector<Attribute> &recordDescriptor, void *recordData,
                                           void *serializedRecord) {
    //todo
}

void
PeterDB::RecordTransformer::deserialize(const std::vector<Attribute> &recordDescriptor, const void *serializedRecord, void* deserializedRecord) {
    //todo:
}
