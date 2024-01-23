#ifndef _record_transformer_h
#define _record_transformer_h

#include <vector>
#include "src/include/rbfm.h"

namespace PeterDB {
    class RecordTransformer {
        public:
        static uint32_t getSerializedDataLength(const std::vector<Attribute> &recordDescriptor, const void *recordData);

        static uint32_t serialize(const std::vector<Attribute> &recordDescriptor, void *recordData, void *serializedRecord);

        static void deserialize(const std::vector<Attribute> &recordDescriptor, const void *serializedRecord, void* deserializedRecord);
    };
}

#endif
