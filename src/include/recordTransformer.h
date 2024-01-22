#include <vector>
#include "rbfm.h"

namespace PeterDB {
    class RecordTransformer {
        public:
        static unsigned short getSerializedDataLength(const std::vector<Attribute> &recordDescriptor, void *recordData);

        static void serialize(const std::vector<Attribute> &recordDescriptor, void *recordData, void *serializedRecord);

        static void *deserialize(const std::vector<Attribute> &recordDescriptor, const void *serializedRecord);
    };
}