#ifndef _record_transformer_h
#define _record_transformer_h

#include <vector>
#include "src/include/rbfm.h"

#define INT_SZ 4
#define REAL_SZ 4
#define VARCHAR_ATTR_LEN_SZ 4

namespace PeterDB {

    struct ProjectedAttrInfo {
        Attribute attrInfo;
        uint16_t attrStart;
        uint16_t attrEnd;
        bool isNull;
    };

    class RecordTransformer {
        public:
        static uint32_t serialize(const std::vector<Attribute> &recordDescriptor,
                                  const void *recordData,
                                  void *serializedRecord);

        static void deserialize(const std::vector<Attribute> &recordDescriptor,
                                const std::vector<std::string> &attributeNames,
                                const void *serializedRecord,
                                void *recordData);

        static void print(const std::vector<Attribute> &recordDescriptor,
                          const void *recordData,
                          std::ostream &stream);
    };
}

#endif
