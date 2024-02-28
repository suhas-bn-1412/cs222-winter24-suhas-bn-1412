#include "src/include/pageDeserializer.h"

typedef char byte;

namespace PeterDB {

    bool PageDeserializer::isLeafPage(const void *data) {
        return false;
    }

    void PageDeserializer::toNonLeafPage(const void *data, NonLeafPage &nonLeafPage) {

    }

    void PageDeserializer::toLeafPage(const void *data, LeafPage &leafPage) {

    }
}