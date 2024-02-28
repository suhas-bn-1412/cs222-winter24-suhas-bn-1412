#ifndef _page_deserializer_h_

#define _page_deserializer_h_

#include "nonLeafPage.h"
#include "leafPage.h"

namespace PeterDB {
    class PageDeserializer {
    public:
        static bool isLeafPage(const void *data);

        static void toNonLeafPage(const void *data, NonLeafPage &nonLeafPage);

        static void toLeafPage(const void *data, LeafPage &leafPage);

        static const unsigned int readFreeByteCount(const void *data);

        static const Attribute readKeyType(const void *data);

        static const unsigned int readNextPageNum(const void *data);

        static void readPageNumAndKeyPairs(const std::vector<PageNumAndKey> &pageNumAndKeyPairs, const void *data);

        static void readKeyAndRidPairs(const std::vector<RidAndKey> &keyAndRidPairs, const void *data);
    };
}

#endif