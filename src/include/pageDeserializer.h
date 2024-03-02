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

        static unsigned int readFreeByteCount(const void *data);

        static Attribute readKeyType(const void *data);

        static int readNextPageNum(const void *data);

        static unsigned int readNumKeys(const void *data);

        static void readPageNumAndKeyPairs(NonLeafPage &nonLeafPage, const void *data, unsigned int numKeys);

        static void readKeyAndRidPairs(LeafPage &leafPage, const void *data, unsigned int numKeys);
    };
}

#endif