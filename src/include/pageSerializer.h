#ifndef _page_serializer_h_
#define _page_serializer_h_

#include "nonLeafPage.h"
#include "leafPage.h"

namespace PeterDB {
    class PageSerializer {
    public:
        static void toBytes(NonLeafPage &nonLeafPage, void *data);

        static void toBytes(LeafPage &leafPage, void *data);

    private:

        static void writeFreeByteCount(const unsigned int freeByteCount, const void *data);

        static void writeKeyType(const Attribute &keyAttribute, const void *data);

        static void writeIsLeafPage(const bool isLeafPage, void *data);

        static void writePageNumAndKeyPairs(NonLeafPage &nonLeafPage, const void *data);

        static void writeKeyAndRidPair(LeafPage &leafPage, void *data);

        static void writeNextPageNum(int nextPageNum, void *data);

        static void writeNumKeys(unsigned int numKeys, void *data);
    };
}

#endif
