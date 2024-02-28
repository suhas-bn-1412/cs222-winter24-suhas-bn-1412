#ifndef _page_deserializer_h_

#define _page_deserializer_h_

#include "nonLeafPage.h"
#include "leafPage.h"

namespace PeterDB {
    class PageDeserializer {
    public:
        bool isLeafPage(const void *data);

        void toNonLeafPage(const void *data, NonLeafPage &nonLeafPage);

        void toLeafPage(const void *data, LeafPage &leafPage);
    };
}

#endif