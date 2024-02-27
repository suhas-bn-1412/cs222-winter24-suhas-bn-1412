#ifndef _page_serializer_h_
#define _page_serializer_h_

#include "nonLeafPage.h"
#include "leafPage.h"

namespace PeterDB {
    class PageSerializer {
    public:
        void toBytes(const NonLeafPage &nonLeafPage, void *data);

        void toBytes(const LeafPage &leafPage, void *data);
    };
}

#endif