#ifndef _non_leaf_page_h_

#define _non_leaf_page_h_

#include <cstring>
#include "rbfm.h"

namespace PeterDB {
    class PageNumAndKey {
    private:
        const unsigned int pageNum;
        int intKey;
        float floatKey;
        std::string stringKey;

    public:
        PageNumAndKey(unsigned int pageNum, int intKey);

        PageNumAndKey(unsigned int pageNum, float floatKey);

        PageNumAndKey(unsigned int pageNum, const std::string &stringKey);

        unsigned int getPageNum() const;

        int getIntKey() const;

        float getFloatKey() const;

        const std::string &getStringKey() const;
    };

    class NonLeafPage {
    public:
        const std::vector<PageNumAndKey> &getPageNumAndKeys() const;

        const Attribute &getKeyType() const;

        unsigned int getFreeByteCount() const;

    private:
        std::vector<PageNumAndKey> pageNumAndKeys;
        Attribute keyType;
        unsigned int freeByteCount;
    };
}

#endif