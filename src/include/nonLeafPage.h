#ifndef _non_leaf_page_h_

#define _non_leaf_page_h_

#include <cstring>
#include "rbfm.h"

namespace PeterDB {
    class PageNumAndKey {
    private:
        const unsigned int _pageNum;
        int _intKey;
        float _floatKey;
        std::string _stringKey;

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

        unsigned int getNextPageNum() const;

        unsigned int getFreeByteCount() const;

        unsigned int getNumKeys();

        void setNextPageNum(unsigned int nextPageNum);

        void setFreeByteCount(unsigned int freeByteCount);

    private:
        std::vector<PageNumAndKey> _pageNumAndKeys;
        Attribute _keyType;
        unsigned int _nextPageNum;
        unsigned int _freeByteCount;
    };
}

#endif