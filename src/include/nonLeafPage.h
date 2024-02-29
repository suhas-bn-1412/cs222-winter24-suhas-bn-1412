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

        std::string &getStringKey();
    };

    class NonLeafPage {
    public:
        /*
         * Use this to create a fresh page in-memory
         * Ensure to subsequently "serialize" it to write through to file
         */
        NonLeafPage();

        std::vector<PageNumAndKey> &getPageNumAndKeys();

        const Attribute &getKeyType() const;

        /*
         * returns -1 to indicate that this is the last Node
         */
        int getNextPageNum() const;

        unsigned int getFreeByteCount() const;

        unsigned int getNumKeys() const;

        void setNextPageNum(int nextPageNum);

        /*
         * This represents the freeByteCount of the page on-file
         * Implying that when string keys are added/ deleted,
         * the freeByteCount must be set (by the caller) keeping in mind the varchar representation
        */
        void setFreeByteCount(unsigned int freeByteCount);

        void setKeyType(Attribute keyType);

    private:
        std::vector<PageNumAndKey> _pageNumAndKeys;
        Attribute _keyType;
        int _nextPageNum;
        unsigned int _freeByteCount;
    };
}

#endif