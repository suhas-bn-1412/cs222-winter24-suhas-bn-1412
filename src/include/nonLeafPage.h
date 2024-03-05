#ifndef _non_leaf_page_h_

#define _non_leaf_page_h_

#include <cstring>
#include "rbfm.h"
#include "leafPage.h"

namespace PeterDB {
    class PageNumAndKey {
    private:
        unsigned int _pageNum = 0;
        int _intKey = 0;
        float _floatKey = float(0);
        std::string _stringKey = "";
        bool valid = false;

    public:
        PageNumAndKey();

        PageNumAndKey(unsigned int pageNum);

        PageNumAndKey(unsigned int pageNum, int intKey);

        PageNumAndKey(unsigned int pageNum, float floatKey);

        PageNumAndKey(unsigned int pageNum, const std::string &stringKey);

        PageNumAndKey& operator=(const PageNumAndKey& other);

        unsigned int getPageNum() const;

        void setPageNum(unsigned int pageNum);

        int getIntKey() const;

        float getFloatKey() const;

        const std::string& getStringKey() const;

        bool isValid() const;

        void makeInvalid();
    };

    class NonLeafPage {
    private:
        std::vector<PageNumAndKey> _pageNumAndKeys;
        AttrType _keyType;
        unsigned int _nextPageNum = 0;
        unsigned int _freeByteCount = 0;

    public:
        /*
         * Use this to create a fresh page in-memory
         * Ensure to subsequently "serialize" it to write through to file
         */
        NonLeafPage();

        std::vector<PageNumAndKey>& getPageNumAndKeys();

        const AttrType getKeyType() const;

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

        void setKeyType(AttrType keyType);

        /*
         * Compare the given keys
         * returns 0 if both are equal
         * returns -1 if first is less than second
         * returns 1 if first is greater than second
         */
        int compare(const PageNumAndKey& first, const PageNumAndKey& second);
        int comparePageNumAndKeyAndRidAndKey(const PageNumAndKey& first,
                                             const RidAndKey& second);

        int findNextPage(const RidAndKey& entry);

        unsigned int getRequiredSpace(const PageNumAndKey& entry);

        bool canInsert(const PageNumAndKey& entry);

        void insertGuideNode(PageNumAndKey& entry, unsigned int nextPagePtr, bool force);

        void resetMetadata();
    };
}

#endif
