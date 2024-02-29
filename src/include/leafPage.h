#ifndef _leaf_page_h_

#define _leaf_page_h_

#include "rbfm.h"

namespace PeterDB {
    class RidAndKey {

    private:
        const RID _rid;
        int _intKey;
        float _floatKey;
        std::string _stringKey;

    public:
        RidAndKey(const RID &rid, int intKey);

        RidAndKey(const RID &rid, float floatKey);

        RidAndKey(const RID &rid, const std::string &stringKey);

        const RID &getRid() const;

        int getIntKey() const;

        float getFloatKey() const;

        std::string &getStringKey();
    };

    class LeafPage {
    private:
        std::vector<RidAndKey> _ridAndKeyPairs;
        Attribute _keyType;
        int _nextPageNum;
        unsigned int _freeByteCount;

    public:
        /*
         * To be used to create a fresh leafPage in-memory
         */
        LeafPage();

        std::vector<RidAndKey> &getRidAndKeyPairs();

        const Attribute &getKeyType() const;

        /*
         * returns -1 to indicate that this is the last Node
         */
        int getNextPageNum() const;

        unsigned int getFreeByteCount() const;

        unsigned int getNumKeys() const;

        void setNextPageNum(const int nextPageNum);

        /*
         * This represents the freeByteCount of the page on-file
         * Implying that when string keys are added/ deleted,
         * the freeByteCount must be set (by the caller) keeping in mind the varchar representation
         */
        void setFreeByteCount(const unsigned int freeByteCount);

        void setKeyType(const Attribute &keyType);
    };
}

#endif