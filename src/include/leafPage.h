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

        const std::string &getStringKey() const;
    };

    class LeafPage {
    private:
        std::vector<RidAndKey> _ridAndKeyPairs;
        Attribute _keyType;
        unsigned int _nextPageNum;
        unsigned int _freeByteCount;

    public:
        /*
         * To be used to create a fresh leafPage in-memory
         */
        LeafPage();

        const std::vector<RidAndKey> &getRidAndKeyPairs() const;

        const Attribute &getKeyType() const;

        unsigned int getNextPageNum() const;

        unsigned int getFreeByteCount() const;

        unsigned int getNumKeys() const;

        void setNextPageNum(const unsigned int nextPageNum);

        void setFreeByteCount(const unsigned int freeByteCount);

        void setKeyType(const Attribute &keyType);
    };
}

#endif