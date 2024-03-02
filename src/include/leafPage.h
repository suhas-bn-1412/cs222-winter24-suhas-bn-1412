#ifndef _leaf_page_h_

#define _leaf_page_h_

#include "rbfm.h"

namespace PeterDB {
    class RidAndKey {

    private:
        RID _rid;
        int _intKey = 0;
        float _floatKey = float(0);
        std::string _stringKey = "";

    public:
        RidAndKey(const RID &rid, int intKey);

        RidAndKey(const RID &rid, float floatKey);

        RidAndKey(const RID &rid, const std::string &stringKey);

        RidAndKey& operator=(const RidAndKey& other);

        const RID &getRid() const;

        int getIntKey() const;

        float getFloatKey() const;

        const std::string& getStringKey() const;
    };

    class LeafPage {
    private:
        std::vector<RidAndKey> _ridAndKeyPairs;
        Attribute _keyType;
        int _nextPageNum = -1;
        unsigned int _freeByteCount = 0;

    public:
        /*
         * To be used to create a fresh leafPage in-memory
         */
        LeafPage();

        std::vector<RidAndKey>& getRidAndKeyPairs();

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

        void setKeyType(const Attribute keyType);

        /*
         * Compare the given keys
         * returns 0 if both are equal
         * returns -1 if first is less than second
         * returns 1 if first is greater than second
         */
        int compare(const RidAndKey& first, const RidAndKey& second);

        bool getRequiredSpace(const RidAndKey& entry);

        bool canInsert(const RidAndKey& entry);

        void insertEntry(const RidAndKey& entry, bool force);

        void resetMetadata();
    };
}

#endif
