#ifndef _leaf_page_h_

#define _leaf_page_h_

#include "rbfm.h"

namespace PeterDB {
    class RidAndKey {

    private:
        const RID rid;
        int intKey;
        float floatKey;
        std::string stringKey;

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
        std::vector<RidAndKey> ridAndKey;
        Attribute keyType;
        unsigned int freeByteCount;

    public:
            const std::vector<RidAndKey> &getRidAndKey() const;

            const Attribute &getKeyType() const;

            unsigned int getFreeByteCount() const;
        };
}

#endif