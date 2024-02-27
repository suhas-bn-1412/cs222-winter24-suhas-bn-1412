#include "src/include/leafPage.h"

namespace PeterDB {

    RidAndKey::RidAndKey(const RID &rid, int intKey) : rid(rid), intKey(intKey) {}

    RidAndKey::RidAndKey(const RID &rid, float floatKey) : rid(rid), floatKey(floatKey) {}

    RidAndKey::RidAndKey(const RID &rid, const std::string &stringKey) : rid(rid), stringKey(stringKey) {}

    const RID &RidAndKey::getRid() const {
        return rid;
    }

    int RidAndKey::getIntKey() const {
        return intKey;
    }

    float RidAndKey::getFloatKey() const {
        return floatKey;
    }

    const std::string &RidAndKey::getStringKey() const {
        return stringKey;
    }

    const std::vector<RidAndKey> &LeafPage::getRidAndKey() const {
        return ridAndKey;
    }

    const Attribute &LeafPage::getKeyType() const {
        return keyType;
    }

    unsigned int LeafPage::getFreeByteCount() const {
        return freeByteCount;
    }
}