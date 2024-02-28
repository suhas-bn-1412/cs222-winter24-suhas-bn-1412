#include "src/include/leafPage.h"

namespace PeterDB {

    RidAndKey::RidAndKey(const RID &rid, int intKey) : _rid(rid), _intKey(intKey) {}

    RidAndKey::RidAndKey(const RID &rid, float floatKey) : _rid(rid), _floatKey(floatKey) {}

    RidAndKey::RidAndKey(const RID &rid, const std::string &stringKey) : _rid(rid), _stringKey(stringKey) {}

    const RID &RidAndKey::getRid() const {
        return _rid;
    }

    int RidAndKey::getIntKey() const {
        return _intKey;
    }

    float RidAndKey::getFloatKey() const {
        return _floatKey;
    }

    const std::string &RidAndKey::getStringKey() const {
        return _stringKey;
    }

    const std::vector<RidAndKey> &LeafPage::getRidAndKeyPairs() const {
        return _ridAndKeyPairs;
    }

    const Attribute &LeafPage::getKeyType() const {
        return _keyType;
    }

    unsigned int LeafPage::getNextPageNum() const {
        return _nextPageNum;
    }

    unsigned int LeafPage::getFreeByteCount() const {
        return _freeByteCount;
    }

    unsigned int LeafPage::getNumKeys() const{
        return _ridAndKeyPairs.size();
    }

    void LeafPage::setNextPageNum(unsigned int nextPageNum) {
        LeafPage::_nextPageNum = nextPageNum;
    }

    void LeafPage::setFreeByteCount(unsigned int freeByteCount) {
        LeafPage::_freeByteCount = freeByteCount;
    }
}