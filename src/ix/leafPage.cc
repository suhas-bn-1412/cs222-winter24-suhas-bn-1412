#include "src/include/leafPage.h"
#include "src/include/pageSerDesConstants.h"

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

    std::string &RidAndKey::getStringKey() {
        return _stringKey;
    }

    /*
     * To be used to create a fresh leafPage in-memory
     */
    LeafPage::LeafPage() {
        _freeByteCount = PAGE_SIZE
                         - PageSerDesConstants::FREEBYTE_COUNT_VAR_SIZE
                         - PageSerDesConstants::IS_LEAF_PAGE_VAR_SIZE
                         - PageSerDesConstants::KEY_TYPE_VAR_SIZE
                         - PageSerDesConstants::NEXT_PAGE_NUM_VAR_SIZE
                         - PageSerDesConstants::NUM_KEYS_VAR_SIZE;
    }

    std::vector<RidAndKey> &LeafPage::getRidAndKeyPairs(){
        return _ridAndKeyPairs;
    }

    const Attribute &LeafPage::getKeyType() const {
        return _keyType;
    }

    /*
     * returns -1 to indicate that this is the last Node
     */
    int LeafPage::getNextPageNum() const {
        return _nextPageNum;
    }

    unsigned int LeafPage::getFreeByteCount() const {
        return _freeByteCount;
    }

    unsigned int LeafPage::getNumKeys() const{
        return _ridAndKeyPairs.size();
    }

    void LeafPage::setNextPageNum(const int nextPageNum) {
        LeafPage::_nextPageNum = nextPageNum;
    }

    void LeafPage::setFreeByteCount(const unsigned int freeByteCount) {
        LeafPage::_freeByteCount = freeByteCount;
    }

    void LeafPage::setKeyType(const Attribute &keyType) {
        _keyType = keyType;
    }
}