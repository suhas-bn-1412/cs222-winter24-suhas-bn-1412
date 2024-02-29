#include "src/include/leafPage.h"
#include "src/include/pageSerDesConstants.h"

namespace PeterDB {

    template<typename T>
    int genericCompare(const T& key1, const T& key2) {
        if (key1 < key2) return -1;
        if (key1 == key2) return 0;
        if (key1 > key2) return 1;

        assert(0);
        return 0;
    }

    RidAndKey::RidAndKey(const RID &rid, int intKey) : _rid(rid), _intKey(intKey) {}

    RidAndKey::RidAndKey(const RID &rid, float floatKey) : _rid(rid), _floatKey(floatKey) {}

    RidAndKey::RidAndKey(const RID &rid, const std::string &stringKey) : _rid(rid), _stringKey(stringKey) {}

    RidAndKey& RidAndKey::operator=(const RidAndKey& other) {
        _rid = other.getRid();
        _intKey = other.getIntKey();
        _floatKey = other.getIntKey();
        _stringKey = other.getStringKey();
        return *this;
    }

    const RID &RidAndKey::getRid() const {
        return _rid;
    }

    int RidAndKey::getIntKey() const {
        return _intKey;
    }

    float RidAndKey::getFloatKey() const {
        return _floatKey;
    }

    const std::string& RidAndKey::getStringKey() const {
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

    std::vector<RidAndKey>& LeafPage::getRidAndKeyPairs() {
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

    /*
     * Compare the given keys
     * returns 0 if both are equal
     * returns -1 if first is less than second
     * returns 1 if first is greater than second
     */
    int LeafPage::compare(const RidAndKey& first, const RidAndKey& second) {
        switch (_keyType.type) {
            case TypeInt:
                return genericCompare(first.getIntKey(), second.getIntKey());
            case TypeReal:
                return genericCompare(first.getFloatKey(), second.getFloatKey());
            case TypeVarChar:
                return genericCompare(first.getStringKey(), second.getStringKey());
        }

        assert(0);
        return 0;
    }

    bool LeafPage::getRequiredSpace(const RidAndKey& entry) {
        auto reqSpace = sizeof(RID);
        switch (_keyType.type) {
            case TypeInt:
            case TypeReal:
                reqSpace += _keyType.length;
                break;
            case TypeVarChar:
                reqSpace += entry.getStringKey().size() + 4;
                break;
            default:
                assert(0);
        }
        return reqSpace;
    }

    bool LeafPage::canInsert(const RidAndKey& entry) {
        auto reqSpace = getRequiredSpace(entry);
        return (_freeByteCount >= reqSpace);
    }

    void LeafPage::insertEntry(const RidAndKey& entry, bool force) {
        // force is used when there'll be split after insert
        // so dont bother sanity checks or updating the metadata
        // if not force, assert there is enough space
        if (!force) assert(true == canInsert(entry));

        // insert the enrty to the vector
        auto insertIdx = _ridAndKeyPairs.size();
        int vectorSz = _ridAndKeyPairs.size();
        for (int i = 0; i < vectorSz; i++) {
            if (1 == compare(_ridAndKeyPairs[i], entry)) {
                // the key in the list is greater than the entry
                // to be inserted. so mark this as the index to insert new entry
                insertIdx = i;
                break;
            }
        }

        _ridAndKeyPairs.insert(_ridAndKeyPairs.begin() + insertIdx, entry);
        
        // if not force, set the metadata properly
        if (!force) _freeByteCount -= getRequiredSpace(entry);
    }

    void LeafPage::resetMetadata() {
        int spaceOccupied = 0;
        for(auto &entry: _ridAndKeyPairs) {
            spaceOccupied += getRequiredSpace(entry);
        }
        
        _freeByteCount = PAGE_SIZE
                         - spaceOccupied
                         - PageSerDesConstants::FREEBYTE_COUNT_VAR_SIZE
                         - PageSerDesConstants::IS_LEAF_PAGE_VAR_SIZE
                         - PageSerDesConstants::KEY_TYPE_VAR_SIZE
                         - PageSerDesConstants::NEXT_PAGE_NUM_VAR_SIZE
                         - PageSerDesConstants::NUM_KEYS_VAR_SIZE;
    }
}
