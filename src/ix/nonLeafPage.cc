#include "src/include/nonLeafPage.h"
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

    PageNumAndKey::PageNumAndKey() {
        _pageNum = 0;
        valid = false;
    }

    PageNumAndKey::PageNumAndKey(unsigned int pageNum) {
        _pageNum = pageNum;
        valid = true;
    }

    PageNumAndKey::PageNumAndKey(unsigned int pageNum, int intKey) {
        _pageNum = pageNum;
        _intKey = intKey;
        valid = true;
    }

    PageNumAndKey::PageNumAndKey(unsigned int pageNum, float floatKey) {
        _pageNum = pageNum;
        _floatKey = floatKey;
        valid = true;
    }

    PageNumAndKey::PageNumAndKey(unsigned int pageNum, const std::string &stringKey) {
        _pageNum = pageNum;
        _stringKey = stringKey;
        valid = true;
    }

    PageNumAndKey& PageNumAndKey::operator=(const PageNumAndKey& other) {
        valid = other.isValid();
        _pageNum = other.getPageNum();
        _intKey = other.getIntKey();
        _floatKey = other.getFloatKey();
        _stringKey = other.getStringKey();
        return *this;
    }

    bool PageNumAndKey::isValid() const {
        return valid;
    }

    void PageNumAndKey::makeInvalid() {
        _pageNum = 0;
        valid = false;
    }

    unsigned int PageNumAndKey::getPageNum() const {
        return _pageNum;
    }

    void PageNumAndKey::setPageNum(unsigned int pageNum) {
        _pageNum = pageNum;
    }

    int PageNumAndKey::getIntKey() const {
        return _intKey;
    }

    float PageNumAndKey::getFloatKey() const {
        return _floatKey;
    }

    const std::string& PageNumAndKey::getStringKey() const {
        return _stringKey;
    }

    /*
     * Use this to create a fresh page in-memory
     * Ensure to subsequently "serialize" it to write through to file
     */
    NonLeafPage::NonLeafPage() {
        _freeByteCount = PAGE_SIZE
                         - PageSerDesConstants::FREEBYTE_COUNT_VAR_SIZE
                         - PageSerDesConstants::IS_LEAF_PAGE_VAR_SIZE
                         - PageSerDesConstants::KEY_TYPE_VAR_SIZE
                         - PageSerDesConstants::NEXT_PAGE_NUM_VAR_SIZE
                         - PageSerDesConstants::NUM_KEYS_VAR_SIZE;
    }

    std::vector<PageNumAndKey>& NonLeafPage::getPageNumAndKeys() {
        return _pageNumAndKeys;
    }

    const AttrType NonLeafPage::getKeyType() const {
        return _keyType;
    }

    unsigned int NonLeafPage::getFreeByteCount() const {
        return _freeByteCount;
    }

    /*
     * returns -1 to indicate that this is the last Node
     */
     int NonLeafPage::getNextPageNum() const {
        return _nextPageNum;
    }

    unsigned int NonLeafPage::getNumKeys() const {
        return _pageNumAndKeys.size();
    }

    void NonLeafPage::setNextPageNum(const int nextPageNum) {
        NonLeafPage::_nextPageNum = nextPageNum;
    }

    void NonLeafPage::setFreeByteCount(const unsigned int freeByteCount) {
        NonLeafPage::_freeByteCount = freeByteCount;
    }

    void NonLeafPage::setKeyType(const AttrType keyType) {
        _keyType = keyType;
    }

    /*
     * Compare the given keys
     * returns 0 if both are equal
     * returns -1 if first is less than second
     * returns 1 if first is greater than second
     */
    int NonLeafPage::compare(const PageNumAndKey& first, const PageNumAndKey& second) {
        switch (_keyType) {
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

    int NonLeafPage::comparePageNumAndKeyAndRidAndKey(const PageNumAndKey& first,
                                                      const RidAndKey& second) {
        switch (_keyType) {
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

    int NonLeafPage::findNextPage(const RidAndKey& entry) {
        // assume the next page is the page pointed by last entry in the list
        int idx = _pageNumAndKeys.size()-1;

        for (int i=0; i < _pageNumAndKeys.size()-1; i++) {
            if (1 == comparePageNumAndKeyAndRidAndKey(_pageNumAndKeys[i], entry)) {
                idx = i;
                break;
            }
        }

        return _pageNumAndKeys[idx].getPageNum();
    }

    bool NonLeafPage::getRequiredSpace(const PageNumAndKey& entry) {
        auto reqSpace = sizeof(entry.getPageNum());
        switch (_keyType) {
            case TypeInt:
            case TypeReal:
                reqSpace += 4;
                break;
            case TypeVarChar:
                reqSpace += entry.getStringKey().size() + 4;
                break;
            default:
                assert(0);
        }
        return reqSpace;
    }

    bool NonLeafPage::canInsert(const PageNumAndKey& entry) {
        auto reqSpace = getRequiredSpace(entry);
        return (_freeByteCount >= reqSpace);
    }

    /*
     * when inserting the guide node, we should take care of the pointers properly
     * given guide node key as entry, and the next pointer of that
     * we should first find the insert position. insert the entry there
     * set the left pointer as next elements left pointer
     * set the next elements left pointer as given next pointer
     */
    void NonLeafPage::insertGuideNode(PageNumAndKey& guideKey, unsigned int nextPagePtr, bool force) {
        // force is used when there'll be split after insert
        // so dont bother sanity checks or updating the metadata
        // if not force, assert there is enough space
        if (!force) assert(true == canInsert(guideKey));

        // insert the enrty to the vector
        int sz = _pageNumAndKeys.size();
        auto insertIdx = sz - 1; // we dont check the value of the last element because
                                 // its dummy value and only stores the page pointer

        for (int i = 0; i < sz - 1; i++) {
            if (1 == compare(_pageNumAndKeys[i], guideKey)) {
                // the key in the list is greater than the entry
                // to be inserted. so mark this as the index to insert new entry
                insertIdx = i;
                break;
            }
        }

        _pageNumAndKeys.insert(_pageNumAndKeys.begin() + insertIdx, guideKey);

        // now swap the page pointers of inserted index and next index
        // if the order was 1-10-2-20-3-$, and we inserted key 15 with
        // 4 as the page pointer for all the values greater than 15
        // the array should become 1-10-2-15-4-20-3-$
        // but now the array is 1-10-4-15-2-20-3-$, so swap the page pointers
        // at insert index and next index
        _pageNumAndKeys[insertIdx].setPageNum(_pageNumAndKeys[insertIdx+1].getPageNum());
        _pageNumAndKeys[insertIdx+1].setPageNum(nextPagePtr);
        
        // if not force, set the metadata properly
        if (!force) _freeByteCount -= getRequiredSpace(guideKey);
    }

    void NonLeafPage::resetMetadata() {
        int spaceOccupied = 0;
        for(auto &entry: _pageNumAndKeys) {
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
