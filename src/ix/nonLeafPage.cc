#include "src/include/nonLeafPage.h"
#include "src/include/pageSerDesConstants.h"

namespace PeterDB {

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

    std::vector<PageNumAndKey> &NonLeafPage::getPageNumAndKeys() {
        return _pageNumAndKeys;
    }

    const Attribute & NonLeafPage::getKeyType() const {
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

    void NonLeafPage::setKeyType(const Attribute keyType) {
        NonLeafPage::_keyType = keyType;
    }

    PageNumAndKey::PageNumAndKey(unsigned int pageNum, int intKey) : _pageNum(pageNum), _intKey(intKey) {}

    PageNumAndKey::PageNumAndKey(unsigned int pageNum, float floatKey) : _pageNum(pageNum), _floatKey(floatKey) {}

    PageNumAndKey::PageNumAndKey(unsigned int pageNum, const std::string &stringKey) : _pageNum(pageNum),
                                                                                       _stringKey(stringKey) {}

    unsigned int PageNumAndKey::getPageNum() const {
        return _pageNum;
    }

    int PageNumAndKey::getIntKey() const {
        return _intKey;
    }

    float PageNumAndKey::getFloatKey() const {
        return _floatKey;
    }

    std::string &PageNumAndKey::getStringKey() {
        return _stringKey;
    }
}
