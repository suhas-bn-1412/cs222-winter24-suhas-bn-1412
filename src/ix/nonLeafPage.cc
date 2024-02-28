#include "src/include/nonLeafPage.h"

namespace PeterDB {

    const std::vector<PageNumAndKey> & NonLeafPage::getPageNumAndKeys() const {
        return _pageNumAndKeys;
    }

    const Attribute & NonLeafPage::getKeyType() const {
        return _keyType;
    }

    unsigned int NonLeafPage::getFreeByteCount() const {
        return _freeByteCount;
    }

    unsigned int NonLeafPage::getNextPageNum() const {
        return _nextPageNum;
    }

    unsigned int NonLeafPage::getNumKeys() {
        return _pageNumAndKeys.size();
    }

    void NonLeafPage::setNextPageNum(unsigned int nextPageNum) {
        NonLeafPage::_nextPageNum = nextPageNum;
    }

    void NonLeafPage::setFreeByteCount(unsigned int freeByteCount) {
        NonLeafPage::_freeByteCount = freeByteCount;
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

    const std::string &PageNumAndKey::getStringKey() const {
        return _stringKey;
    }
}
