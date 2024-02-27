#include "src/include/nonLeafPage.h"

namespace PeterDB {

    const std::vector<PageNumAndKey> & NonLeafPage::getPageNumAndKeys() const {
        return pageNumAndKeys;
    }

    const Attribute & NonLeafPage::getKeyType() const {
        return keyType;
    }

    unsigned int NonLeafPage::getFreeByteCount() const {
        return freeByteCount;
    }

    PageNumAndKey::PageNumAndKey(unsigned int pageNum, int intKey) : pageNum(pageNum), intKey(intKey) {}

    PageNumAndKey::PageNumAndKey(unsigned int pageNum, float floatKey) : pageNum(pageNum), floatKey(floatKey) {}

    PageNumAndKey::PageNumAndKey(unsigned int pageNum, const std::string &stringKey) : pageNum(pageNum),
                                                                                       stringKey(stringKey) {}

    unsigned int PageNumAndKey::getPageNum() const {
        return pageNum;
    }

    int PageNumAndKey::getIntKey() const {
        return intKey;
    }

    float PageNumAndKey::getFloatKey() const {
        return floatKey;
    }

    const std::string &PageNumAndKey::getStringKey() const {
        return stringKey;
    }
}
