#include "src/include/pageDeserializer.h"
#include "src/include/pageSerDesConstants.h"
#include "src/include/varcharSerDes.h"

typedef char byte;

namespace PeterDB {

    bool PageDeserializer::isLeafPage(const void *data) {
        byte *readPtr = (byte *) data + PageSerDesConstants::IS_LEAF_PAGE_OFFSET;
        bool isLeafPage = *( (bool*) readPtr );
        return isLeafPage;
    }

    void PageDeserializer::toNonLeafPage(const void *data, NonLeafPage &nonLeafPage) {
        assert(!PageDeserializer::isLeafPage(data));

        nonLeafPage.setFreeByteCount(readFreeByteCount(data));
        nonLeafPage.setKeyType(readKeyType(data));
        nonLeafPage.setNextPageNum(readNextPageNum(data));

        unsigned int numKeys = readNumKeys(data);
        readPageNumAndKeyPairs(nonLeafPage, data, numKeys);
    }

    void PageDeserializer::toLeafPage(const void *data, LeafPage &leafPage) {
        assert(PageDeserializer::isLeafPage(data));

        leafPage.setFreeByteCount(readFreeByteCount(data));
        AttrType keyType = readKeyType(data);
        leafPage.setKeyType(keyType);
        leafPage.setNextPageNum(readNextPageNum(data));

        unsigned int numKeys = readNumKeys(data);
        readKeyAndRidPairs(leafPage, data, numKeys);
    }

    unsigned int PageDeserializer::readFreeByteCount(const void *data) {
        unsigned int freeByteCount;
        byte *readPtr = (byte *) data + PageSerDesConstants::FREEBYTE_COUNT_OFFSET;
        memcpy((void *) &freeByteCount,
               (void *) readPtr,
               PageSerDesConstants::FREEBYTE_COUNT_VAR_SIZE);
        return freeByteCount;
    }

    AttrType PageDeserializer::readKeyType(const void *data) {
        int keyType;
        byte *readPtr = (byte *) data + PageSerDesConstants::KEY_TYPE_OFFSET;
        memcpy((void *) &keyType,
               (void *) readPtr,
               PageSerDesConstants::KEY_TYPE_VAR_SIZE);
        assert(0 <= keyType && keyType <= 2);
        return static_cast<AttrType>(keyType);
    }

    int PageDeserializer::readNextPageNum(const void *data) {
        int nextPageNum;
        byte *readPtr = (byte *) data + PageSerDesConstants::NEXT_PAGE_NUM_OFFSET;
        memcpy((void *) &nextPageNum,
               (void *) readPtr,
               PageSerDesConstants::NEXT_PAGE_NUM_VAR_SIZE);
        return nextPageNum;
    }

    unsigned int PageDeserializer::readNumKeys(const void *data) {
        unsigned int numKeys;
        byte *readPtr = (byte *) data + PageSerDesConstants::NUM_KEYS_OFFSET;
        memcpy((void *) &numKeys,
               (void *) readPtr,
               PageSerDesConstants::NUM_KEYS_VAR_SIZE);
        return numKeys;
    }

    void
    PageDeserializer::readPageNumAndKeyPairs(NonLeafPage &nonLeafPage, const void *data, unsigned int numKeys) {
        byte *readPtr = (byte *) data;
        const size_t pageNumSize = sizeof(unsigned int);

        nonLeafPage.getPageNumAndKeys().clear();

        for (int elementNum = 0; elementNum < numKeys; ++elementNum) {
            // read pageNum to file
            unsigned int pageNum;
            memcpy((void *) &pageNum, (void *) readPtr, pageNumSize);
            readPtr += pageNumSize;

            // read key from file
            const AttrType keyAttribute = nonLeafPage.getKeyType();
            switch (keyAttribute) {
                case TypeInt: {
                    const size_t keySize = 4;
                    int key;
                    memcpy((void *) &key, (void *) readPtr, keySize);
                    readPtr += keySize;

                    const PageNumAndKey pageNumAndKey(pageNum, key);
                    nonLeafPage.getPageNumAndKeys().push_back(pageNumAndKey);
                    break;
                }

                case TypeReal: {
                    const size_t keySize = 4;
                    float key;
                    memcpy((void *) &key, (void *) readPtr, keySize);
                    readPtr += keySize;

                    const PageNumAndKey pageNumAndKey(pageNum, key);
                    nonLeafPage.getPageNumAndKeys().push_back(pageNumAndKey);
                    break;
                }
                case TypeVarChar: {
                    std::string key = VarcharSerDes::deserialize(readPtr);
                    const size_t keySizePreDeserialization = VarcharSerDes::computeSerializedSize(key);
                    readPtr += keySizePreDeserialization;

                    const PageNumAndKey pageNumAndKey(pageNum, key);
                    nonLeafPage.getPageNumAndKeys().push_back(pageNumAndKey);
                    break;
                }
                default:
                    ERROR("Illegal Attribute type");
                    assert(1);
            }
        }
    }

    void PageDeserializer::readKeyAndRidPairs(LeafPage &leafPage, const void *data, unsigned int numKeys) {
        byte *readPtr = (byte *) data;
        const size_t ridSize = sizeof(RID);

        leafPage.getRidAndKeyPairs().clear();

        for (int elementNum = 0; elementNum < numKeys; ++elementNum) {
            // read RID from file
            RID rid;
            memcpy((void *) &rid, (void *) readPtr, ridSize);
            readPtr += ridSize;

            // read key to file
            const AttrType keyAttributeType = leafPage.getKeyType();
            switch (keyAttributeType) {
                case TypeInt: {
                    const size_t keySize = 4;
                    int key;
                    memcpy((void *) &key, (void *) readPtr, keySize);
                    readPtr += keySize;

                    const RidAndKey ridAndKey(rid, key);
                    leafPage.getRidAndKeyPairs().push_back(ridAndKey);
                    break;
                }

                case TypeReal: {
                    const size_t keySize = 4;
                    float key;
                    memcpy((void *) &key, (void *) readPtr, keySize);
                    readPtr += keySize;

                    const RidAndKey ridAndKey(rid, key);
                    leafPage.getRidAndKeyPairs().push_back(ridAndKey);
                    break;
                }

                case TypeVarChar: {
                    std::string key = VarcharSerDes::deserialize(readPtr);
                    const size_t keySizePreDeserialization = VarcharSerDes::computeSerializedSize(key);
                    readPtr += keySizePreDeserialization;

                    const RidAndKey ridAndKey(rid, key);
                    leafPage.getRidAndKeyPairs().push_back(ridAndKey);
                    break;
                }
                default:
                    ERROR("Illegal Attribute type");
                    assert(1);
            }
        }
    }
}
