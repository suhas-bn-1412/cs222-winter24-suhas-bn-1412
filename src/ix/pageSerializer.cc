#include "src/include/pageSerializer.h"
#include "src/include/pageSerDesConstants.h"

typedef char byte;

namespace PeterDB {

    void PageSerializer::toBytes(NonLeafPage nonLeafPage, void *data) {
        writeFreeByteCount(nonLeafPage.getFreeByteCount(), data);
        writeIsLeafPage(false, data);
        writeKeyType(nonLeafPage.getKeyType(), data);
        writeNextPageNum(nonLeafPage.getNextPageNum(), data);
        writeNumKeys(nonLeafPage.getNumKeys(), data);

        writePageNumAndKeyPairs(nonLeafPage, data);
    }

    void PageSerializer::toBytes(const LeafPage &leafPage, void *data) {
        writeFreeByteCount(leafPage.getFreeByteCount(), data);
        writeIsLeafPage(true, data);
        writeKeyType(leafPage.getKeyType(), data);
        writeNextPageNum(leafPage.getNextPageNum(), data);
        writeNumKeys(leafPage.getNumKeys(), data);

        writeKeyAndRidPair(leafPage, data);
    }

    void PageSerializer::writeFreeByteCount(unsigned int freeByteCount, const void *data) {
        byte *writePtr = (byte *) data + PageSerDesConstants::FREEBYTE_COUNT_OFFSET;
        memcpy((void *) writePtr,
               (void *) &freeByteCount,
               PageSerDesConstants::FREEBYTE_COUNT_VAR_SIZE);
    }

    void PageSerializer::writeIsLeafPage(bool isLeafPage, void *data) {
        byte *writePtr = (byte *) data + PageSerDesConstants::IS_LEAF_PAGE_OFFSET;
        memcpy((void *) writePtr,
               (void *) &isLeafPage,
               PageSerDesConstants::IS_LEAF_PAGE_VAR_SIZE);
    }

    void PageSerializer::writeKeyType(const Attribute &keyAttribute, const void *data) {
        byte *writePtr = (byte *) data + PageSerDesConstants::KEY_TYPE_OFFSET;
        memcpy((void *) writePtr,
               (void *) &keyAttribute,
               PageSerDesConstants::KEY_TYPE_VAR_SIZE);
    }

    void PageSerializer::writeNextPageNum(unsigned int nextPageNum, void *data) {
        byte *writePtr = (byte *) data + PageSerDesConstants::NEXT_PAGE_NUM_OFFSET;
        memcpy((void *) writePtr,
               (void *) &nextPageNum,
               PageSerDesConstants::NEXT_PAGE_NUM_VAR_SIZE);
    }

    void PageSerializer::writeNumKeys(unsigned int numKeys, void *data) {
        byte *writePtr = (byte *) data + PageSerDesConstants::NUM_KEYS_OFFSET;
        memcpy((void *) writePtr,
               (void *) &numKeys,
               PageSerDesConstants::NUM_KEYS_VAR_SIZE);
    }

    void PageSerializer::writePageNumAndKeyPairs(const NonLeafPage &nonLeafPage, const void *data) {
        byte *writePtr = (byte *) data;
        const size_t pageNumSize = sizeof(unsigned int);

        for (const auto &pageNumAndKey: nonLeafPage.getPageNumAndKeys()) {
            // write _pageNum to file
            const unsigned int pageNum = pageNumAndKey.getPageNum();
            memcpy((void *) writePtr, (void *) &pageNum, pageNumSize);
            writePtr += pageNumSize;

            // write key to file
            switch (nonLeafPage.getKeyType().type) {
                case TypeInt: {
                    const size_t keySize = sizeof(nonLeafPage.getKeyType().length);
                    const int key = pageNumAndKey.getIntKey();
                    memcpy((void *) writePtr, (void *) &key, keySize);
                    writePtr += keySize;
                    break;
                }

                case TypeReal: {
                    const size_t keySize = sizeof(nonLeafPage.getKeyType().length);
                    const float key = pageNumAndKey.getFloatKey();
                    memcpy((void *) writePtr, (void *) &key, keySize);
                    writePtr += keySize;
                    break;
                }
                case TypeVarChar:
                    //todo
                    break;
                default:
                    ERROR("Illegal Attribute type");
                    assert(1);
            }
        }


    }

    void PageSerializer::writeKeyAndRidPair(const LeafPage &leafPage, void *data) {
        byte *writePtr = (byte *) data;
        const size_t ridSize = sizeof(RID);

        for (const auto &ridAndKeyPair: leafPage.getRidAndKeyPairs()) {
            // write RID to file
            const RID &rid = ridAndKeyPair.getRid();
            memcpy((void *) writePtr, (void *) &rid, ridSize);
            writePtr += ridSize;

            // write key to file
            switch (leafPage.getKeyType().type) {
                case TypeInt: {
                    const size_t keySize = sizeof(leafPage.getKeyType().length);
                    const int key = ridAndKeyPair.getIntKey();
                    memcpy((void *) writePtr, (void *) &key, keySize);
                    writePtr += keySize;
                    break;
                }
                case TypeReal: {
                    const size_t keySize = sizeof(leafPage.getKeyType().length);
                    const float key = ridAndKeyPair.getFloatKey();
                    memcpy((void *) writePtr, (void *) &key, keySize);
                    writePtr += keySize;
                    break;
                }
                case TypeVarChar:
                    //todo
                    break;
                default:
                    ERROR("Illegal Attribute type");
                    assert(1);
            }
        }
    }
}