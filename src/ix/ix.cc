#include "src/include/ix.h"
#include "src/include/pageDeserializer.h"
#include "src/include/pageSerializer.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        _index_manager._pagedFileManager = &PagedFileManager::instance();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        return _pagedFileManager->createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return _pagedFileManager->destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        return _pagedFileManager->openFile(fileName, ixFileHandle._pfmFileHandle);
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return _pagedFileManager->closeFile(ixFileHandle._pfmFileHandle);
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        // switch insertion method on attribute
        // case integer, float, varchar

        return -1;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        void *pageData = malloc(PAGE_SIZE); //todo: migrate to class member, perhaps Suhas has done this already, so wait for his commits
        unsigned int pageNum;

        //todo:
        // 1) get root pageNum and load the root page
        pageNum = 1;
        ixFileHandle._pfmFileHandle.readPage((unsigned) pageNum, pageData);

        // 2) recursively search for the leaf node that should contain the given (key, rid)
        while (!PageDeserializer::isLeafPage(pageData)) {
            NonLeafPage nonLeafPage;
            PageDeserializer::toNonLeafPage(pageData, nonLeafPage);

            // traverse down a level of the B+ index tree
            pageNum = getLowerLevelNode(key, attribute, nonLeafPage);

            ixFileHandle._pfmFileHandle.readPage((unsigned) pageNum, pageData);
        }

        // 3) delete the (key, rid) entry from the leaf page, if it exists
        LeafPage leafPage;
        PageDeserializer::toLeafPage(pageData, leafPage);
        RC rc = deleteFromPage(key, rid, attribute, leafPage);
        if (rc == 0) {
            // 4) serialize and write-through the page back back to file to effect the delete operation
            PageSerializer::toBytes(leafPage, pageData);
            ixFileHandle._pfmFileHandle.writePage(pageNum, pageData);
        }
        free(pageData);
        return rc;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        return 0;
    }

    unsigned int IndexManager::getLowerLevelNode(const void *searchKey, const Attribute &attribute, NonLeafPage &nonLeafPage) {
        std::vector<PageNumAndKey> &pageNumAndKeyPairs = nonLeafPage.getPageNumAndKeys();
        for (const auto& pageNumAndKey: pageNumAndKeyPairs) {
            if (keyCompare(searchKey, attribute, pageNumAndKey) > 0 || &pageNumAndKey == &pageNumAndKeyPairs.back()) {
                return pageNumAndKey.getPageNum();
            }
        }
        assert(1);
    }

    RC IndexManager::deleteFromPage(const void *targetKey, const RID &targetRid, const Attribute &targetKeyAttribute,
                                    PeterDB::LeafPage &leafPage) {
        std::vector<RidAndKey> ridAndKeyPairs = leafPage.getRidAndKeyPairs();
        for (auto ridAndKeyIter = ridAndKeyPairs.begin(); ridAndKeyIter != ridAndKeyPairs.end(); ridAndKeyIter++) {
            if (keyCompare(targetKey, targetRid, targetKeyAttribute, *ridAndKeyIter) == 0) {
                ridAndKeyPairs.erase(ridAndKeyIter);
                return 0;
            }
        }
        return -1;
    }

    /*
     * Compares a search key with PageNumAndKey (handling all types)
     * Return value:
     * 0   : both the keys are equal
     * < 0 : 'searchKey' < pageNumAndKeyPair.get___Key()
     * > 0 : 'searchKey' > pageNumAndKeyPair.get___Key()
     */
    int IndexManager::keyCompare(const void *searchKey, const Attribute &searchKeyType, const PageNumAndKey &pageNumAndKeyPair) {
        switch (searchKeyType.type) {
            case TypeInt: {
                int keyA;
                memcpy((void*) &keyA, searchKey, sizeof(keyA));
                int keyB = pageNumAndKeyPair.getIntKey();
                if (keyA == keyB) {
                    return 0;
                } else if (keyA < keyB) {
                    return -1;
                } else {
                    return 1;
                }
            }

            case TypeReal: {
                float keyA;
                memcpy((void*) &keyA, searchKey, sizeof(keyA));
                float keyB = pageNumAndKeyPair.getFloatKey();
                if (keyA == keyB) {
                    return 0;
                } else if (keyA < keyB) {
                    return -1;
                } else {
                    return 1;
                }
            }
            case TypeVarChar:
                //todo
                assert(1);
                break;
            default:
                ERROR("Illegal Attribute type");
                assert(1);
        }
    }

    /*
    * Compares a search key with RidAndKey (handling all types)
    * Return value:
    * 0   : both the keys are equal
    * < 0 : 'searchKey' < pageNumAndKeyPair.get___Key()
    * > 0 : 'searchKey' > pageNumAndKeyPair.get___Key()
    */
    int IndexManager::keyCompare(const void *searchKey, const RID searchRid, const Attribute searchKeyType, const RidAndKey ridAndKeyPair) {
        switch (searchKeyType.type) {
            case TypeInt: {
                int keyA;
                memcpy((void*) &keyA, searchKey, sizeof(keyA));
                const int keyB = ridAndKeyPair.getIntKey();
                RID ridB = ridAndKeyPair.getRid();
                if (keyA == keyB) {
                    return searchRid.pageNum == ridB.pageNum && searchRid.slotNum == ridB.slotNum;
                } else if (keyA < keyB) {
                    return -1;
                } else {
                    return 1;
                }
            }

            case TypeReal: {
                float keyA;
                memcpy((void*) &keyA, searchKey, sizeof(keyA));
                float keyB = ridAndKeyPair.getFloatKey();
                RID ridB = ridAndKeyPair.getRid();
                if (keyA == keyB) {
                    return searchRid.pageNum == ridB.pageNum && searchRid.slotNum == ridB.slotNum;
                } else if (keyA < keyB) {
                    //todo: compare RIDs
                    return -1;
                } else {
                    return 1;
                }
            }
            case TypeVarChar:
                //todo
                assert(1);
                break;
            default:
                ERROR("Illegal Attribute type");
                assert(1);
        }
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

} // namespace PeterDB
