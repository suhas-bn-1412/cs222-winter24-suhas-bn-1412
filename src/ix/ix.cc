#include "src/include/ix.h"
#include "src/include/pageDeserializer.h"
#include "src/include/pageSerializer.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        if (nullptr == _index_manager._pagedFileManager)
            _index_manager._pagedFileManager = &PagedFileManager::instance();
        if (nullptr == _index_manager.serializer)
            _index_manager.serializer = new PageSerializer();
        if (nullptr == _index_manager.deserializer)
            _index_manager.deserializer = new PageDeserializer();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        return _pagedFileManager->createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return _pagedFileManager->destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if (0 != _pagedFileManager->openFile(fileName, ixFileHandle._pfmFileHandle)) {
            ERROR("Error while opening the index file %s\n", fileName.c_str());
            return -1;
        }

        ixFileHandle._fileName = fileName;

        // if dummy head is not created, create the head, and store the root node pointer
        if (0 == ixFileHandle._pfmFileHandle.getNextPageNum()) {
            void* data = malloc(PAGE_SIZE);
            assert(nullptr != data);
            memset(data, 0, PAGE_SIZE);

            if (0 != ixFileHandle._pfmFileHandle.appendPage(data)) {
                ERROR("Error while creating head node in index file %s\n", fileName.c_str());
                return -1;
            }
        }
        else {
            // when there is dummy head created, then read the head node to
            // find the root node pointer (page number of root node)
            ixFileHandle.fetchRootNodePtrFromDisk();
        }
        return 0;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        ixFileHandle.writeRootNodePtrToDisk();

        return _pagedFileManager->closeFile(ixFileHandle._pfmFileHandle);
    }

    RidAndKey createEntryToInsert(const Attribute &attribute, const void *key, const RID &rid) {
        switch (attribute.type) {
            case TypeInt:
                return RidAndKey(rid, *( (const int*) key ) );
            case TypeReal:
                return RidAndKey(rid, *( (const float*) key ) );
            case TypeVarChar:
                int stringSize = *( (uint32_t*) key);
                std::string stringKey((const char*)key + 4, stringSize);
                return RidAndKey(rid, stringKey);
        }
        assert(0);
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        if (0 == ixFileHandle._rootPagePtr) {
            // root page is not created yet, so create a new one and write
            // that to the disk. when first root node is created it will be
            // a LeafPage
            LeafPage leafPage;
            leafPage.setKeyType(attribute);

            auto newPageNum = ixFileHandle._pfmFileHandle.getNextPageNum();
            assert(0 == writePageToDisk(ixFileHandle, leafPage, -1 /* create page and insert */));
            assert(newPageNum == ixFileHandle._pfmFileHandle.getNextPageNum()-1);

            ixFileHandle._rootPagePtr = newPageNum;
        }

        RidAndKey entryToInsert = createEntryToInsert(attribute, key, rid);

        PageNumAndKey newChild;
        if (0 != insertHelper(ixFileHandle, ixFileHandle._rootPagePtr, entryToInsert, newChild)) {
            ERROR("Error while inserting entry into b+ tree in the file %s\n", ixFileHandle._fileName.c_str());
            return -1;
        }

        if (newChild.isValid()) {
            // root has been split into two pages
            // left page is the same as old root page
            // right page is the new page.. newChild has the page number

            // create a non leaf page, which is the new root
            // to this insert first element as old pagenum, key from newChild
            // insert second element as new pageNum and dummy key
            NonLeafPage newRoot;
            newRoot.setKeyType(attribute);

            PageNumAndKey entry1;
            PageNumAndKey entry2;

            switch (attribute.type) {
                case TypeInt:
                    entry1 = PageNumAndKey(ixFileHandle._rootPagePtr, newChild.getIntKey());
                    entry2 = PageNumAndKey(newChild.getPageNum(), 0);
                    break;
                case TypeReal:
                    entry1 = PageNumAndKey(ixFileHandle._rootPagePtr, newChild.getFloatKey());
                    entry2 = PageNumAndKey(newChild.getPageNum(), float(0));
                    break;
                case TypeVarChar:
                    entry1 = PageNumAndKey(ixFileHandle._rootPagePtr, newChild.getStringKey());
                    entry2 = PageNumAndKey(newChild.getPageNum(), "");
                    break;
            }

            std::vector<PageNumAndKey>& newRootPageNumAndKeys = newRoot.getPageNumAndKeys();
            newRootPageNumAndKeys.push_back(entry1);
            newRootPageNumAndKeys.push_back(entry2);

            newRoot.resetMetadata();

            auto newRootPageNum = ixFileHandle._pfmFileHandle.getNextPageNum();
            assert(0 == writePageToDisk(ixFileHandle, newRoot, -1 /* create page and insert */));
            assert(newRootPageNum == ixFileHandle._pfmFileHandle.getNextPageNum()-1);

            ixFileHandle._rootPagePtr = newRootPageNum;
        }
        return 0;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        //todo: verify IXFileHandle is active (perhaps Suhas has done this func. already; wait)

        void *pageData = malloc(
                PAGE_SIZE); //todo: migrate to class member, perhaps Suhas has done this already, so wait for his commits
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
            // update freeByteCount
            unsigned int oldFreeByteCount = leafPage.getFreeByteCount();
            unsigned int newFreeByteCount = oldFreeByteCount - getKeySize(key, attribute);
            leafPage.setFreeByteCount(newFreeByteCount);
            
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
        return 0;
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
        assert(0);
        return 0;
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
        assert(0);
        return 0;
    }

    unsigned int IndexManager::getKeySize(const void *key, const Attribute &attributeOfKey) {
        switch (attributeOfKey.type) {
            case TypeInt:
                return 4;
            case TypeReal:
                return 4;
            case TypeVarChar:
                unsigned int varcharSize;
                memcpy(&varcharSize, key, sizeof(varcharSize));
                return varcharSize + sizeof(varcharSize);
        }
        assert(0);
        return 0;
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
        return _pfmFileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    }

    void IXFileHandle::fetchRootNodePtrFromDisk() {
        // read page 0 (page 0 is always the page in which we store the
        // page number of the root node of the b+ tree index)
        void* data = malloc(PAGE_SIZE);
        assert(nullptr != data);
        memset(data, 0, PAGE_SIZE);

        if (0 != _pfmFileHandle.readPage(0, data)) {
            free(data);
            ERROR("Error while reading Head pointer of index file %s\n", _fileName.c_str());
            assert(0);
            return;
        }

        _rootPagePtr = *( (unsigned int*) data);
        free(data);
    }

    void IXFileHandle::writeRootNodePtrToDisk() {
        void* data = malloc(PAGE_SIZE);
        assert(nullptr != data);
        memset(data, 0, PAGE_SIZE);

        memmove(data, &_rootPagePtr, sizeof(unsigned int));

        if (0 != _pfmFileHandle.writePage(0, data)) {
            ERROR("Error while writing Head pointer of the index file %s\n", _fileName.c_str());
            free(data);
            assert(0);
            return;
        }
        free(data);
    }

    template<typename T>
    RC IndexManager::writePageToDisk(IXFileHandle& fileHandle, T& page, int pageNum) {
        void* data = malloc(PAGE_SIZE);
        assert(nullptr != data);
        memset(data, 0, PAGE_SIZE);

        // if pageNum is -1, create the page and insert the data
        if (-1 == pageNum) {
            auto newPageNum = fileHandle._pfmFileHandle.getNextPageNum();
            if (0 != fileHandle._pfmFileHandle.appendPage(data)) {
                free(data);
                ERROR("Can't create new page to write the new leaf/non-leaf page\n");
                return -1;
            }
            pageNum = newPageNum;
        }

        serializer->toBytes(page, data);

        assert(pageNum >= 0);
        if (0 != fileHandle._pfmFileHandle.writePage(pageNum, data)) {
            free(data);
            ERROR("Writing leaf/non-leaf page with pageNum %d resulted in failure\n", pageNum);
            return -1;
        }

        free(data);
        return 0;
    }

    PageNumAndKey insertAndSplitLeafPages(LeafPage& page1, LeafPage& page2, const RidAndKey& toInsert) {
        // insert the toInsert key-rid into the vector
        page1.insertEntry(toInsert, true /* force insert */);

        // determine the split
        // move the data to page2
        int elementsToMove = page1.getRidAndKeyPairs().size() / 2;

        auto tmpVec = page1.getRidAndKeyPairs();
        std::vector<RidAndKey>& vector1 = page1.getRidAndKeyPairs();
        std::vector<RidAndKey>& vector2 = page2.getRidAndKeyPairs();

        vector1.clear();
        vector1.insert(vector1.end(), tmpVec.begin(), tmpVec.begin() + elementsToMove);
        
        vector2.clear();
        vector2.insert(vector2.end(), tmpVec.begin() + elementsToMove, tmpVec.end());

        // set the metadata properly
        page1.resetMetadata();
        page2.resetMetadata();
        page2.setNextPageNum(page1.getNextPageNum());

        // return lowest in page2 as guide node to caller
        // we don't know the pageNum of the page2, that should be
        // set as guide node's pageNum by the caller
        RidAndKey guideNodeData = vector2.front();
        
        switch (page2.getKeyType().type) {
            case TypeInt:
                return PageNumAndKey(0, guideNodeData.getIntKey());
            case TypeReal:
                return PageNumAndKey(0, guideNodeData.getFloatKey());
            case TypeVarChar:
                return PageNumAndKey(0, guideNodeData.getStringKey());
        }
        assert(0);
        return PageNumAndKey();
    }

    PageNumAndKey insertAndSplitNonLeafPages(NonLeafPage& page1, NonLeafPage& page2, PageNumAndKey& toInsert) {
        page1.insertGuideNode(toInsert, toInsert.getPageNum(), true /* force insert */);

        // determine the split
        // move the data to page2
        int elementsToMove = page1.getPageNumAndKeys().size() / 2;

        auto tmpVec = page1.getPageNumAndKeys();
        page1.getPageNumAndKeys().clear();
        page1.getPageNumAndKeys().insert(page1.getPageNumAndKeys().end(),
                                         tmpVec.begin(),
                                         tmpVec.begin() + elementsToMove);
        page2.getPageNumAndKeys().clear();
        page2.getPageNumAndKeys().insert(page2.getPageNumAndKeys().end(),
                                         tmpVec.begin() + elementsToMove,
                                         tmpVec.end());

        // return lowest in page2 as guide node to caller
        PageNumAndKey guideNode;
        auto guideNodeData = page2.getPageNumAndKeys().front();

        // remove the first element in the page2, and add that pagenum as last element
        // in the page1. just add the pageNum, and not the key.. since that's how
        // the last entry in every non-leaf node is stored as
        page1.getPageNumAndKeys().push_back(PageNumAndKey(guideNodeData.getPageNum()));
        page2.getPageNumAndKeys().erase(page2.getPageNumAndKeys().begin());

        // set the metadata properly
        page1.resetMetadata();
        page2.resetMetadata();
        
        // we dont know the new page number, the caller should set that
        switch (page2.getKeyType().type) {
            case TypeInt:
                return PageNumAndKey(0, guideNodeData.getIntKey());
            case TypeReal:
                return PageNumAndKey(0, guideNodeData.getFloatKey());
            case TypeVarChar:
                return PageNumAndKey(0, guideNodeData.getStringKey());
        }
        assert(0);
        return PageNumAndKey();
    }

    RC IndexManager::insertHelper(IXFileHandle& fileHandle, PageNum node, const RidAndKey& entry, PageNumAndKey& newChild) {
        // read the page with the given pageNum
        void* data = malloc(PAGE_SIZE);
        assert(nullptr != data);
        memset(data, 0, PAGE_SIZE);

        if (0 != fileHandle._pfmFileHandle.readPage(node, data)) {
            free(data);
            ERROR("Error while reading page %d while inserting into index\n", node);
            return -1;
        }

        LeafPage leafPage;
        NonLeafPage nonLeafPage;
        bool isLeaf = deserializer->isLeafPage(data);

        if (isLeaf) {
            deserializer->toLeafPage(data, leafPage);
        }
        else {
            deserializer->toNonLeafPage(data, nonLeafPage);
        }

        free(data);

        // if node is non-leaf node, find the next page number to
        // insert the entry into that page num (recursive call)
        if (!isLeaf) {
            auto nextPage = nonLeafPage.findNextPage(entry);
            if (0 != insertHelper(fileHandle, nextPage, entry, newChild)) {
                return -1;
            }
            if (!newChild.isValid()) {
                // the above insert didn't result in any page split
                // hence i dont need to insert anything into the current page
                return 0;
            }

            // the above insert entry resulted into page split, so i have to
            // insert new entry (key and pageNum) into this page
            if (nonLeafPage.canInsert(newChild)) {
                nonLeafPage.insertGuideNode(newChild, newChild.getPageNum(), false);
                assert(0 == writePageToDisk(fileHandle, nonLeafPage, node));
                newChild.makeInvalid();
                return 0;
            }

            // can't insert the guiding entry into the page, so we'll have
            // to split the current page

            // create a new non-leaf page, set page num in newChild we return
            // split the page and write both the pages into disk
            NonLeafPage newNonLeafPage;
            PageNum newPageNum = fileHandle._pfmFileHandle.getNextPageNum();
            newChild = insertAndSplitNonLeafPages(nonLeafPage, newNonLeafPage, newChild);
            assert(0 == writePageToDisk(fileHandle, nonLeafPage, node));
            assert(0 == writePageToDisk(fileHandle, newNonLeafPage, -1 /* create page and insert*/));
            newChild.setPageNum(newPageNum);
            return 0;
        }
        else {
            if (leafPage.canInsert(entry)) {
                leafPage.insertEntry(entry, false);
                newChild.makeInvalid();
                return 0;
            }

            // can't insert to this page, so split the page
            LeafPage newLeafPage;
            PageNum newPageNum = fileHandle._pfmFileHandle.getNextPageNum();
            newChild = insertAndSplitLeafPages(leafPage, newLeafPage, entry);
            assert(0 == writePageToDisk(fileHandle, leafPage, node));
            assert(0 == writePageToDisk(fileHandle, newLeafPage, -1 /* create page and insert */));
            leafPage.setNextPageNum(newPageNum);
            newChild.setPageNum(newPageNum);
            return 0;
        }

        return 0;
    }

} // namespace PeterDB
