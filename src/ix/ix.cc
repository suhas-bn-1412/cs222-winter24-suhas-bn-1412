#include "src/include/ix.h"
#include "src/include/pageDeserializer.h"
#include "src/include/pageSerializer.h"
#include "src/include/varcharSerDes.h"

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
        if (m_indexesCreated.end() != m_indexesCreated.find(fileName)) {
            return -1;
        }

        if (0 != _pagedFileManager->createFile(fileName)) {
            return -1;
        }

        m_indexesCreated[fileName] = true;
        return 0;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        if (m_indexesCreated.end() == m_indexesCreated.find(fileName)) {
            return -1;
        }

        if (0 != _pagedFileManager->destroyFile(fileName)) {
            return -1;
        }

        m_indexesCreated.erase(fileName);
        return 0;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if (m_indexesCreated.end() == m_indexesCreated.find(fileName)) {
            return -1;
        }

        if (0 != _pagedFileManager->openFile(fileName, ixFileHandle._pfmFileHandle)) {
            ERROR("Error while opening the index file %s\n", fileName.c_str());
            return -1;
        }

        ixFileHandle._fileName = fileName;
        return 0;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        if ("" == ixFileHandle._fileName ||
            m_indexesCreated.end() == m_indexesCreated.find(ixFileHandle._fileName)) {
            return -1;
        }

        if (0 != _pagedFileManager->closeFile(ixFileHandle._pfmFileHandle)) {
            return -1;
        }

        ixFileHandle._fileName = "";
        return 0;
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
        // if dummy head is not created, create the head, and store the root node pointer
        if (0 == ixFileHandle._pfmFileHandle.getNextPageNum()) {
            void* data = malloc(PAGE_SIZE);
            assert(nullptr != data);
            memset(data, 0, PAGE_SIZE);

            if (0 != ixFileHandle._pfmFileHandle.appendPage(data)) {
                free(data);
                ERROR("Error while creating head node in index file %s\n", ixFileHandle._fileName.c_str());
                return -1;
            }
            free(data);
        }
        else {
            // when there is dummy head created, then read the head node to
            // find the root node pointer (page number of root node)
            ixFileHandle.fetchRootNodePtrFromDisk();
        }

        if (0 == ixFileHandle._rootPageNum) {
            // root page is not created yet, so create a new one and write
            // that to the disk. when first root node is created it will be
            // a LeafPage
            LeafPage leafPage;
            leafPage.setKeyType(attribute.type);

            auto newPageNum = ixFileHandle._pfmFileHandle.getNextPageNum();
            assert(0 == writePageToDisk(ixFileHandle, leafPage, -1 /* create page and insert */));
            assert(newPageNum == ixFileHandle._pfmFileHandle.getNextPageNum()-1);

            ixFileHandle._rootPageNum = newPageNum;
            ixFileHandle.writeRootNodePtrToDisk();
        }

        RidAndKey entryToInsert = createEntryToInsert(attribute, key, rid);

        PageNumAndKey newChild;
        if (0 != insertHelper(ixFileHandle, ixFileHandle._rootPageNum, entryToInsert, newChild)) {
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
            newRoot.setKeyType(attribute.type);

            PageNumAndKey entry1;
            PageNumAndKey entry2;

            switch (attribute.type) {
                case TypeInt:
                    entry1 = PageNumAndKey(ixFileHandle._rootPageNum, newChild.getIntKey());
                    entry2 = PageNumAndKey(newChild.getPageNum(), 0);
                    break;
                case TypeReal:
                    entry1 = PageNumAndKey(ixFileHandle._rootPageNum, newChild.getFloatKey());
                    entry2 = PageNumAndKey(newChild.getPageNum(), float(0));
                    break;
                case TypeVarChar:
                    entry1 = PageNumAndKey(ixFileHandle._rootPageNum, newChild.getStringKey());
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

            ixFileHandle._rootPageNum = newRootPageNum;
            ixFileHandle.writeRootNodePtrToDisk();
        }
        return 0;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        //todo: verify IXFileHandle is active (perhaps Suhas has done this func. already; wait)

        void *pageData = malloc(
                PAGE_SIZE); //todo: migrate to class member, perhaps Suhas has done this already, so wait for his commits
        unsigned int pageNum;

        ixFileHandle.fetchRootNodePtrFromDisk();
        pageNum = ixFileHandle._rootPageNum;
        loadPage(pageNum, pageData, ixFileHandle);

        // 2) recursively search for the leaf node that should contain the given (key, rid)
        while (!PageDeserializer::isLeafPage(pageData)) {
            // traverse down a level of the B+ index tree
            pageNum = getLowerLevelNode(key, attribute, pageData);
            loadPage(pageNum, pageData, ixFileHandle);
        }

        // 3) delete the (key, rid) entry from the leaf page, if it exists
        LeafPage leafPage;
        PageDeserializer::toLeafPage(pageData, leafPage);
        bool continueToNextPage = false;
        RC rc = deleteFromPage(key, rid, attribute, leafPage, continueToNextPage);

        while (continueToNextPage && (-1 != leafPage.getNextPageNum())) {
            // if not found, but the key spans into the next page, we need to check in next page
            pageNum = (unsigned) leafPage.getNextPageNum();

            memset(pageData, 0, PAGE_SIZE);

            loadPage(pageNum, pageData, ixFileHandle);
            PageDeserializer::toLeafPage(pageData, leafPage);

            continueToNextPage = false;
            rc = deleteFromPage(key, rid, attribute, leafPage, continueToNextPage);
        }
        if (continueToNextPage && (-1 == leafPage.getNextPageNum())) {
            return -1;
        }
        if (rc == 0) {
            // update freeByteCount
            unsigned int oldFreeByteCount = leafPage.getFreeByteCount();
            unsigned int newFreeByteCount = oldFreeByteCount + getKeySize(key, attribute);
            leafPage.setFreeByteCount(newFreeByteCount);

            assert(0 == writePageToDisk(ixFileHandle, leafPage, pageNum));
        }
        free(pageData);
        return rc;
    }

    void IndexManager::writePage(const void *pageData, unsigned int pageNum,
                                 IXFileHandle &ixFileHandle) const {
        ixFileHandle._pfmFileHandle.writePage(pageNum, pageData);
    }

    void IndexManager::loadPage(unsigned int pageNum, void *pageData,
                                IXFileHandle &ixFileHandle) const {
        if (0 != ixFileHandle._pfmFileHandle.readPage((unsigned) pageNum, pageData)) {
            ERROR("Error while reading page %d from file %s\n", pageNum, ixFileHandle._fileName.c_str());
            assert(0);
        }
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {

        if (m_indexesCreated.end() == m_indexesCreated.find(ixFileHandle._fileName) ||
            "" == ixFileHandle._fileName) {
            return -1;
        }

        void *pageData = malloc(
                PAGE_SIZE); //todo: migrate to class member, perhaps Suhas has done this already, so wait for his commits
        unsigned int pageNum;

        //todo:
        // 1) get root pageNum and load the root page
        ixFileHandle.fetchRootNodePtrFromDisk();
        pageNum = ixFileHandle._rootPageNum;
        loadPage(pageNum, pageData, ixFileHandle);

        // 2) recursively search for the leaf node that should contain the given lowKey
        // if the lowKey is null, simply traverse to the leftmost leafPage
        while (!PageDeserializer::isLeafPage(pageData)) {
            pageNum = getLowerLevelNode(lowKey, attribute, pageData);
            loadPage(pageNum, pageData, ixFileHandle);
        }

        ix_ScanIterator.init(&ixFileHandle, pageNum,
                             lowKey, lowKeyInclusive,
                             highKey, highKeyInclusive,
                             attribute);
        free(pageData);
        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        if (0 == ixFileHandle._rootPageNum) {
            ixFileHandle.fetchRootNodePtrFromDisk();

            assert(0 != ixFileHandle._rootPageNum);
        }

        switch (attribute.type) {
            case TypeInt:
                return intPrinter(ixFileHandle, ixFileHandle._rootPageNum, out);
            case TypeReal:
                return floatPrinter(ixFileHandle, ixFileHandle._rootPageNum, out);
            case TypeVarChar:
                return varcharPrinter(ixFileHandle, ixFileHandle._rootPageNum, out);
        }
        assert(0);
        return 0;
    }

    unsigned int
    IndexManager::getLowerLevelNode(const void *searchKey, const Attribute &attribute, const void* pageData) {
        NonLeafPage nonLeafPage;
        PageDeserializer::toNonLeafPage(pageData, nonLeafPage);

        std::vector<PageNumAndKey> &pageNumAndKeyPairs = nonLeafPage.getPageNumAndKeys();
        if (searchKey == nullptr) {
            // scenario used by scan() (searchKey = null implies traverse to the leftmost leafNode
            return pageNumAndKeyPairs.begin()->getPageNum();
        }

        for (const auto &pageNumAndKey: pageNumAndKeyPairs) {
            if (keyCompare(searchKey, attribute.type, pageNumAndKey) <= 0 || &pageNumAndKey == &pageNumAndKeyPairs.back()) {
                return pageNumAndKey.getPageNum();
            }
        }
        assert(0);
        return 0;
    }

    RC IndexManager::deleteFromPage(const void *targetKey, const RID &targetRid,
                                    const Attribute &targetKeyAttribute,
                                    PeterDB::LeafPage &leafPage, bool& continueToNextPage) {
        std::vector<RidAndKey> &ridAndKeyPairs = leafPage.getRidAndKeyPairs();
        std::vector<RidAndKey> updatedPairs;

        int numEntries = ridAndKeyPairs.size();
        if (0 == numEntries) {
            continueToNextPage = true;
            return 0;
        }
        else if (keyCompare(targetKey, targetRid, targetKeyAttribute.type, ridAndKeyPairs[numEntries-1]) == 0) {
            continueToNextPage = true;
        }

        bool found = false;
        for (auto ridAndKeyIter = ridAndKeyPairs.begin(); ridAndKeyIter != ridAndKeyPairs.end(); ++ridAndKeyIter) {
            if (keyCompare(targetKey, targetRid, targetKeyAttribute.type, *ridAndKeyIter) == 0) {
                // Skip the element to be deleted
                found = true;
                continue;
            }
            updatedPairs.push_back(*ridAndKeyIter);
        }

        // Check if the target key was found and deleted
        if (found) {
            // Replace the original vector with the updated one
            continueToNextPage = false;
            ridAndKeyPairs = std::move(updatedPairs);
            leafPage.resetMetadata();
            return 0;
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
    int IndexManager::keyCompare(const void *searchKey, const AttrType& searchKeyType,
                                 const PageNumAndKey &pageNumAndKeyPair) {
        switch (searchKeyType) {
            case TypeInt: {
                int keyA;
                memcpy((void *) &keyA, searchKey, sizeof(keyA));
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
                memcpy((void *) &keyA, searchKey, sizeof(keyA));
                float keyB = pageNumAndKeyPair.getFloatKey();
                if (keyA == keyB) {
                    return 0;
                } else if (keyA < keyB) {
                    return -1;
                } else {
                    return 1;
                }
            }
            case TypeVarChar: {
                const std::string keyA = VarcharSerDes::deserialize(searchKey);
                const std::string& keyB = pageNumAndKeyPair.getStringKey();
                return strcmp(keyA.data(), keyB.data());
            }
            default:
                ERROR("Unhandled Attribute type");
                assert(0);
        }
        assert(0);
        return 0;
    }

    /*
* Compares a search key with RidAndKey (handling all types)
* Return value:
* 0   : both the keys are equal
* < 0 : 'searchKey' < ridAndKeyPair.get___Key()
* > 0 : 'searchKey' > ridAndKeyPair.get___Key()
*/
    int IndexManager::keyCompare(const void *searchKey, const AttrType &searchKeyType,
                                 const RidAndKey &ridAndKeyPair) {
        const RID &sameRid = ridAndKeyPair.getRid();
        return keyCompare(searchKey, sameRid, searchKeyType, ridAndKeyPair);
    }

    /*
    * Compares a search key with RidAndKey (handling all types)
    * Return value:
    * 0   : both the keys are equal
    * < 0 : 'searchKey' < ridAndKeyPair.get___Key()
    * > 0 : 'searchKey' > ridAndKeyPair.get___Key()
    */
    int IndexManager::keyCompare(const void *searchKey, const RID searchRid, const AttrType &searchKeyType,
                                 const RidAndKey &ridAndKeyPair) {
        bool doRidsMatch = isEqual(searchRid, ridAndKeyPair.getRid());
        switch (searchKeyType) {
            case TypeInt: {
                int keyA;
                memcpy((void *) &keyA, searchKey, sizeof(keyA));
                const int keyB = ridAndKeyPair.getIntKey();
                if (keyA == keyB) {
                    return (doRidsMatch) ? 0 : -1;
                } else if (keyA < keyB) {
                    return -1;
                } else {
                    return 1;
                }
            }

            case TypeReal: {
                float keyA;
                memcpy((void *) &keyA, searchKey, sizeof(keyA));
                float keyB = ridAndKeyPair.getFloatKey();
                if (keyA == keyB) {
                    return (doRidsMatch) ? 0 : -1;
                } else if (keyA < keyB) {
                    return -1;
                } else {
                    return 1;
                }
            }
            case TypeVarChar: {
                const std::string keyA = VarcharSerDes::deserialize(searchKey);
                const std::string &keyB = ridAndKeyPair.getStringKey();
                int strCmpResult = strcmp(keyA.data(), keyB.data());
                if (strCmpResult == 0) {
                    return (doRidsMatch) ? 0 : -1;
                } else {
                    return strCmpResult;
                }
            }
            default:
                ERROR("Illegal Attribute type");
                assert(0);
        }
        assert(0);
        return 0;
    }

    bool IndexManager::isEqual(const RID &ridA, const RID& ridB) {
        return ridA.pageNum == ridB.pageNum
               && ridA.slotNum == ridB.slotNum;
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
        _pageData = malloc(PAGE_SIZE);
    }

    IX_ScanIterator::~IX_ScanIterator() {
        assert(_pageData != nullptr);
        free(_pageData);
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        // while at the end of current page
        while (atEndOfCurrentPage()) {
            // if more pages exist
            int nextPageNum = getNextLeafPage();
            if (nextPageNum != -1) {
                // load the next page
                loadLeafPage(nextPageNum);
            } else {
                // else return IX EOF
                return IX_EOF;
            }
        }

        if (wasPreviouslyReturnedEntryDeleted()) {
            _nextElementPositionOnPage--; // elements in the vector would
            // have been left-shifted by 1 to fill in the void of the
            // deleted entry; so, the 'next' entry is now at
            // the same position as the previous entry
        }

        // if the next record is within the endKey
        const RidAndKey &nextRidAndKey = _currentLeafPage.getRidAndKeyPairs().at(_nextElementPositionOnPage);
        if (isWithinRange(nextRidAndKey)) {
            // return the next record
            copy(rid, key, nextRidAndKey);
            _nextElementPositionOnPage++;
        } else {
            // else return IX EOF
            return IX_EOF;
        }
        _currentPageKeysCount = _currentLeafPage.getNumKeys();
        return 0;
    }

    RC IX_ScanIterator::close() {
        return 0;
    }

    void IX_ScanIterator::loadLeafPage(PageNum pageNum) {
        _ixFileHandle->_pfmFileHandle.readPage(pageNum, _pageData);
        assert(PageDeserializer::isLeafPage(_pageData));
        PageDeserializer::toLeafPage(_pageData, _currentLeafPage);
        _nextElementPositionOnPage = 0;
        _currentPageKeysCount = _currentLeafPage.getNumKeys();
    }

    void
    IX_ScanIterator::init(IXFileHandle *ixFileHandle, unsigned int pageNumBegin,
                          const void *startKey, const bool shouldIncludeStartKey,
                          const void *endKey, const bool shouldIncludeEndKey,
                          const Attribute &keyAttribute) {

        _ixFileHandle = ixFileHandle;
        _shouldIncludeEndKey = shouldIncludeEndKey;
        _keyType = keyAttribute.type;
        copyEndKey(endKey, keyAttribute);

        loadLeafPage(pageNumBegin);
        _nextElementPositionOnPage = getIndex(_currentLeafPage, startKey, shouldIncludeStartKey, _keyType);
    }

    void IX_ScanIterator::copyEndKey(const void *endKey, const Attribute &keyAttribute) {
        if (endKey == nullptr) {
            _endKey = nullptr;
        } else {
            unsigned int endKeySize = IndexManager::getKeySize(endKey, keyAttribute);
            _endKey = malloc(endKeySize);
            memcpy(_endKey, endKey, endKeySize);
        }
    }

    int IX_ScanIterator::getNextLeafPage() {
        return _currentLeafPage.getNextPageNum();
    }

    bool IX_ScanIterator::atEndOfCurrentPage() {
        const unsigned int numKeysInCurrentPage = _currentLeafPage.getRidAndKeyPairs().size();
        return _nextElementPositionOnPage == numKeysInCurrentPage;
    }

    bool IX_ScanIterator::isWithinRange(const RidAndKey &candidateRidAndKey) {
        if (_endKey == nullptr) {
            return true;
        }

        int comparisionResult = IndexManager::keyCompare(_endKey, candidateRidAndKey.getRid(), _keyType,
                                                         candidateRidAndKey);
        if (comparisionResult == 0 && _shouldIncludeEndKey) {
            return true;
        } else {
            return comparisionResult > 0;
        }
    }

    void IX_ScanIterator::copy(RID &destRid, void *destKey, const RidAndKey &srcRidAndKey) {
        destRid.pageNum = srcRidAndKey.getRid().pageNum;
        destRid.slotNum = srcRidAndKey.getRid().slotNum;
        switch (_keyType) {
            case TypeInt: {
                int srcKey = srcRidAndKey.getIntKey();
                memcpy(destKey, &srcKey, sizeof(srcKey));
            }
                break;
            case TypeReal: {
                float srcKey = srcRidAndKey.getFloatKey();
                memcpy(destKey, &srcKey, sizeof(srcKey));
            }
                break;
            case TypeVarChar: {
                const std::string &srcKey = srcRidAndKey.getStringKey();
                VarcharSerDes::serialize(srcKey, destKey);
            }
                break;
        }
    }

    unsigned int IX_ScanIterator::getIndex(LeafPage leafPage, const void *searchKey, const bool shouldIncludeSearchKey,
                                           const AttrType &keyType) {
        if (searchKey == nullptr) {
            return 0;
        }

        for (int index = 0; index < leafPage.getRidAndKeyPairs().size(); ++index) {
            const auto &ridAndKey = leafPage.getRidAndKeyPairs().at(index);

            // pass in the same RID for both keys as an RID comparision is out of place
            int comparisionResult = IndexManager::keyCompare(searchKey, keyType, ridAndKey);
            if (comparisionResult == 0 && shouldIncludeSearchKey) {
                return index;
            } else if (comparisionResult < 0) {
                return index;
            }
        }
        assert(1);
    }

    bool IX_ScanIterator::wasPreviouslyReturnedEntryDeleted() {
        if (_currentPageKeysCount == _currentLeafPage.getNumKeys() + 1) {
            _currentPageKeysCount--;
            return true;
        } else {
            return false;
        }
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

        _rootPageNum = *( (unsigned int*) data);
        free(data);
    }

    void IXFileHandle::writeRootNodePtrToDisk() {
        void* data = malloc(PAGE_SIZE);
        assert(nullptr != data);
        memset(data, 0, PAGE_SIZE);

        memmove(data, &_rootPageNum, sizeof(unsigned int));

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

        serializer->toBytes(page, data);

        // if pageNum is -1, create the page and insert the data
        if (-1 == pageNum) {
            auto newPageNum = fileHandle._pfmFileHandle.getNextPageNum();
            if (0 != fileHandle._pfmFileHandle.appendPage(data)) {
                free(data);
                ERROR("Can't create new page to write the new leaf/non-leaf page\n");
                return -1;
            }
        }
        else {
            assert(pageNum >= 0);
            if (0 != fileHandle._pfmFileHandle.writePage(pageNum, data)) {
                free(data);
                ERROR("Writing leaf/non-leaf page with pageNum %d resulted in failure\n", pageNum);
                return -1;
            }
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

        std::vector<RidAndKey> tmpVec = page1.getRidAndKeyPairs();
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
        
        switch (page2.getKeyType()) {
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

        std::vector<PageNumAndKey> tmpVec = page1.getPageNumAndKeys();
        std::vector<PageNumAndKey>& vector1 = page1.getPageNumAndKeys();
        std::vector<PageNumAndKey>& vector2 = page2.getPageNumAndKeys();

        vector1.clear();
        vector1.insert(vector1.end(), tmpVec.begin(), tmpVec.begin() + elementsToMove);
        
        vector2.clear();
        vector2.insert(vector2.end(), tmpVec.begin() + elementsToMove, tmpVec.end());

        // return lowest in page2 as guide node to caller
        PageNumAndKey guideNode = vector2.front();

        // remove the first element in the page2, and add that pagenum as last element
        // in the page1. just add the pageNum, and not the key.. since that's how
        // the last entry in every non-leaf node is stored as
        vector1.push_back(PageNumAndKey(guideNode.getPageNum()));
        vector2.erase(vector2.begin());

        // set the metadata properly
        page1.resetMetadata();
        page2.resetMetadata();
        
        // we dont know the new page number, the caller should set that
        switch (page2.getKeyType()) {
            case TypeInt:
                return PageNumAndKey(0, guideNode.getIntKey());
            case TypeReal:
                return PageNumAndKey(0, guideNode.getFloatKey());
            case TypeVarChar:
                return PageNumAndKey(0, guideNode.getStringKey());
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
            newNonLeafPage.setKeyType(nonLeafPage.getKeyType());
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
                assert(0 == writePageToDisk(fileHandle, leafPage, node));
                newChild.makeInvalid();
                return 0;
            }

            // can't insert to this page, so split the page
            LeafPage newLeafPage;
            newLeafPage.setKeyType(leafPage.getKeyType());
            PageNum newPageNum = fileHandle._pfmFileHandle.getNextPageNum();
            newChild = insertAndSplitLeafPages(leafPage, newLeafPage, entry);
            leafPage.setNextPageNum(newPageNum);
            assert(0 == writePageToDisk(fileHandle, leafPage, node));
            assert(0 == writePageToDisk(fileHandle, newLeafPage, -1 /* create page and insert */));
            newChild.setPageNum(newPageNum);
            return 0;
        }

        return 0;
    }

    void printIntegerLeafNode(LeafPage& leafPage, std::ostream &out) {
        std::map<int, std::vector<RID>> keyToRids;

        auto ridAndKeyVec = leafPage.getRidAndKeyPairs();
        for (auto &ridAndKey : ridAndKeyVec) {
            keyToRids[ridAndKey.getIntKey()].push_back(ridAndKey.getRid());
        }

        out << "{\"keys\": [";
        for (auto it = keyToRids.begin(); it != keyToRids.end(); ++it) {
            out << "\"" << it->first << ":";

            auto ridList = it->second;

            for (auto ridIt = ridList.begin(); ridIt != ridList.end(); ++ridIt) {
                RID rid = *ridIt;
                out << "(" << rid.pageNum << "," << rid.slotNum << ")";
                if (std::next(ridIt) != ridList.end()) {
                    out << ",";
                }
            }

            out << "\"";

            if (std::next(it) != keyToRids.end()) {
                out << ",";
            }
        }
        out << "]}";
    }

    RC IndexManager::intPrinter(IXFileHandle &ixFileHandle, PageNum node, std::ostream &out) const {
        void* data = malloc(PAGE_SIZE);
        assert(nullptr != data);
        memset(data, 0, PAGE_SIZE);

        if (0 != ixFileHandle._pfmFileHandle.readPage(node, data)) {
            ERROR("Error while reading page %d from file %s\n", node, ixFileHandle._fileName.c_str());
            free(data);
            return -1;
        }

        LeafPage leafPage;
        NonLeafPage nonLeafPage;

        if (deserializer->isLeafPage(data)) {
            deserializer->toLeafPage(data, leafPage);
            printIntegerLeafNode(leafPage, out);
        }
        else {
            deserializer->toNonLeafPage(data, nonLeafPage);

            std::vector<int> keyList;
            std::vector<PageNum> nodesList;

            std::vector<PageNumAndKey> pageNumAndKeys = nonLeafPage.getPageNumAndKeys();

            for (auto it : pageNumAndKeys) {
                keyList.push_back(it.getIntKey());
                nodesList.push_back(it.getPageNum());
            }

            // the last node in the keylist is invalid (only the pointer is valid in that entry)
            out << "{";
            out << "\"keys\": [";
            for (int i=0; i<keyList.size()-1; i++) {
                out << "\"" << keyList[i] << "\"";
                if (i != keyList.size()-2) {
                    out << ",";
                }
            }
            out << "],";

            // print eh childrens now
            out << "\"children\": [";
            for (auto childIt = nodesList.begin(); childIt != nodesList.end(); ++childIt) {
                intPrinter(ixFileHandle, *childIt, out);
                if (std::next(childIt) != nodesList.end()) {
                    out << ",";
                }
            }
            out << "]";

            out << "}";
        }

        free(data);

        return 0;
    }

    void printFloatLeafNode(LeafPage& leafPage, std::ostream &out) {
        std::map<float, std::vector<RID>> keyToRids;

        auto ridAndKeyVec = leafPage.getRidAndKeyPairs();
        for (auto &ridAndKey : ridAndKeyVec) {
            keyToRids[ridAndKey.getFloatKey()].push_back(ridAndKey.getRid());
        }

        out << "{\"keys\": [";
        for (auto it = keyToRids.begin(); it != keyToRids.end(); ++it) {
            out << "\"" << it->first << ":";

            auto ridList = it->second;

            for (auto ridIt = ridList.begin(); ridIt != ridList.end(); ++ridIt) {
                RID rid = *ridIt;
                out << "(" << rid.pageNum << "," << rid.slotNum << ")";
                if (std::next(ridIt) != ridList.end()) {
                    out << ",";
                }
            }

            out << "\"";

            if (std::next(it) != keyToRids.end()) {
                out << ",";
            }
        }
        out << "]}";
    }

    RC IndexManager::floatPrinter(IXFileHandle &ixFileHandle, PageNum node, std::ostream &out) const {
        void* data = malloc(PAGE_SIZE);
        assert(nullptr != data);
        memset(data, 0, PAGE_SIZE);

        if (0 != ixFileHandle._pfmFileHandle.readPage(node, data)) {
            ERROR("Error while reading page %d from file %s\n", node, ixFileHandle._fileName.c_str());
            free(data);
            return -1;
        }

        LeafPage leafPage;
        NonLeafPage nonLeafPage;

        if (deserializer->isLeafPage(data)) {
            deserializer->toLeafPage(data, leafPage);
            printFloatLeafNode(leafPage, out);
        }
        else {
            deserializer->toNonLeafPage(data, nonLeafPage);

            std::vector<float> keyList;
            std::vector<PageNum> nodesList;

            std::vector<PageNumAndKey> pageNumAndKeys = nonLeafPage.getPageNumAndKeys();

            for (auto it : pageNumAndKeys) {
                keyList.push_back(it.getFloatKey());
                nodesList.push_back(it.getPageNum());
            }

            // the last node in the keylist is invalid (only the pointer is valid in that entry)
            out << "{";
            out << "\"keys\": [";
            for (int i=0; i<keyList.size()-1; i++) {
                out << "\"" << keyList[i] << "\"";
                if (i != keyList.size()-2) {
                    out << ",";
                }
            }
            out << "],";

            // print eh childrens now
            out << "\"children\": [";
            for (auto childIt = nodesList.begin(); childIt != nodesList.end(); ++childIt) {
                floatPrinter(ixFileHandle, *childIt, out);
                if (std::next(childIt) != nodesList.end()) {
                    out << ",";
                }
            }
            out << "]";

            out << "}";
        }

        free(data);

        return 0;
    }

    void printVarcharLeafNode(LeafPage& leafPage, std::ostream &out) {
        std::map<std::string, std::vector<RID>> keyToRids;

        auto ridAndKeyVec = leafPage.getRidAndKeyPairs();
        for (auto &ridAndKey : ridAndKeyVec) {
            keyToRids[ridAndKey.getStringKey()].push_back(ridAndKey.getRid());
        }

        out << "{\"keys\": [";
        for (auto it = keyToRids.begin(); it != keyToRids.end(); ++it) {
            out << "\"" << it->first << ":";

            auto ridList = it->second;

            for (auto ridIt = ridList.begin(); ridIt != ridList.end(); ++ridIt) {
                RID rid = *ridIt;
                out << "(" << rid.pageNum << "," << rid.slotNum << ")";
                if (std::next(ridIt) != ridList.end()) {
                    out << ",";
                }
            }

            out << "\"";

            if (std::next(it) != keyToRids.end()) {
                out << ",";
            }
        }
        out << "]}";
    }

    RC IndexManager::varcharPrinter(IXFileHandle &ixFileHandle, PageNum node, std::ostream &out) const {
        void* data = malloc(PAGE_SIZE);
        assert(nullptr != data);
        memset(data, 0, PAGE_SIZE);

        if (0 != ixFileHandle._pfmFileHandle.readPage(node, data)) {
            ERROR("Error while reading page %d from file %s\n", node, ixFileHandle._fileName.c_str());
            free(data);
            return -1;
        }

        LeafPage leafPage;
        NonLeafPage nonLeafPage;

        if (deserializer->isLeafPage(data)) {
            deserializer->toLeafPage(data, leafPage);
            printVarcharLeafNode(leafPage, out);
        }
        else {
            deserializer->toNonLeafPage(data, nonLeafPage);

            std::vector<std::string> keyList;
            std::vector<PageNum> nodesList;

            std::vector<PageNumAndKey> pageNumAndKeys = nonLeafPage.getPageNumAndKeys();

            for (auto it : pageNumAndKeys) {
                keyList.push_back(it.getStringKey());
                nodesList.push_back(it.getPageNum());
            }

            // the last node in the keylist is invalid (only the pointer is valid in that entry)
            out << "{";
            out << "\"keys\": [";
            for (int i=0; i<keyList.size()-1; i++) {
                out << "\"" << keyList[i] << "\"";
                if (i != keyList.size()-2) {
                    out << ",";
                }
            }
            out << "],";

            // print eh childrens now
            out << "\"children\": [";
            for (auto childIt = nodesList.begin(); childIt != nodesList.end(); ++childIt) {
                varcharPrinter(ixFileHandle, *childIt, out);
                if (std::next(childIt) != nodesList.end()) {
                    out << ",";
                }
            }
            out << "]";

            out << "}";
        }

        free(data);

        return 0;
    }

} // namespace PeterDB
