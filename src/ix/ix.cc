#include "src/include/ix.h"

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
        return -1;
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
