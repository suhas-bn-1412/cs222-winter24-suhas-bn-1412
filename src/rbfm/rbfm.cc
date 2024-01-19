#include "src/include/rbfm.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        pagedFileManager.createFile(fileName);
        return 0;
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        pagedFileManager.destroyFile(fileName);
        return -1;
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        pagedFileManager.openFile(fileName, fileHandle);
        return -1;
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        pagedFileManager.closeFile(fileHandle);
        return -1;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        // 1. serializedRecord, serializedRecordlength = transform record data into page-storage format
        // 2. Determine pageNo
        //      a. scan all existing pages for sufficient space
        //      b. create (append to file) a new page if needed
        // 3. Page page = PFM.readPage(pageNo)
        // 4. page.insertRecord(serializedRecord, serializedRecordLength)
        return -1;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        // 1. pageNo = RID.pageNo
        // 2. Page page = PFM.readPage(pageNo)
        // 3. serializedRecord = page.readRecord(slotNo)
        // 4. *data <- transform to unserializedFormat(serializedRecord)
        return -1;
    }

    /*
     * End of CS222 Project1 impl
     */

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        return -1;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

