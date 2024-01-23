#include "src/include/rbfm.h"
#include "src/include/page.h"
#include "src/include/recordTransformer.h"
#include "src/include/util.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    //todo: fix compile error
//    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

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
        unsigned short serializedRecordLength = RecordTransformer::getSerializedDataLength(recordDescriptor, data);
        void *serializedRecordData = malloc(serializedRecordLength);

        // 2. Determine pageNo
        //      a. scan all existing pages for sufficient space
        //      b. create (append to file) a new page if needed

        PageNum pageNum;
        INFO("Determining page number for new record insertion");
        if (fileHandle.getNumberOfPages() == 0) {
            INFO("No pages found in file. Appending new page");
            pageNum = 0;
            Page *page = new Page();
            fileHandle.appendPage(page->getDataPtr());
        } else {
            for (pageNum = 0; pageNum < fileHandle.getNumberOfPages(); ++pageNum) {
                Page *page = new Page(); //todo: refactor out to loop to optimize memory alloc
                fileHandle.readPage(pageNum, page->getDataPtr());
                if (page->canInsertRecord(serializedRecordLength)) {
                    break;
                }
            }
        }

        INFO("Page number = %ui", pageNum);
        Page *page = new Page(); //todo: try to reuse from before
        fileHandle.readPage(pageNum, page->getDataPtr());
        unsigned short slotNum = page->insertRecord(serializedRecordData, serializedRecordLength);
        rid.pageNum = pageNum;
        rid.slotNum = slotNum;
        fileHandle.writePage(pageNum, page->getDataPtr());
        INFO("Inserted record into page=%ui, slot=%us", pageNum, slotNum);

        free(serializedRecordData);
        return -1;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        // 1. pageNo = RID.pageNo
        PageNum pageNum = rid.pageNum;

        // 2. Page page = PFM.readPage(pageNo)
        Page *page = new Page();
        fileHandle.readPage(pageNum, page->getDataPtr());

        // 3. serializedRecord = page.readRecord(slotNo)
        unsigned short slotNo = rid.slotNum;
        byte serializedRecordLengthBytes = page->getRecordLengthBytes(slotNo);
        void *serializedRecord = malloc(serializedRecordLengthBytes);
        page->readRecord(slotNo, serializedRecord);

        // 4. *data <- transform to unserializedFormat(serializedRecord)
        RecordTransformer::deserialize(recordDescriptor, serializedRecord, data);
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

