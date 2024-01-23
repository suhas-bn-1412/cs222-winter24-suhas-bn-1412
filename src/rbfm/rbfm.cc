#include "src/include/rbfm.h"
#include "src/include/page.h"
#include "src/include/recordTransformer.h"
#include "src/include/util.h"

#include <assert.h>

namespace PeterDB {

    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        _rbf_manager.m_pagedFileManager = &PagedFileManager::instance();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    //todo: fix compile error
//    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return m_pagedFileManager->createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return m_pagedFileManager->destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return m_pagedFileManager->openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return m_pagedFileManager->closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {

        // get the length of the serialised data
        // then allocate that much memory and then
        // serialize the data into that memory
        auto serializedRecordLength = RecordTransformer::serialize(recordDescriptor, data, nullptr);
        void *serializedRecord = malloc(serializedRecordLength);
        assert(nullptr != serializedRecord);

        RecordTransformer::serialize(recordDescriptor, data, serializedRecord);

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
        unsigned short slotNum = page->insertRecord(serializedRecord, serializedRecordLength);
        rid.pageNum = pageNum;
        rid.slotNum = slotNum;
        fileHandle.writePage(pageNum, page->getDataPtr());
        INFO("Inserted record into page=%ui, slot=%us", pageNum, slotNum);

        free(serializedRecord);
        return 0;
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

        return 0;
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

