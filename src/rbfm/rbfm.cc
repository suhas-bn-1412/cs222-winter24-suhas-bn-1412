#include "src/include/rbfm.h"
#include "src/include/recordTransformer.h"

#include <assert.h>
#include <iostream>

namespace PeterDB {

    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        _rbf_manager.m_pagedFileManager = &PagedFileManager::instance();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

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
        m_page.eraseData();
        if (fileHandle.getNumberOfPages() == 0) {
            pageNum = 0;
            fileHandle.appendPage(m_page.getDataPtr());
        } else {
            bool pageFound = false;
            int i = fileHandle.getNumberOfPages()-1;
            while (!pageFound && i >= 0) {
                if ( 0 != fileHandle.readPage(i, m_page.getDataPtr())) {
                    std::cerr << "Error while reading the page " << i << "\n";
                    return -1;
                }
                if (m_page.canInsertRecord(serializedRecordLength)) {
                    pageNum = i;
                    pageFound = true;
                }
                else {
                    m_page.eraseData();
                }
                i--;
            }
            if (!pageFound) {
                pageNum = fileHandle.getNumberOfPages();
                if (0 != fileHandle.appendPage(m_page.getDataPtr())) {
                    std::cerr << "Error while appending the page " << pageNum << "\n";
                    return -1;
                }
            }
        }

        unsigned short slotNum = m_page.insertRecord(serializedRecord, serializedRecordLength);
        rid.pageNum = pageNum;
        rid.slotNum = slotNum;
        if (0 != fileHandle.writePage(pageNum, m_page.getDataPtr())) {
            std::cerr << "Error while writing the page " << pageNum << "\n";
            return -1;
        }

        free(serializedRecord);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        // 1. pageNo = RID.pageNo
        PageNum pageNum = rid.pageNum;

        // 2. Page page = PFM.readPage(pageNo)
        if ( 0 != fileHandle.readPage(pageNum, m_page.getDataPtr())) {
            std::cerr << "Error while reading the page " << pageNum << "\n";
            return -1;
        }

        // 3. serializedRecord = page.readRecord(slotNo)
        unsigned short slotNo = rid.slotNum;
        byte serializedRecordLengthBytes = m_page.getRecordLengthBytes(slotNo);
        void *serializedRecord = malloc(serializedRecordLengthBytes);
        m_page.readRecord(slotNo, serializedRecord);

        // 4. *data <- transform to unserializedFormat(serializedRecord)
        RecordTransformer::deserialize(recordDescriptor, serializedRecord, data);

        free(serializedRecord);
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        RecordTransformer::print(recordDescriptor, data, out);
        return 0;
    }

    /*
     * End of CS222 Project1 impl
     */

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
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

