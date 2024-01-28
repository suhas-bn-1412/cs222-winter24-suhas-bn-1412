#include "src/include/rbfm.h"
#include "src/include/recordTransformer.h"
#include "src/include/util.h"

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
        unsigned short serializedRecordLength = RecordTransformer::serialize(recordDescriptor, data, nullptr);
        void *serializedRecord = malloc(serializedRecordLength);
        assert(nullptr != serializedRecord);
        RecordTransformer::serialize(recordDescriptor, data, serializedRecord);

        // 2. Determine pageNo
        //      a. scan all existing pages for sufficient space
        //      b. create (append to file) a new page if needed
        int pageNumber = computePageNumForInsertion(serializedRecordLength, fileHandle);
        assert(pageNumber != -1);

        if (pageNumber == fileHandle.getNumberOfPages()) {
            m_page.eraseAndReset();
            fileHandle.appendPage(m_page.getDataPtr());
        }

        fileHandle.readPage(pageNumber, m_page.getDataPtr());
        unsigned short slotNum = m_page.insertRecord(serializedRecord, serializedRecordLength);
        rid.pageNum = pageNumber;
        rid.slotNum = slotNum;
        INFO("Inserted record into page=%hu, slot=%hu\n", rid.pageNum, rid.slotNum);
        if (0 != fileHandle.writePage(pageNumber, m_page.getDataPtr())) {
            ERROR("Error while writing the page %d\n", pageNumber);
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
        if (0 != fileHandle.readPage(pageNum, m_page.getDataPtr())) {
            ERROR("Error while reading page %d\n", pageNum);
            return -1;
        }

        // 3. serializedRecord = page.readRecord(slotNum)
        unsigned short slotNum = rid.slotNum;
        unsigned short serializedRecordLengthBytes = m_page.getRecordLengthBytes(slotNum);
        if (serializedRecordLengthBytes == 0) {
            WARNING("Cannot read record on page=%hu, slot=%hu as it was previously deleted", rid.pageNum, rid.slotNum);
            return -1;
        }

        INFO("Reading record of size=%hu from page=%hu, slot=%hu\n", serializedRecordLengthBytes, rid.pageNum,
               rid.slotNum);
        void *serializedRecord = malloc(serializedRecordLengthBytes);
        m_page.readRecord(slotNum, serializedRecord);

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


    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
//        1. read the page indicated by rid.pageNum into memory (m_page)
        assert(rid.pageNum >= 0 && rid.pageNum < fileHandle.getNumberOfPages());
        fileHandle.readPage(rid.pageNum, m_page.getDataPtr());

//        2. page.deleteRecord(rid.slotNum)
        m_page.deleteRecord(rid.slotNum);

//      3. write through - current page from memory to file
        if (0 != fileHandle.writePage(rid.pageNum, m_page.getDataPtr())) {
            ERROR("Error while writing the page %d\n", rid.pageNum);
            return -1;
        }

        INFO("Deleted record from page=%hu, slot=%hu", rid.pageNum, rid.slotNum);
        return 0;
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

    int RecordBasedFileManager::computePageNumForInsertion(unsigned short recordLength, FileHandle &fileHandle) {
        if (fileHandle.getNumberOfPages() == 0) {
            return 0;
        }
        for (int potentialPageNum = fileHandle.getNumberOfPages() - 1; potentialPageNum >= 0; potentialPageNum--) {
            if (fileHandle.readPage(potentialPageNum, m_page.getDataPtr()) != 0) {
                ERROR("Error while reading page %d\n", potentialPageNum);
                return -1;
            }
            if (m_page.canInsertRecord(recordLength)) {
                return potentialPageNum;
            }
        }
        return fileHandle.getNumberOfPages();
    }

} // namespace PeterDB

