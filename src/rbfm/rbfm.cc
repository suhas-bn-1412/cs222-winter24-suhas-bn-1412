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
        auto retCode = m_pagedFileManager->openFile(fileName, fileHandle);
        if (0 != retCode) {
            return retCode;
        }

        // Check if PageSelector for this filename already exists
        auto it = m_pageSelectors.find(fileName);
        if (it == m_pageSelectors.end()) {
            // If not, create a new PageSelector and add it to the map
            PageSelector* pageSelector = new PageSelector(fileName, &fileHandle);
            m_pageSelectors[fileName] = pageSelector;
            pageSelector->readMetadataFromDisk();
        }

        return 0;
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        // Check if PageSelector for this filename exists
        auto it = m_pageSelectors.find(fileHandle.getFileName());
        if (it != m_pageSelectors.end()) {
            // Write metadata to disk and delete the PageSelector
            it->second->writeMetadataToDisk();
            delete it->second;
            m_pageSelectors.erase(it);
        }

        auto retCode = m_pagedFileManager->closeFile(fileHandle);
        if (0 != retCode) {
            return retCode;
        }
        return 0;
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

        unsigned prevPages = fileHandle.getNextPageNum();

        // 2. Determine pageNo
        //      a. scan all existing pages for sufficient space
        //      b. create (append to file) a new page if needed
        assert(m_pageSelectors.end() != m_pageSelectors.find(fileHandle.getFileName()));
        int pageNumber = m_pageSelectors[fileHandle.getFileName()]->selectPage(serializedRecordLength + Slot::SLOT_LENGTH_BYTES);
        assert(pageNumber != -1);

        if (prevPages < fileHandle.getNextPageNum()) {
            // meaning there was new page added
            // so initialise the available space metadata for that page
            m_page.initPageMetadata();
        }
        
        fileHandle.readPage(pageNumber, m_page.getDataPtr());

        unsigned short slotNum = m_page.generateSlotForInsertion(serializedRecordLength);
        RecordAndMetadata recordAndMetadata;
        recordAndMetadata.init(pageNumber, slotNum, false, serializedRecordLength, serializedRecord);
        m_page.insertRecord(&recordAndMetadata, slotNum);

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

        // 2. Load the page
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
        RecordAndMetadata recordAndMetadata;
        m_page.readRecord(&recordAndMetadata, slotNum);

        while (recordAndMetadata.isTombstone()) {
            RID updatedRid;
            memcpy(&updatedRid, recordAndMetadata.getRecordDataPtr(), sizeof(RID));

            // load the page of the updated record into memory
            if (0 != fileHandle.readPage(updatedRid.pageNum, m_page.getDataPtr())) {
                ERROR("Error while reading page %d\n", pageNum);
                return -1;
            }

            m_page.readRecord(&recordAndMetadata, slotNum);
        }

        // 4. *data <- transform to unserializedFormat(serializedRecord)
        RecordTransformer::deserialize(recordDescriptor, recordAndMetadata.getRecordDataPtr(), data);

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
        assert(rid.pageNum >= 0 && rid.pageNum < fileHandle.getNextPageNum());
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
                                            const void *data, const RID &existingRid) {
        // 1. serialize the record data
        unsigned short serializedRecordLength = RecordTransformer::serialize(recordDescriptor, data, nullptr);
        void *serializedRecord = malloc(serializedRecordLength);
        assert(nullptr != serializedRecord);
        RecordTransformer::serialize(recordDescriptor, data, serializedRecord);

        // 2. Load the record's page into memory
        fileHandle.readPage(existingRid.pageNum, m_page.getDataPtr());

        // 3. If the new record still fits into the original page, just update the record in-place.
        unsigned short oldLengthOfRecord = m_page.getRecordLengthBytes(existingRid.slotNum);
        unsigned short newLengthOfRecord = serializedRecordLength + RecordAndMetadata::RECORD_METADATA_LENGTH_BYTES;
        INFO("Updating record in page=%hu, slot=%hu. Old size=%hu, new size=%hu\n",
             existingRid.pageNum, existingRid.slotNum, oldLengthOfRecord,
             newLengthOfRecord);

        int growthInRecordLength = newLengthOfRecord - oldLengthOfRecord;
        if (growthInRecordLength <= 0 || m_page.canInsertRecord(growthInRecordLength - 4)) {
            // the updated record fits into the original page
            RecordAndMetadata recordAndMetadata;
            recordAndMetadata.init(existingRid.pageNum, existingRid.slotNum, false, serializedRecordLength, serializedRecord);
            m_page.updateRecord(&recordAndMetadata, existingRid.slotNum);
            fileHandle.writePage(existingRid.pageNum, m_page.getDataPtr());

        } else {
//          the updated record does not fit into the original page.
            //todo:
//1.        'clean-insert' the new record into any oher page.
            int updatedPageNum = computePageNumForInsertion(serializedRecordLength, fileHandle);
            assert(updatedPageNum != -1);

            if (updatedPageNum == fileHandle.getNumberOfPages()) {
                // create and append a new page to the file
                m_page.eraseAndReset();
                fileHandle.appendPage(m_page.getDataPtr());
            }

            fileHandle.readPage(updatedPageNum, m_page.getDataPtr());

            unsigned short updatedSlotNum = m_page.generateSlotForInsertion(serializedRecordLength);
            RecordAndMetadata freshRecordAndMetadata;
            freshRecordAndMetadata.init(existingRid.pageNum, existingRid.slotNum, false, serializedRecordLength, serializedRecord);
            m_page.insertRecord(&freshRecordAndMetadata, updatedSlotNum);
            fileHandle.writePage(updatedPageNum, m_page.getDataPtr());

            RID updatedRid;
            updatedRid.pageNum = updatedPageNum;
            updatedRid.slotNum = updatedSlotNum;

//            2. tombstone the old record, and link it to the new record.
            RecordAndMetadata tombstoneRecordAndMetadata;
            tombstoneRecordAndMetadata.init(existingRid.pageNum, existingRid.slotNum, true, sizeof(RID), &updatedRid);

            fileHandle.readPage(existingRid.pageNum, m_page.getDataPtr());
            m_page.updateRecord(&tombstoneRecordAndMetadata, existingRid.slotNum);
            fileHandle.writePage(existingRid.pageNum, m_page.getDataPtr());
        }

        free(serializedRecord);
        return 0;
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

