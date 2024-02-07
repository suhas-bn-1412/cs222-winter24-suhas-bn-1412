#include "src/include/rbfm.h"
#include "src/include/util.h"
#include "src/include/slot.h"
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

        unsigned pageNumber = computePageNumForInsertion(serializedRecordLength, fileHandle);
        
        m_page.readPage(fileHandle, pageNumber);

        unsigned short slotNum = m_page.generateSlotForInsertion(serializedRecordLength);
        RecordAndMetadata recordAndMetadata;
        recordAndMetadata.init(pageNumber, slotNum, false, serializedRecordLength, serializedRecord);
        m_page.insertRecord(&recordAndMetadata, slotNum);

        rid.pageNum = pageNumber;
        rid.slotNum = slotNum;
        INFO("Inserted record into page=%hu, slot=%hu\n", rid.pageNum, rid.slotNum);
        if (0 != m_page.writePage(fileHandle, pageNumber)) {
            ERROR("Error while writing the page %d\n", pageNumber);
            return -1;
        }

        free(serializedRecord);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        std::vector<std::string> attrNames;
        for (auto &attrInfo : recordDescriptor) {
            attrNames.push_back(attrInfo.name);
        }

        return readRecordWithAttrFilter(fileHandle, recordDescriptor, attrNames, rid, data);
    }

    RC RecordBasedFileManager::readRecordWithAttrFilter(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                                        const std::vector<std::string> &attributeNames, const RID &rid, void *data) {
        // 1. pageNo = RID.pageNo
        PageNum pageNum = rid.pageNum;

        // 2. Load the page
        if (0 != m_page.readPage(fileHandle, pageNum)) {
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
            if (0 != m_page.readPage(fileHandle, updatedRid.pageNum)) {
                ERROR("Error while reading page %d\n", pageNum);
                return -1;
            }

            m_page.readRecord(&recordAndMetadata, slotNum);
        }

        // 4. *data <- transform to unserializedFormat(serializedRecord)
        RecordTransformer::deserialize(recordDescriptor, attributeNames, recordAndMetadata.getRecordDataPtr(), data);

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
        m_page.readPage(fileHandle, rid.pageNum);

        m_pageSelectors[fileHandle.getFileName()]->decrementAvailableSpace(rid.pageNum, -1 * m_page.getSlot(rid.slotNum).getRecordLengthBytes());

//        2. page.deleteRecord(rid.slotNum)
        m_page.deleteRecord(rid.slotNum);

//      3. write through - current page from memory to file
        if (0 != m_page.writePage(fileHandle, rid.pageNum)) {
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
        m_page.readPage(fileHandle, existingRid.pageNum);

        // 3. If the new record still fits into the original page, just update the record in-place.
        unsigned short oldLengthOfRecord = m_page.getRecordLengthBytes(existingRid.slotNum);
        unsigned short newLengthOfRecord = serializedRecordLength;
        INFO("Updating record in page=%hu, slot=%hu. Old size=%hu, new size=%hu\n",
             existingRid.pageNum, existingRid.slotNum, oldLengthOfRecord,
             newLengthOfRecord);

        int growthInRecordLength = newLengthOfRecord - oldLengthOfRecord;
        if (growthInRecordLength <= 0 || m_page.canInsertRecord(growthInRecordLength - 4)) {
            // the updated record fits into the original page
            RecordAndMetadata recordAndMetadata;
            recordAndMetadata.init(existingRid.pageNum, existingRid.slotNum, false, serializedRecordLength, serializedRecord);
            m_page.updateRecord(&recordAndMetadata, existingRid.slotNum);
            m_page.writePage(fileHandle, existingRid.pageNum);
            m_pageSelectors[fileHandle.getFileName()]->decrementAvailableSpace(existingRid.pageNum, growthInRecordLength);

        } else {
//          the updated record does not fit into the original page.
            //todo:
//1.        'clean-insert' the new record into any oher page.
            int updatedPageNum = computePageNumForInsertion(serializedRecordLength, fileHandle);
            assert(updatedPageNum != -1);

            m_page.readPage(fileHandle, updatedPageNum);

            unsigned short updatedSlotNum = m_page.generateSlotForInsertion(serializedRecordLength);
            RecordAndMetadata freshRecordAndMetadata;
            freshRecordAndMetadata.init(existingRid.pageNum, existingRid.slotNum, false, serializedRecordLength, serializedRecord);
            m_page.insertRecord(&freshRecordAndMetadata, updatedSlotNum);
            m_page.writePage(fileHandle, updatedPageNum);

            RID updatedRid;
            updatedRid.pageNum = updatedPageNum;
            updatedRid.slotNum = updatedSlotNum;

//            2. tombstone the old record, and link it to the new record.
            RecordAndMetadata tombstoneRecordAndMetadata;
            tombstoneRecordAndMetadata.init(existingRid.pageNum, existingRid.slotNum, true, sizeof(RID), &updatedRid);

            m_page.readPage(fileHandle, existingRid.pageNum);
            m_page.updateRecord(&tombstoneRecordAndMetadata, existingRid.slotNum);
            m_page.writePage(fileHandle, existingRid.pageNum);
            m_pageSelectors[fileHandle.getFileName()]->decrementAvailableSpace(existingRid.pageNum, sizeof(RID) - oldLengthOfRecord);
        }

        free(serializedRecord);
        return 0;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        auto attrNames = std::vector<std::string>();
        attrNames.push_back(attributeName);

        Attribute attrInfo;
        for (auto &record : recordDescriptor) {
            if (attributeName == record.name) {
                attrInfo = record;
                break;
            }
        }
        assert(attrInfo.name != "");

        auto nullFlagSz = (recordDescriptor.size() + 7) / 8;

        // to read varchar we need 4 extra byte in the beginning to know the length of it
        void *dataToBeRead = malloc(nullFlagSz + ((attrInfo.type == TypeVarChar) ? (VARCHAR_ATTR_LEN_SZ + attrInfo.length) : attrInfo.length));
        assert(nullptr != dataToBeRead);

        auto rr = readRecordWithAttrFilter(fileHandle, recordDescriptor, attrNames, rid, dataToBeRead);
        if (0 != rr) {
            ERROR("Error while reading attribute %s in record P.%d S.%d \n", attributeName, rid.pageNum, rid.slotNum);
            free(dataToBeRead);
            return rr;
        }

        void *tmp = dataToBeRead;
        dataToBeRead = (void*)( (char*)dataToBeRead + nullFlagSz );
        if (TypeVarChar == attrInfo.type) {
            auto len = *( (uint32_t*) dataToBeRead );
            memmove(data, (void*)( (char*) dataToBeRead + VARCHAR_ATTR_LEN_SZ ), len);
        } else {
            memmove(data, dataToBeRead, INT_SZ);
        }

        free(tmp);
        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        rbfm_ScanIterator.init(this, &fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
        return 0;
    }

    unsigned RecordBasedFileManager::computePageNumForInsertion(unsigned recordLength, FileHandle &fileHandle) {
        unsigned prevPages = fileHandle.getNextPageNum();

        assert(m_pageSelectors.end() != m_pageSelectors.find(fileHandle.getFileName()));
        int pageNumber = m_pageSelectors[fileHandle.getFileName()]->selectPage(recordLength +
                                                                               RecordAndMetadata::RECORD_METADATA_LENGTH_BYTES +
                                                                               Slot::SLOT_METADATA_LENGTH_BYTES);
        assert(pageNumber != -1);

        if (prevPages < fileHandle.getNextPageNum()) {
            // meaning there was new page added
            // so initialise the available space metadata for that page
            appendFreshPage(pageNumber, fileHandle);
        }
        return pageNumber;
    }

    void RecordBasedFileManager::appendFreshPage(int pageNumber, FileHandle &fileHandle) {
        auto rp = m_page.readPage(fileHandle, pageNumber);
        assert(0 == rp);
        m_page.eraseAndReset();
        auto rc = m_page.writePage(fileHandle, pageNumber);
        assert(rc == 0);
    }

    bool RecordBasedFileManager::isValidDataPage(FileHandle &fileHandle, PageNum pageNum) {
        assert(true == fileHandle.isActive());

        // check if the page number is under the max page
        if (pageNum >= fileHandle.getNextPageNum()) {
            return false;
        }

        // check if the page is one of the metadata pages
        assert(m_pageSelectors.end() != m_pageSelectors.find(fileHandle.getFileName()));
        if (m_pageSelectors[fileHandle.getFileName()]->isThisPageAMetadataPage(pageNum)) {
            return false;
        }

        return true;
    }

    bool RecordBasedFileManager::isValidRid(FileHandle &fileHandle, const RID &rid) {
        if (!isValidDataPage(fileHandle, rid.pageNum)) {
            return false;
        }

        // check if given slot is present in this page
        if ((m_page.getCurrentPage() == -1) || (m_page.getCurrentPage() != rid.pageNum)) {
            auto rp = m_page.readPage(fileHandle, rid.pageNum);
            assert(0 == rp);

            if (0 != rp) {
                ERROR("Error while reading the page %d from file %s \n", rid.pageNum, fileHandle.getFileName());
                return false;
            }
        }

        if (rid.slotNum >= m_page.getSlotCount()) {
            return false;
        }

        RecordAndMetadata recordAndMetadata;
        m_page.readRecord(&recordAndMetadata, rid.slotNum);
        return !recordAndMetadata.isTombstone();
    }

    bool RecordBasedFileManager::maxSlotBreached(FileHandle &fileHandle, const RID &rid) {
        if (!isValidDataPage(fileHandle, rid.pageNum)) {
            return true;
        }

        // check if given slot is present in this page
        if ((m_page.getCurrentPage() == -1) || (m_page.getCurrentPage() != rid.pageNum)) {
            auto rp = m_page.readPage(fileHandle, rid.pageNum);
            assert(0 == rp);

            if (0 != rp) {
                ERROR("Error while reading the page %d from file %s \n", rid.pageNum, fileHandle.getFileName());
                return true;
            }
        }

        if (rid.slotNum >= m_page.getSlotCount()) {
            return true;
        }

        return false;
    }

    void RBFM_ScanIterator::init(RecordBasedFileManager *rbfm, FileHandle *fileHandle,
                                 const std::vector<Attribute> &recordDescriptor, const std::string &conditionAttribute,
                                 const CompOp compOp, const void *value, const std::vector<std::string> &attributeNames) {
        m_initDone = true;
        m_rbfm = rbfm;
        m_fileHandle = fileHandle;
        m_recodrdDescriptor = recordDescriptor;
        m_conditionAttribute = conditionAttribute;
        m_compOp = compOp;
        m_value = value;
        m_attributeNames = attributeNames;
    }

    bool RBFM_ScanIterator::pickNextValidRID() {
        bool samePage = true;
        // check if the scan has started, else start from the first page in the file
        if (!m_scanStarted) {
            samePage = false;
            m_scanStarted = true;

            m_currentRid.pageNum = 0;
            m_currentRid.slotNum = 0;

            while (!m_rbfm->isValidDataPage(*m_fileHandle, m_currentRid.pageNum)) {
                m_currentRid.pageNum += 1;
            }
        }

        if (samePage) {
            m_currentRid.slotNum += 1;
        }

        // at this point we have potential RID, we just have to validate we have
        // valid slotNum and valid pageNum ans read the record
        bool slotsExhausted = false;
        bool pagesExhausted = false;

        while (!pagesExhausted && !m_rbfm->isValidRid(*m_fileHandle, m_currentRid)) {
            slotsExhausted = false;
            while (!slotsExhausted) {
                m_currentRid.slotNum += 1;

                if (m_rbfm->maxSlotBreached(*m_fileHandle, m_currentRid)) {
                    slotsExhausted = true;
                }
            }

            if (slotsExhausted) {
                // slots are exhausted, go to next page
                unsigned lastPage = m_fileHandle->getNextPageNum()-1;
                if (m_currentRid.pageNum == lastPage) {
                    pagesExhausted = true;
                }
                else {
                    m_currentRid.pageNum += 1;
                    m_currentRid.slotNum = 0;
                }
            }
        }

        return !pagesExhausted;
    }

    template <typename T>
    bool compare(const CompOp &op, const T& a, const T& b) {
        switch (op) {
            case EQ_OP:
                return a == b;
            case LT_OP:
                return a < b;
            case LE_OP:
                return a <= b;
            case GT_OP:
                return a > b;
            case GE_OP:
                return a >= b;
            case NE_OP:
                return a != b;
            case NO_OP:
                return true;
        }
        return false;
    }

    int varcharCompare(const CompOp &op, const uint32_t &len, const char *str1, const char *str2) {
        int result = strncmp(str1, str2, len);

        switch (op) {
            case EQ_OP:
                return result == 0;
            case LT_OP:
                return result < 0;
            case LE_OP:
                return result <= 0;
            case GT_OP:
                return result > 0;
            case GE_OP:
                return result >= 0;
            case NE_OP:
                return result != 0;
            case NO_OP:
                return true;
        }
        return false;
    }

    bool getComparisonResult(const AttrType &attrType, const CompOp &compOp,
                             const void *data, const void *expected) {
        switch (attrType) {
            case TypeInt:
                return compare(compOp, *static_cast<const int*>(data), *static_cast<const int*>(expected));
            case TypeReal:
                return compare(compOp, *static_cast<const float*>(data), *static_cast<const float*>(expected));
            case TypeVarChar:
                // in case of varchar, in the expected value and data the first 4 bytes
                // has the length of the varchar string and next <n> bytes has the data
                uint32_t len1 = * ( (uint32_t*) data);
                uint32_t len2 = * ( (uint32_t*) expected);
                if (len1 != len2) {
                    return false;
                }
                return varcharCompare(compOp, len1, ( (char*) data + VARCHAR_ATTR_LEN_SZ ), ( (char*)expected + VARCHAR_ATTR_LEN_SZ ) );
        }
        return false;
    }

    bool RBFM_ScanIterator::recordSatisfiesCondition() {
        if (NO_OP == m_compOp) {
            return true;
        }

        // read the attribute with conditionAttrName from record m_currentRid
        // then compare the values and return true or false

        void *data = nullptr;

        AttrType condAttrType = TypeInt;

        for (auto &recordInfo : m_recodrdDescriptor) {
            if (recordInfo.name == m_conditionAttribute) {
                data = malloc(recordInfo.length);
                memset(data, 0, recordInfo.length);
                condAttrType = recordInfo.type;

                break;
            }
        }

        assert(data != nullptr);

        auto ra = m_rbfm->readAttribute(*m_fileHandle, m_recodrdDescriptor, m_currentRid, m_conditionAttribute, data);
        if (0 != ra) {
            ERROR("Error while reading attribute %s from record with pageNum %u and slotNum %u\n", m_conditionAttribute, m_currentRid.pageNum, m_currentRid.slotNum);
            free(data);
            return false;
        }

        bool comparisonResult = getComparisonResult(condAttrType, m_compOp, data, m_value);
        free(data);
        return comparisonResult;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        assert(true == m_initDone);

        if (!pickNextValidRID())
            return RBFM_EOF;

        // we have valid next RID to read, read that data
        auto rr = m_rbfm->readRecordWithAttrFilter(*m_fileHandle, m_recodrdDescriptor, m_attributeNames, m_currentRid, data);
        if (0 != rr) {
            ERROR("Error while reading record with pageNum - %u and slotNum - %u", m_currentRid.pageNum, m_currentRid.slotNum);
            return rr;
        }

        // check for condition attr if it satisfies then return the data
        // else call this function recursively
        if (recordSatisfiesCondition()) {
            rid = m_currentRid;
            return 0;
        }

        return getNextRecord(rid, data);
    }

} // namespace PeterDB

