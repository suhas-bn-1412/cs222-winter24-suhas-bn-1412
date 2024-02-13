#include "src/include/page.h"


namespace PeterDB {
    Page::Page() {
        initPageMetadata();
    }

    Page::~Page() {
        delete[] m_data;
        m_data = nullptr;
    }

    void Page::initPageMetadata() {
        setFreeByteCount(PAGE_SIZE - PAGE_METADATA_SIZE);
        setSlotCount(0);
    }

    RC Page::readPage(FileHandle &fileHandle, PageNum pageNum) {
        // if we have same file and same page, then no need to do disk i/o
        if (fileHandle.getFileName() == m_fileName &&
            pageNum == m_pageNum) {
            return 0;
        }

        // if we have the same file, but different page, then we need
        // to write the current page to disk before reading the new page
        if (fileHandle.getFileName() == m_fileName &&
                -1 != m_pageNum) {
            auto wp = writePage(fileHandle, m_pageNum);
            if (0 != wp) {
                ERROR("Error while writing the page %d into file %s", m_pageNum, fileHandle.getFileName().c_str());
                return -1;
            }
        }

        eraseAndReset();

        auto rp = fileHandle.readPage(pageNum, m_data);
        if (0 != rp) {
            return rp;
        }

        m_fileName = fileHandle.getFileName();
        m_pageNum = pageNum;
        return 0;
    }

    RC Page::writePage(FileHandle &fileHandle, PageNum pageNum) {
        // write page should be called after previous operations with same page
        // those previous operations should have made sure that the current page
        // we have is the same as the pagenum passed to the writePage
        assert(pageNum == m_pageNum);
        assert(m_fileName == fileHandle.getFileName());

        auto wp = fileHandle.writePage(pageNum, m_data);
        if (0 != wp) {
            return wp;
        }

        return 0;
    }

    bool Page::canInsertRecord(unsigned short recordDataLengthBytes) {
        unsigned short availableBytes = getFreeByteCount();
        // account for the new slot metadata that we need to write after inserting a new record
        return availableBytes >= (recordDataLengthBytes
                                  + RecordAndMetadata::RECORD_METADATA_LENGTH_BYTES + Slot::SLOT_METADATA_LENGTH_BYTES);
    }

    void Page::insertRecord(RecordAndMetadata *recordAndMetadata, unsigned short slotNum) {
        if (!canInsertRecord(recordAndMetadata->getRecordAndMetadataLength())) {
            ERROR("Cannot insert record and metadata of size=%hu into page having %hu bytes free\n",
                  recordAndMetadata->getRecordAndMetadataLength(),
                  getFreeByteCount());
        }

        // insert the record data into the page
        unsigned short recordOffset = computeRecordOffset(slotNum);
        byte *recordStart = m_data + recordOffset;
        recordAndMetadata->write(recordStart);

        // set the newly inserted record's slot metadata
        Slot slot = getSlot(recordAndMetadata->getSlotNumber());
        slot.setRecordOffsetBytes(recordOffset);
        slot.setRecordLengthBytes(recordAndMetadata->getRecordAndMetadataLength());

        // update the page's metadata
        if (recordAndMetadata->getSlotNumber() == getSlotCount()) {
            // this was a newly created slot.
            setSlotCount(getSlotCount() + 1);
        }
        setFreeByteCount(getFreeByteCount() - recordAndMetadata->getRecordAndMetadataLength() -
                         Slot::SLOT_METADATA_LENGTH_BYTES);

        INFO("Inserted record of length=%hu, dataLength=%hu into slot=%hu. Free bytes avlbl=%hu\n",
             recordAndMetadata->getRecordAndMetadataLength(), recordAndMetadata->getRecordDataLength(), recordAndMetadata->getSlotNumber(), getFreeByteCount());
    }

    void Page::readRecord(RecordAndMetadata *recordAndMetadata, unsigned short slotNum) {
        Slot recordSlot = getSlot(slotNum);
        if (recordSlot.getRecordLengthBytes() == 0) {
            return; // this was a deleted record
        }
        void *recordDataStart = (void *) (m_data + recordSlot.getRecordOffsetBytes());
        recordAndMetadata->read(recordDataStart, recordSlot.getRecordLengthBytes());
    }

    void Page::deleteRecord(unsigned short slotNumber) {
        assert(slotNumber >= 0 && slotNumber < getSlotCount());
        Slot recordSlot = getSlot(slotNumber);

//        shift records from subsequent slots (if any) left by the length of the deleted record
        shiftRecordsLeft(slotNumber + 1, recordSlot.getRecordLengthBytes());

//          update the page's freeByteCount
//          Note: the slot of the deleted record, and the slot's metadata size,
//          is 'lost' (can never be used) forever.
        setFreeByteCount(getFreeByteCount() + recordSlot.getRecordLengthBytes());

//      Set the record to be deleted'd length = 0 in the slot directory.
        recordSlot.setRecordLengthBytes(0);
    }

    void Page::eraseAndReset() {
        if (nullptr != m_data)
            memset((void *) m_data, 0, PAGE_SIZE);
        initPageMetadata();
    }

    unsigned short Page::getFreeByteCount() {
        return *freeByteCount;
    }

    void Page::setFreeByteCount(unsigned short numBytesFree) {
        *freeByteCount = numBytesFree;
    }

    unsigned short Page::getSlotCount() {
        return *slotCount;
    }

    void Page::setSlotCount(unsigned short numSlotsInPage) {
        *slotCount = numSlotsInPage;
    }

    unsigned short Page::computeRecordOffset(unsigned short slotNumber) {
        if (slotNumber == 0) {
            return FIRST_RECORD_OFFSET;
        }
        Slot previousSlot = getSlot(slotNumber - 1);
        return previousSlot.getRecordOffsetBytes() + previousSlot.getRecordLengthBytes();
    }

    Slot Page::getSlot(unsigned short slotNum) {
        Slot slot((void *) (slotMetadataEnd - (Slot::SLOT_METADATA_LENGTH_BYTES * (slotNum + 1))));
        return slot;
    }

    void Page::shiftRecordsLeft(int slotNumStart, unsigned short shiftOffsetBytes) {
        if (slotNumStart >= getSlotCount()) {
            return;
        }

        for (unsigned short slotNum = slotNumStart; slotNum < getSlotCount(); ++slotNum) {
            Slot slot = getSlot(slotNum);
            unsigned short recordOffsetOld = slot.getRecordOffsetBytes();
            unsigned short recordOffsetNew = recordOffsetOld - shiftOffsetBytes;
            memmove(m_data + recordOffsetNew, m_data + recordOffsetOld, slot.getRecordLengthBytes());

            slot.setRecordOffsetBytes(recordOffsetNew);
        }
    }

    void Page::shiftRecordsRight(int slotNumStart, unsigned short shiftOffsetBytes) {
        if (slotNumStart >= getSlotCount()) {
            return;
        }

        for (unsigned short slotNum = getSlotCount() - 1; slotNum >= slotNumStart; --slotNum) {
            Slot slot = getSlot(slotNum);
            unsigned short recordOffsetOld = slot.getRecordOffsetBytes();
            unsigned short recordOffsetNew = recordOffsetOld + shiftOffsetBytes;
            memmove(m_data + recordOffsetNew, m_data + recordOffsetOld, slot.getRecordLengthBytes());

            slot.setRecordOffsetBytes(recordOffsetNew);
        }
    }

    unsigned short Page::getRecordLengthBytes(unsigned short slotNumber) {
        return getSlot(slotNumber).getRecordLengthBytes();
    }

    unsigned short Page::generateSlotForInsertion(unsigned short recordDataLengthBytes) {
        if (getSlotCount() == 0) {
            return 0;
        }
        for (unsigned short slotNum = 0; slotNum < getSlotCount(); ++slotNum) {
            Slot slot = getSlot(slotNum);
            if (slot.getRecordLengthBytes() == 0) {
                shiftRecordsRight(slotNum + 1, recordDataLengthBytes + RecordAndMetadata::RECORD_METADATA_LENGTH_BYTES);
                return slotNum;
            }
        }
        return getSlotCount();
    }

    void Page::updateRecord(RecordAndMetadata* recordAndMetadata, unsigned short slotNum) {
        adjustSlotLength(slotNum, recordAndMetadata->getRecordAndMetadataLength());
        // account for that fact that inserting a record shall decrease freeByteCount.
        // So record the freeBytes "gained" by replacing the existing record.
        setFreeByteCount(getFreeByteCount() + getSlot(slotNum).getRecordLengthBytes());
        insertRecord(recordAndMetadata, slotNum);
    }

    void Page::adjustSlotLength(unsigned short slotNum, unsigned short recordAndMetadataLength) {
        Slot slot = getSlot(slotNum);
        unsigned short existingLengthOfSlot = slot.getRecordLengthBytes();
        unsigned short newLengthOfSlot = recordAndMetadataLength;
        int slotLengthGrowth = newLengthOfSlot - existingLengthOfSlot;
        if (slotLengthGrowth < 0) {
            // shrink the slot
            shiftRecordsLeft(slotNum + 1, -slotLengthGrowth);
        } else if (slotLengthGrowth > 0) {
            // grow the slot
            shiftRecordsRight(slotNum + 1, slotLengthGrowth);
        } else {
            // pleasant co-incident! no change in slot length so, no action for us.
        }
    }
}
