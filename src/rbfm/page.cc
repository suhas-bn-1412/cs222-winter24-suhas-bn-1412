#include "src/include/page.h"

#include <assert.h>
#include <string.h>

namespace PeterDB {
    Page::Page() {
        initPageMetadata();
    }

    void Page::initPageMetadata() {
        setFreeByteCount(PAGE_SIZE - PAGE_METADATA_SIZE);
        setSlotCount(0);
    }

    bool Page::canInsertRecord(unsigned short recordLengthBytes) {
        // account for the new slot metadata that we need to write after inserting a new record
        unsigned short availableBytes = getFreeByteCount() - SLOT_METADATA_SIZE;
        return availableBytes >= recordLengthBytes;
    }

    unsigned short Page::insertRecord(void *recordData, unsigned short recordLengthBytes) {
        if (!canInsertRecord(recordLengthBytes)) {
            // RbfmUtil::ERROR("Cannot insert record. Insufficient free space in page.");
            return -1;
        }

        // insert the record data into the page
        unsigned short slotNumber = getSlotCount();
        unsigned short recordOffset = computeRecordOffset(slotNumber);
        // RbfmUtil::INFO("Inserting record at slot=%ui, offset=%ui", slotNumber, recordOffset);
        byte *recordStart = m_data + recordOffset;
        memcpy(recordStart, recordData, recordLengthBytes);

        // set the newly inserted record's slot metadata
        setSlotCount(getSlotCount() + 1);
        setRecordOffset(recordOffset, slotNumber);
        setRecordLengthBytes(recordLengthBytes, slotNumber);

        // update the page's metadata
        setFreeByteCount(getFreeByteCount() - recordLengthBytes);
        // RbfmUtil::INFO("Free bytes remaining in page=%ui", getFreeByteCount());

        return slotNumber;
    }

    void Page::readRecord(unsigned short slotNumber, void *data) {
//        assert(slotNumber >= 0 && slotNumber < getSlotCount());
        unsigned short recordOffset = getRecordOffset(slotNumber);
        unsigned short recordLengthBytes = getRecordLengthBytes(slotNumber);
        // RbfmUtil::INFO("Reading record from slotNum=%ui: offset=%ui, length=%ui", slotNumber, recordOffset, recordLengthBytes);
        void *recordDataStart = (void*) (m_data + recordOffset);
        memcpy(data, recordDataStart, recordLengthBytes);
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

    void *Page::getDataPtr() {
        return (void *) m_data;
    }

    unsigned short Page::computeRecordOffset(unsigned short slotNumber) {
        if (slotNumber == 0) {
            return FIRST_RECORD_OFFSET;
        }
        unsigned short previousRecordOffset = getRecordOffset(slotNumber - 1);
        unsigned short previousRecordLength = getRecordLengthBytes(slotNumber - 1);
        return previousRecordOffset + previousRecordLength;
    }

    unsigned short Page::getRecordOffset(unsigned short slotNumber) {
//        assert(slotNumber >= 0 && slotNumber < getSlotCount());
        unsigned short *recordOffset = tailSlotMetadata - (SLOT_METADATA_SIZE * slotNumber);
        return *recordOffset;
    }

    unsigned short Page::getRecordLengthBytes(unsigned short slotNumber) {
//        assert(slotNumber >= 0 && slotNumber < getSlotCount());
        unsigned short *recordLength = tailSlotMetadata - (SLOT_METADATA_SIZE * slotNumber) + 1;
        return *recordLength;
    }

    void Page::setRecordOffset(unsigned short recordOffset, unsigned short slotNumber) {
//        assert(slotNumber >= 0 && slotNumber < getSlotCount());
        unsigned short *recordOffsetPtr = tailSlotMetadata - (SLOT_METADATA_SIZE * slotNumber);
        *recordOffsetPtr = recordOffset;
    }

    void Page::setRecordLengthBytes(unsigned short recordLengthBytes, unsigned short slotNumber) {
//        assert(slotNumber >= 0 && slotNumber < getSlotCount());
        unsigned short *recordLengthBytesPtr = tailSlotMetadata - (SLOT_METADATA_SIZE * slotNumber) + 1;
        *recordLengthBytesPtr = recordLengthBytes;
    }
}
