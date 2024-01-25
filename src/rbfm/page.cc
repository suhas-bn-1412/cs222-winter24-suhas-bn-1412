#include "src/include/page.h"

#include <assert.h>
#include <string.h>

namespace PeterDB {
    Page::Page() {
        initPageMetadata();
    }

    Page::~Page() {
        if (nullptr != m_data) {
            delete m_data;
        }
        m_data = nullptr;
    }

    void Page::initPageMetadata() {
        setFreeByteCount(PAGE_SIZE - PAGE_METADATA_SIZE - SLOT_COUNT_METADAT_SIZE);
        setSlotCount(0);
    }

    bool Page::canInsertRecord(unsigned short recordLengthBytes) {
        // account for the new slot metadata that we need to write after inserting a new record
        unsigned short availableBytes = getFreeByteCount() - SLOT_METADATA_SIZE;
        return availableBytes >= recordLengthBytes;
    }

    unsigned short Page::insertRecord(void *recordData, unsigned short recordLengthBytes) {
        if (!canInsertRecord(recordLengthBytes)) {
            return -1;
        }

        // insert the record data into the page
        unsigned short slotNumber = getSlotCount();
        unsigned short recordOffset = computeRecordOffset(slotNumber);
        byte *recordStart = m_data + recordOffset;
        memcpy(recordStart, recordData, recordLengthBytes);

        // set the newly inserted record's slot metadata
        setRecordOffset(recordOffset, slotNumber);
        setRecordLengthBytes(recordLengthBytes, slotNumber);

        // update the page's metadata
        setSlotCount(getSlotCount() + 1);
        setFreeByteCount(getFreeByteCount() - recordLengthBytes - SLOT_METADATA_SIZE);

        return slotNumber;
    }

    void Page::readRecord(unsigned short slotNumber, void *data) {
        unsigned short recordOffset = getRecordOffset(slotNumber);
        unsigned short recordLengthBytes = getRecordLengthBytes(slotNumber);
        void *recordDataStart = (void*) (m_data + recordOffset);
        memcpy(data, recordDataStart, recordLengthBytes);
    }

    void Page::eraseData() {
        if (nullptr != m_data)
            memset((void*)m_data, 0, PAGE_SIZE);
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
        unsigned short *recordOffset = tailSlotMetadata - (SLOT_METADATA_SIZE * (slotNumber+1));
        return *recordOffset;
    }

    unsigned short Page::getRecordLengthBytes(unsigned short slotNumber) {
        unsigned short *recordLength = tailSlotMetadata - (SLOT_METADATA_SIZE * (slotNumber+1)) + 1;
        return *recordLength;
    }

    void Page::setRecordOffset(unsigned short recordOffset, unsigned short slotNumber) {
        unsigned short *recordOffsetPtr = tailSlotMetadata - (SLOT_METADATA_SIZE * (slotNumber+1));
        *recordOffsetPtr = recordOffset;
    }

    void Page::setRecordLengthBytes(unsigned short recordLengthBytes, unsigned short slotNumber) {
        unsigned short *recordLengthBytesPtr = tailSlotMetadata - (SLOT_METADATA_SIZE * (slotNumber+1)) + 1;
        *recordLengthBytesPtr = recordLengthBytes;
    }
}
