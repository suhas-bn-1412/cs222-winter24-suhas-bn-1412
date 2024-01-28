#include "src/include/page.h"
#include "src/include/util.h"
#include <cstring>
#include <cstdio>

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

    bool Page::canInsertRecord(unsigned short recordLengthBytes) {
        unsigned short availableBytes = getFreeByteCount();
        // account for the new slot metadata that we need to write after inserting a new record
        return availableBytes >= (recordLengthBytes + SLOT_METADATA_SIZE);
    }

    unsigned short Page::insertRecord(void *recordData, unsigned short recordLengthBytes) {
        if (!canInsertRecord(recordLengthBytes)) {
            ERROR("Cannot insert record of size=%hu into page having %hu bytes free\n", recordLengthBytes,
                  getFreeByteCount());
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

        INFO("Inserted record into slot=%hu. Free bytes avlbl=%hu\n", slotNumber, getFreeByteCount());
        return slotNumber;
    }

    void Page::readRecord(unsigned short slotNumber, void *data) {
        unsigned short recordOffset = getRecordOffset(slotNumber);
        unsigned short recordLengthBytes = getRecordLengthBytes(slotNumber);
        if (recordLengthBytes == 0) {
            return; // this was a deleted record
        }
        void *recordDataStart = (void*) (m_data + recordOffset);
        memcpy(data, recordDataStart, recordLengthBytes);
    }

    void Page::deleteRecord(unsigned short slotNumber) {
        assert(slotNumber >= 0 && slotNumber < getSlotCount());

        unsigned short recordLengthBytes = getRecordLengthBytes(slotNumber);
//      Set the record to be deleted'd length = 0 in the slot directory.
        setRecordLengthBytes(0, slotNumber);

//        shift records from subsequent slots (if any) left by the length of the deleted record
        shiftRecordsLeft(slotNumber + 1, recordLengthBytes);

//          update the page's freeByteCount
//          Note: the slot of the deleted record, and the slot's metadata size,
//          is 'lost' (can never be used) forever.
        setFreeByteCount(getFreeByteCount() + recordLengthBytes);
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

    unsigned short* Page::getSlot(unsigned short slotNum){
        return (unsigned short *) (slotMetadataEnd - (SLOT_METADATA_SIZE * (slotNum + 1)));
    }

    unsigned short Page::getRecordOffset(unsigned short slotNumber) {
        unsigned short *slotData = getSlot(slotNumber);
        return *(slotData+0);
    }

    unsigned short Page::getRecordLengthBytes(unsigned short slotNumber) {
        unsigned short *slotData = getSlot(slotNumber);
        return *(slotData + 1);
    }

    void Page::setRecordOffset(unsigned short recordOffset, unsigned short slotNumber) {
        unsigned short *slotData = getSlot(slotNumber);
        *(slotData + 0) = recordOffset;
    }

    void Page::setRecordLengthBytes(unsigned short recordLengthBytes, unsigned short slotNumber) {
        unsigned short *slotData = getSlot(slotNumber);
        *(slotData + 1) = recordLengthBytes;
    }

    void Page::shiftRecordsLeft(int slotNumStart, unsigned short shiftOffsetBytes) {
        if (slotNumStart >= getSlotCount()) {
            return;
        }

        for (unsigned short slotNum = slotNumStart; slotNum < getSlotCount(); ++slotNum) {
            unsigned short recordLengthBytes = getRecordLengthBytes(slotNum);
            unsigned short recordOffsetOld = getRecordOffset(slotNum);
            unsigned short recordOffsetNew = recordOffsetOld - shiftOffsetBytes;
            memmove(m_data + recordOffsetNew, m_data + recordOffsetOld, recordLengthBytes);

            setRecordOffset(recordOffsetNew, slotNum);
        }
    }
}
