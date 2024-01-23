#include "src/include/page.h"
#include "src/include/util.h"

namespace PeterDB {
    Page::Page() {
        initPageMetadata();
    }

    void Page::initPageMetadata() {
        setFreeByteCount(PAGE_SIZE - PAGE_METADATA_SIZE);
        setSlotCount(0);
    }

    short Page::insertRecord(void *data, unsigned short length) {
        //todo
        if (!canInsertRecord(length)) {
            ERROR("Cannot insert record. Insufficient free space in page.");
            return -1;
        }

        return 0;
    }

    void Page::readRecord(unsigned short slotNumber, void *data) {
        //todo:
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
        return (void *) m_date;
    }

    bool Page::canInsertRecord(unsigned short recordLengthBytes) {
        unsigned short availableBytes = getFreeByteCount() - SLOT_METADATA_SIZE;
        return availableBytes >= recordLengthBytes;
    }
}
