#include "src/include/page.h"
#include "src/include/util.h"

namespace PeterDB {
    Page::Page() {
    }

    void Page::init(void *data) {
        m_data_head = (byte *) data;
        initMetadataPtrs();
    }

    void *Page::init() {
        m_data_head = (byte *) malloc(PAGE_SIZE);
        initMetadataPtrs();
        setRecordCount(0);
        setFreeByteCount(PAGE_SIZE - (2 * sizeof(unsigned short)));
        return m_data_head;
    }

    void Page::teardown() {
        free(m_data_head);
    }

    short Page::insertRecord(void *data, unsigned short length) {
        if (getFreeByteCount() < length) {
            ERROR("Cannot insert record. Insufficient free space in page.");
            return -1;
        }

        unsigned short *slotMetadataHeadPtr = slotMetadataTailPtr - getRecordCount();

        return 0;
    }

    void *Page::readRecord(unsigned short slotNumber) {
        // todo
        return nullptr;
    }

    unsigned short Page::getFreeByteCount() {
        return *freeByteCountPtr;
    }

    void Page::setFreeByteCount(unsigned short freeByteCount) {
        *freeByteCountPtr = freeByteCount;
    }

    unsigned short Page::getRecordCount() {
        return *recordCountPtr;
    }

    void Page::setRecordCount(unsigned short recordCount) {
        *recordCountPtr = recordCount;
    }

    void Page::initMetadataPtrs() {
        freeByteCountPtr = (unsigned short *) (m_data_head + PAGE_SIZE) - 1;
        recordCountPtr = (unsigned short *) (m_data_head + PAGE_SIZE) - 2;
        slotMetadataTailPtr = (unsigned short *) (m_data_head + PAGE_SIZE) - 2 - SLOT_METADATA_SIZE;
    }
}

