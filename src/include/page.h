#ifndef _page_h_
#define _page_h_

#define PAGE_SIZE 4096 //todo: consolidate with pfm's definition
#define PAGE_METADATA_SIZE sizeof(unsigned short) * 2
#define FIRST_RECORD_OFFSET 0

#include <cstdlib>
#include <cstring>
#include "src/include/pfm.h"
#include "src/include/slot.h"
#include "src/include/record.h"
#include "src/include/util.h"


namespace PeterDB {
    typedef unsigned char byte;

    class Page {

    public:
        Page();
        ~Page();

        void initPageMetadata(); // used to create an (initially) zero records page.

        RC readPage(FileHandle &fileHandle, PageNum pageNum);

        RC writePage(FileHandle &fileHandle, PageNum pageNum);

        bool canInsertRecord(unsigned short recordDataLengthBytes);

        unsigned short generateSlotForInsertion(unsigned short recordDataLengthBytes);

        void insertRecord(RecordAndMetadata* recordAndMetadata, unsigned short slotNum);

        void readRecord(RecordAndMetadata* recordAndMetadata, unsigned short slotNum);

        void updateRecord(RecordAndMetadata* recordAndMetadata, unsigned short slotNum);

        void deleteRecord(unsigned short slotNumber);

        unsigned short getRecordLengthBytes(unsigned short slotNumber);

        void eraseAndReset();

        unsigned short getSlotCount();

        Slot getSlot(unsigned short slotNum);

    private:
        std::string m_fileName = "";
        int m_pageNum = -1;
        byte *m_data = new byte[PAGE_SIZE];
        unsigned short* freeByteCount = (unsigned short *) (m_data + PAGE_SIZE - PAGE_METADATA_SIZE) + 1;
        unsigned short* slotCount = (unsigned short *) (m_data + PAGE_SIZE - PAGE_METADATA_SIZE);
        byte *slotMetadataEnd =  (m_data + PAGE_SIZE - PAGE_METADATA_SIZE);

        unsigned short getFreeByteCount();

        unsigned short computeRecordOffset(unsigned short slotNumber);

        void setFreeByteCount(unsigned short numBytesFree);

        void setSlotCount(unsigned short numSlotsInPage);

        void shiftRecordsLeft(int slotNumStart, unsigned short shiftOffsetBytes);

        void shiftRecordsRight(int slotNumStart, unsigned short shiftOffsetBytes);

        void adjustSlotLength(unsigned short slotNum, unsigned short recordAndMetadataLength);
    };
}

#endif
