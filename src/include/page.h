#ifndef _page_h_
#define _page_h_

#define PAGE_SIZE 4096 //todo: consolidate with pfm's definition
#define PAGE_METADATA_SIZE sizeof(unsigned short) * 2
#define SLOT_METADATA_SIZE sizeof(unsigned short) * 2

#include <cstdlib>

typedef unsigned char byte;

namespace PeterDB {
    class Page {

    public:
        Page();

        void initPageMetadata(); // used to create an (initially) zero records page.

        void* getDataPtr();

        bool canInsertRecord(unsigned short recordLengthBytes);

        short insertRecord(void *data, unsigned short length);

        void readRecord(unsigned short slotNumber, void* data);

    private:

        byte *m_date = new byte[PAGE_SIZE];
        unsigned short* freeByteCount = (unsigned short *) (m_date + PAGE_SIZE) - 1;
        unsigned short* slotCount = (unsigned short *) (m_date + PAGE_SIZE) - 2;
        unsigned short *slotMetadataTail = (unsigned short *) (m_date + PAGE_SIZE) - PAGE_METADATA_SIZE;

        unsigned short getFreeByteCount();

        unsigned short getSlotCount();

        void setFreeByteCount(unsigned short numBytesFree);

        void setSlotCount(unsigned short numSlotsInPage);
    };
}

#endif
