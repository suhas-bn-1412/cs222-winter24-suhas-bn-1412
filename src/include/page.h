#ifndef _page_h_
#define _page_h_

#define PAGE_SIZE 4096 //todo: consolidate with pfm's definition
#define SLOT_METADATA_SIZE sizeof(unsigned short) * 2

#include <cstdlib>

typedef char byte;

namespace PeterDB {
    class Page {

    public:
        Page();

        void *init(); // used to create an (initially) zero records page.

        void init(void* data); // used when page data is read from file.

        void teardown();

        unsigned short getFreeByteCount();

        short insertRecord(void *data, unsigned short length);

        void *readRecord(unsigned short slotNumber);

    private:
        byte *m_data_head;
        unsigned short *freeByteCountPtr;
        unsigned short *recordCountPtr;
        unsigned short *slotMetadataTailPtr;

        void initMetadataPtrs();

        void setFreeByteCount(unsigned short freeByteCount);

        unsigned short getRecordCount();

        void setRecordCount(unsigned short recordCount);
    };
}

#endif
