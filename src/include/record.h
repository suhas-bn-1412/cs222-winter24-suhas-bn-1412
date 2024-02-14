#ifndef _record_h_
#define _record_h_

typedef char byte;

namespace PeterDB {
    class RecordAndMetadata {
    private:
        unsigned short m_pageNum;
        unsigned short m_slotNum;
        bool m_isTombStone;
        void *m_recordData = nullptr;

        unsigned short m_recordAndMetadataLength;
        unsigned short m_recordDataLength;

    public:
        RecordAndMetadata();
        ~RecordAndMetadata();

        static const unsigned short RECORD_METADATA_LENGTH_BYTES = (sizeof(unsigned short) * 2) + (sizeof(bool) * 1);

        void init(unsigned short pageNum, unsigned short slotNum, bool isTombstone, unsigned short recordDataLength, void *recordData);

        void read(void *data, unsigned short recordAndMetadataLength);

        void write(void *writeBuffer);

        unsigned short getMPageNum() const;

        unsigned short getSlotNumber() const;

        bool isTombstone() const;

        void *getRecordDataPtr() const;

        unsigned short getRecordAndMetadataLength() const;

        unsigned short getRecordDataLength() const;
    };
}

#endif
