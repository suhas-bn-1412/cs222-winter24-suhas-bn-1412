#ifndef _record_h_
#define _record_h_

typedef char byte;

namespace PeterDB {
    class RecordAndMetadata {
    private:
        static const unsigned short RECORD_METADATA_FIELD_COUNT = 2;
        static const unsigned short RECORD_METADATA_FIELD_SIZE = sizeof(unsigned short);

        unsigned short m_pageNum;
        unsigned short m_slotNum;
        void *m_recordData;

        unsigned short m_recordAndMetadataLength;
        unsigned short m_recordDataLength;

    public:
        RecordAndMetadata();
        ~RecordAndMetadata();

        static const unsigned short RECORD_METADATA_LENGTH_BYTES = RECORD_METADATA_FIELD_COUNT * RECORD_METADATA_FIELD_SIZE;

        void init(unsigned short pageNum, unsigned short slotNum, unsigned short recordDataLength, void *recordData);

        void read(void *data, unsigned short recordAndMetadataLength);

        void write(void *writeBuffer);

        unsigned short getSlotNumber() const;

        void *getRecordDataPtr() const;

        unsigned short getRecordAndMetadataLength() const;
    };
}

#endif