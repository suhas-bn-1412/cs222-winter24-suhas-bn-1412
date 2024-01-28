#ifndef _slot_h_
#define _slot_h_

namespace PeterDB {
    class Slot {
    private:
        /*
         * Field 1 = recordOffsetBytes
         * Field 2 = recordLengthBytes
         */
        //todo: move to std::map

        static const unsigned short FIELD_SIZE = sizeof(unsigned short);

        static const unsigned short NUM_FIELDS = 2;

        unsigned short *m_slotData;

    public:
        static const unsigned short SLOT_LENGTH_BYTES = NUM_FIELDS * FIELD_SIZE;

        Slot(void* slotData);

        unsigned short getRecordOffsetBytes();

        unsigned short getRecordLengthBytes();

        void setRecordOffsetBytes(unsigned short recordOffsetBytes);

        void setRecordLengthBytes(unsigned short recordLengthBytes);
    };
}

#endif