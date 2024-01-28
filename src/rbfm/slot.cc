#include "src/include/slot.h"

PeterDB::Slot::Slot(void *slotData) {
    m_slotData = (unsigned short *) slotData;
}

unsigned short PeterDB::Slot::getRecordOffsetBytes() {
    return m_slotData[0];
}

unsigned short PeterDB::Slot::getRecordLengthBytes() {
    return m_slotData[1];
}

void PeterDB::Slot::setRecordOffsetBytes(unsigned short recordOffsetBytes) {
    m_slotData[0] = recordOffsetBytes;
}

void PeterDB::Slot::setRecordLengthBytes(unsigned short recordLengthBytes) {
    m_slotData[1] = recordLengthBytes;
}
