#include <cstdlib>
#include <cstring>
#include "src/include/record.h"

PeterDB::RecordAndMetadata::RecordAndMetadata() {

}

unsigned short PeterDB::RecordAndMetadata::getRecordAndMetadataLength() const {
    return m_recordAndMetadataLength;
}

void PeterDB::RecordAndMetadata::init(unsigned short pageNum, unsigned short slotNum, unsigned short recordDataLength,
                                      void *recordData) {
    m_pageNum = pageNum;
    m_slotNum = slotNum;
    m_recordData = malloc(recordDataLength);
    memcpy(m_recordData, recordData, recordDataLength);
    m_recordDataLength = recordDataLength;
    m_recordAndMetadataLength = m_recordDataLength + RECORD_METADATA_LENGTH_BYTES;
}

void PeterDB::RecordAndMetadata::read(void *data, unsigned short recordAndMetadataLength) {
    m_pageNum = *((unsigned short*) data);
    m_slotNum = *(((unsigned short *) data) + 1);
    m_recordAndMetadataLength = recordAndMetadataLength;
    m_recordDataLength = recordAndMetadataLength - RECORD_METADATA_LENGTH_BYTES;
    void *recordData = (byte *) data + RECORD_METADATA_LENGTH_BYTES;
    m_recordData = malloc(m_recordAndMetadataLength);
    memcpy(m_recordData, recordData, m_recordAndMetadataLength);
}

void PeterDB::RecordAndMetadata::write(void *writeBuffer) {
    byte *writePtr = (byte*) writeBuffer;
    memcpy((void *) writePtr, (void *) &m_pageNum, RECORD_METADATA_FIELD_SIZE);
    writePtr += RECORD_METADATA_FIELD_SIZE;

    memcpy((void *) writePtr, (void *) &m_slotNum, RECORD_METADATA_FIELD_SIZE);
    writePtr += RECORD_METADATA_FIELD_SIZE;

    memcpy((void *) writePtr, m_recordData, m_recordDataLength);
}

PeterDB::RecordAndMetadata::~RecordAndMetadata() {
    free(m_recordData);
}

unsigned short PeterDB::RecordAndMetadata::getSlotNumber() const {
    return m_slotNum;
}

void *PeterDB::RecordAndMetadata::getRecordDataPtr() const {
    return m_recordData;
}

unsigned short PeterDB::RecordAndMetadata::getMPageNum() const {
    return m_pageNum;
}
