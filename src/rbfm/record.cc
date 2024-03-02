#include <cstdlib>
#include <cstring>
#include "src/include/record.h"

PeterDB::RecordAndMetadata::RecordAndMetadata() {

}

unsigned short PeterDB::RecordAndMetadata::getRecordAndMetadataLength() const {
    return m_recordAndMetadataLength;
}

void PeterDB::RecordAndMetadata::init(unsigned short pageNum, unsigned short slotNum, bool isTombstone, unsigned short recordDataLength,
                                      void *recordData) {
    m_pageNum = pageNum;
    m_slotNum = slotNum;
    m_isTombStone = isTombstone;
    m_recordDataLength = recordDataLength;
    m_recordData = malloc(recordDataLength);
    memcpy(m_recordData, recordData, recordDataLength);
    m_recordAndMetadataLength = m_recordDataLength + RECORD_METADATA_LENGTH_BYTES;
}

void PeterDB::RecordAndMetadata::read(void *data, unsigned short recordAndMetadataLength) {
    byte *readPtr = (byte *) data;
    size_t fieldLength = sizeof(unsigned short);
    memcpy(&m_pageNum, readPtr, fieldLength);
    readPtr += fieldLength;

    fieldLength = sizeof(unsigned short);
    memcpy(&m_slotNum, readPtr, fieldLength);
    readPtr += fieldLength;

    fieldLength = sizeof(bool);
    memcpy(&m_isTombStone, readPtr, fieldLength);
    readPtr += fieldLength;

    m_recordAndMetadataLength = recordAndMetadataLength;
    m_recordDataLength = recordAndMetadataLength - RECORD_METADATA_LENGTH_BYTES;

    void *recordData = (byte *) data + RECORD_METADATA_LENGTH_BYTES;
    m_recordData = malloc(m_recordAndMetadataLength);
    memcpy(m_recordData, recordData, m_recordAndMetadataLength);
}

void PeterDB::RecordAndMetadata::write(void *writeBuffer) {
    byte *writePtr = (byte *) writeBuffer;
    size_t fieldLength;

    fieldLength = sizeof(unsigned short);
    memcpy(writePtr, &m_pageNum, fieldLength);
    writePtr += fieldLength;

    fieldLength = sizeof(unsigned short);
    memcpy(writePtr, &m_slotNum, fieldLength);
    writePtr += fieldLength;

    fieldLength = sizeof(bool);
    memcpy(writePtr, &m_isTombStone, fieldLength);
    writePtr += fieldLength;

    memcpy(writePtr, m_recordData, m_recordDataLength);
}

PeterDB::RecordAndMetadata::~RecordAndMetadata() {
    if (m_recordData) {
        free(m_recordData);
    }
    m_recordData = nullptr;
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

bool PeterDB::RecordAndMetadata::isTombstone() const {
    return m_isTombStone;
}

unsigned short PeterDB::RecordAndMetadata::getRecordDataLength() const {
    return m_recordDataLength;
}
