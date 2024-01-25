#include "src/include/pfm.h"
#include "src/include/util.h"

#include <cstring>

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        // check if the file with the same name is already present
        if (file_exists(fileName)) {
            ERROR("PagedFileManager::createFile - file '%s' already exists", fileName.c_str());
            return -1;
        }

        if (m_createdFilenames.end() != m_createdFilenames.find(fileName)) {
            ERROR("PagedFileManager::createFile - file '%s' already created", fileName.c_str());
            return -1;
        }

        FILE *fstream = fopen(fileName.c_str(), "wb+");
        if (nullptr == fstream) {
            ERROR("PagedFileManager::createFile - error while creating file '%s'", fileName.c_str());
            return -1;
        }

        if (0 != fclose(fstream)) {
            ERROR("PagedFileManager::createFile - error while closing file '%s'", fileName.c_str());
            return -1;
        }

        m_createdFilenames.insert(fileName);
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        m_createdFilenames.erase(fileName);

        if (!file_exists(fileName)) {
            return -1;
        }
        file_delete(fileName);
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if (fileHandle.isActive()) {
            ERROR("PagedFileManager::openFile - fileHandle is currently active");
            return -1;
        }

        fileHandle.setFileName(fileName);
        return fileHandle.openFile();
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (!fileHandle.isActive()) {
            ERROR("PagedFileManager::closeFile - fileHandle is not currently active");
            return -1;
        }
        return fileHandle.closeFile();
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    bool FileHandle::isActive() {
        return (nullptr != m_fstream);
    }

    std::string FileHandle::getFileName() {
        return m_fileName;
    }

    void FileHandle::setFileName(const std::string &fileName) {
        m_fileName = fileName;
    }

    void FileHandle::loadMetadataFromDisk() {
        void *data = malloc(PAGE_SIZE);
        memset(data, 0, PAGE_SIZE);

        if (0 != fseek(m_fstream, 0, SEEK_SET)) {
            free(data);
            ERROR("FileHandle::loadMetadataFromDisk - Error while seeking to start of the file. err - %s\n", std::strerror(ferror(m_fstream)));
            return;
        }
        if (1 != fread(data, PAGE_SIZE, 1, m_fstream)) {
            free(data);
            ERROR("FileHandle::loadMetadataFromDisk - Error while reading metadata. err - %s\n", std::strerror(ferror(m_fstream)));
            return;
        }

        unsigned *metadata = (unsigned *) data;
        if (metadata[0] == (metadata[1] ^ metadata[2] ^ metadata[3] ^ metadata[4])) {
            m_curPagesInFile = metadata[1];
            readPageCounter = metadata[2];
            writePageCounter = metadata[3];
            appendPageCounter = metadata[4];
        } else {
            ERROR("Error while reading metadata\n");
        }
        free(data);
    }

    void FileHandle::writeMetadataToDisk() {
        unsigned *data = (unsigned *) malloc(PAGE_SIZE);
        memset((void *) data, 0, PAGE_SIZE);
        data[1] = m_curPagesInFile;
        data[2] = readPageCounter;
        data[3] = writePageCounter;
        data[4] = appendPageCounter;

        data[0] = (data[1] ^ data[2] ^ data[3] ^ data[4]);

        if (0 != fseek(m_fstream, 0, SEEK_SET)) {
            ERROR("FileHandle::writeMetadataToDisk - Error while seeking to start of the file. err - %s\n", std::strerror(ferror(m_fstream)));
            free(data);
            return;
        }
        if (1 != fwrite(data, PAGE_SIZE, 1, m_fstream)) {
            ERROR("FileHandle::writeMetadataToDisk - Error while writing metadata - %s\n", std::strerror(ferror(m_fstream)));
            free(data);
            return;
        }
        free(data);
    }

    RC FileHandle::openFile() {
        assert(0 != m_fileName.length());
        assert(nullptr == m_fstream);

        m_fstream = fopen(m_fileName.c_str(), "rb+");
        if (nullptr == m_fstream) {
            ERROR("FileHandle::openFile - unable to open file '%s'", m_fileName.c_str());
            return -1;
        }

        if (0 == file_size(m_fileName)) {
            writeMetadataToDisk();
        }
        loadMetadataFromDisk();

        return 0;
    }

    RC FileHandle::closeFile() {
        if (nullptr == m_fstream) {
            return 0;
        }

        writeMetadataToDisk();

        if (0 != fclose(m_fstream)) {
            WARNING("FileHandle::closeFile - couldn't properly close the file '%s'. err - %s\n", m_fileName.c_str(), std::strerror(ferror(m_fstream)));
        }
        m_fstream = nullptr;

        return 0;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        // pageNum should be less than the number of pages present
        // pages are 0 indexed from user pov, so if pageNum is 0, then
        // user is asking to read page 0, and totalPages might be 1
        if (pageNum >= m_curPagesInFile) {
            ERROR("FileHandle::readPage - page %d not found", pageNum);
            return -1;
        }

        assert(nullptr != data);
        assert(nullptr != m_fstream);

        if (0 != fseek(m_fstream,
                       PAGE_SIZE * (HIDDEN_PAGES + pageNum),
                       SEEK_SET)) {
            ERROR("FileHandle::readPage - error while trying to seek to page '%d' in file '%s'. err - %s\n", pageNum, m_fileName.c_str(), std::strerror(ferror(m_fstream)));
            return -1;
        }

        if (1 != fread(data, PAGE_SIZE, 1, m_fstream)) {
            ERROR("FileHandle::readPage - error while reading '%d' page from file '%s'. err - %s\n", pageNum, m_fileName.c_str(), std::strerror(ferror(m_fstream)));
            return -1;
        }

        readPageCounter++;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        assert(nullptr != data);
        assert(nullptr != m_fstream);

        if (pageNum >= m_curPagesInFile) {
            ERROR("FileHandle::writePage - page %d not found", pageNum);
            return -1;
        }

        if (0 != fseek(m_fstream,
                       PAGE_SIZE * (HIDDEN_PAGES + pageNum),
                       SEEK_SET)) {
            ERROR("FileHandle::writePage - error while trying to seek to page '%d' in file '%s'. err - %s\n", pageNum, m_fileName.c_str(), std::strerror(ferror(m_fstream)));
            return -1;
        }

        if (1 != fwrite(data, PAGE_SIZE, 1, m_fstream)) {
            ERROR("FileHandle::writePage - error while writing '%d' page from file '%s'. err - %s\n", pageNum, m_fileName.c_str(), std::strerror(ferror(m_fstream)));
            return -1;
        }

        writePageCounter++;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        assert(nullptr != data);
        assert(nullptr != m_fstream);

        if (0 != fseek(m_fstream, 0, SEEK_END)) {
            ERROR("FileHandle::appendPage - error while trying to seek to end of file '%s'. err - %s\n", m_fileName.c_str(), std::strerror(ferror(m_fstream)));
            return -1;
        }

        if (1 != fwrite(data, PAGE_SIZE, 1, m_fstream)) {
            ERROR("FileHandle::appendPage - error while appending page to file '%s'. err - %s\n", m_fileName.c_str(), std::strerror(ferror(m_fstream)));
            return -1;
        }

        m_curPagesInFile++;
        appendPageCounter++;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return m_curPagesInFile;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

} // namespace PeterDB
