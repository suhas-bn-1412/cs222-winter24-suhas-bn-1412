#include "src/include/pfm.h"
#include "src/include/util.h"

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
            ERROR("PagedFileManager::createFile - file '%s' already exists", fileName);
            return -1;
        }

        if (m_fhStore.end() == m_fhStore.find(fileName)) {
            ERROR("PagedFileManager::createFile - file '%s' already created", fileName);
            return -1;
        }

        // create file handle for this file and store it in the map
        FileHandle *fh = new FileHandle();
        fh->setFileName(fileName);
        m_fhStore[fileName] = fh;

        int fd = open(fileName.c_str(), O_CREAT);
        if (-1 == fd) {
            ERROR("PagedFileManager::createFile - error while creating file '%s'", fileName);
            return -1;
        }

        if (!close(fd)) {
            ERROR("PagedFileManager::createFile - error while closing file '%s'", fileName);
            return -1;
        }

        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        auto iter = m_fhStore.find(fileName);
        if (iter == m_fhStore.end()) {
            ERROR("PagedFileManager::destroyFile - file '%s' not created", fileName);
            return -1;
        }

        FileHandle* fh = iter->second;
        fh->closeFile();
        
        m_fhStore.erase(iter);

        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        assert(fileHandle.getFileName() == fileName);
        return fileHandle.openFile();
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return fileHandle.closeFile();
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    std::string FileHandle::getFileName() {
        return "";
    }

    void FileHandle::setFileName(const std::string& fileName) {
    }

    RC FileHandle::openFile() {
        return -1;
    }

    RC FileHandle::closeFile() {
        return -1;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        /*
         * fd is present, file is open
         *
         * using fseek move the file stream to point to that pagenum
         * execute fread
         */
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        /*
         * move to pagenum using fseek and execute fwrite
         */
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        /*
         * move to end of file using fseek and write the data
         */
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        return -1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

} // namespace PeterDB
