#include "src/include/pfm.h"

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
        /*
         * check if the file is already present
         * return error if present
         *
         * create file using fopen in a+ mode
         * create file handle and set the fd, fname
         *
         * create metadata for the file. it just contains number of pages present
         * write the metadata using filehandle.writepage (page num is 0)
         *
         * set fd as -1 before closing the file
         * close the file (fclose)
         */
        return -1;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        /*
         * check if the fh is present with us
         * if not return error
         *
         * close the file if it's currently open
         * delete the file using unlink
         * call destructor of the fh
         */
        return -1;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        /*
         * check if filename is present
         *
         * fetch the fh from the map
         * using fopen open the file for rd/wr
         * set the fd in fh
         *
         * return fh
         */
        return -1;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        /*
         * check if file is present
         *
         * using fh, get the fd
         * do fclose on fd
         *
         * set fd to -1
         */
        return -1;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

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
