#ifndef _page_selector_h_
#define _page_selector_h_

#define PAGE_SIZE 4096
#define PAGE_OCCUPANCY_METADATA_PAGE 0
#define PAGE_OCCUPANCY_FIRST_PAGE 1

#include "pfm.h"

#include <map>
#include <vector>
#include <algorithm>

namespace PeterDB {

struct PageOccupancy {
    unsigned pageNum;
    unsigned availableSpace;
};

class PageSelector {
    public:
    PageSelector(const std::string& fileName, FileHandle *fileHandle);
    ~PageSelector();

    void readMetadataFromDisk();
    void writeMetadataToDisk();
    unsigned selectPage(const uint32_t& requiredBytes);
    void decrementAvailableSpace(unsigned pageNum, int diff);
    bool isThisPageAMetadataPage(const PageNum &pageNum);

    private:
    std::string m_fileName = "";
    FileHandle *m_fileHandle = nullptr;

    // the below declaration didn't work, reason being the following
    // the methos called using the fileHandle pointer (getNumPages, readPage..etc) are not marked
    // constant, meaning that those functions can change 'this' pointer
    // const FileHandle* m_fileHandle = nullptr; // pointer to constant.. can change where we point to
                                                 // but can't change what we're ponting to

    // page occupancy metadata.. contains array of integer
    // first integer represents how many pages are used currently to store
    // page occupancy info. if that is n, then next n integers
    // represent the pageNums of all those n pages
    uint32_t *m_pageOccupancyMetadata = nullptr;

    // page occupancy info stored in multiple hidden pages
    // page occupancy of entire file might not be stored in single hidden page
    // hence we'll have to store nultiple hidden pages
    std::map<uint16_t, std::vector<PageOccupancy>> m_pageOccupancyInfo;

    void readPageOccupancyInfo();
    std::vector<PageOccupancy> deserializePageOccupancyInfo(const void *data);

    void writePageOccupancyInfo();
    void serializePageOccupancyInfo(const std::vector<PageOccupancy>& pageOccupancyArr, void* serializedData);

    unsigned createPageForPageOccupancyInfo();
    void insertNewPageOccupancyInfo(const unsigned &pageNum, const unsigned &availableSpace);
};

} // namespace PeterDB

#endif
