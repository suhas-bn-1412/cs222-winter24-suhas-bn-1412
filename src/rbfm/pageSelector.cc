#include "src/include/pageSelector.h"
#include "src/include/util.h"

namespace PeterDB {

PageSelector::PageSelector(const std::string& fileName, FileHandle *fileHandle) {
    assert(fileName == fileHandle->getFileName());

    m_fileName = fileName;
    m_fileHandle = fileHandle;

    m_pageOccupancyMetadata = (uint32_t*)malloc(PAGE_SIZE);
    assert(nullptr != m_pageOccupancyMetadata);
    memset(m_pageOccupancyMetadata, 0, PAGE_SIZE);
}

PageSelector::~PageSelector() {
    assert(nullptr != m_pageOccupancyMetadata);

    free(m_pageOccupancyMetadata);
}

void heapify(std::vector<PageOccupancy> &pageOccupancyArr) {
    std::make_heap(pageOccupancyArr.begin(), pageOccupancyArr.end(), [](const PageOccupancy& a, const PageOccupancy& b) {
        return a.availableSpace < b.availableSpace;
    });
}

std::vector<PageOccupancy> PageSelector::deserializePageOccupancyInfo(const void *data) {
    uint32_t numEntries = *( (uint32_t*) data);

    uint16_t szNumEntries = sizeof(numEntries);
    uint16_t szPageOccupancy = sizeof(PageOccupancy);

    assert(numEntries <= ((PAGE_SIZE - szNumEntries) / szPageOccupancy));

    std::vector<PageOccupancy> pageOccupancyArr;

    for (uint32_t i = 0; i < numEntries; i++) {
        PageOccupancy pageOcc;

        memmove((void*)&pageOcc,
                (void*) ( (char*)data + szNumEntries + i*szPageOccupancy ),
                szPageOccupancy);
        pageOccupancyArr.push_back(pageOcc);
    }


    return pageOccupancyArr;
}

void PageSelector::readPageOccupancyInfo() {
    assert(nullptr != m_pageOccupancyMetadata);
    assert(true == m_fileHandle->isActive());

    // go through all the page numbers present in metadata
    // and read those as array of PageOccupancy and store it
    // in the vector m_pageOccupancyInfo
    // the first integer in each page represents the number of
    // PageOccupancy data present in that page

    for (int i=1; i < m_pageOccupancyMetadata[0]+1; i++) {
        auto pageNum = m_pageOccupancyMetadata[i];

        // read page with pageNum=pageNum
        // convert the data into PageOccupancy metainfo
        void *data = malloc(PAGE_SIZE);
        assert(nullptr != data);
        memset(data, 0, PAGE_SIZE);

        auto rp = m_fileHandle->readPage(pageNum, data);
        if (0 != rp) {
            ERROR("Error while reading the PageOccupancy info page with pageNum %d", i);
            free(data);
            return;
        }

       m_pageOccupancyInfo.insert(std::make_pair(pageNum, deserializePageOccupancyInfo(data)));
       free(data);
    }
}

void PageSelector::readMetadataFromDisk() {
    assert(true == m_fileHandle->isActive());

    if (0 != m_fileHandle->getNextPageNum()) {
        auto rp = m_fileHandle->readPage(PAGE_OCCUPANCY_METADATA_PAGE, m_pageOccupancyMetadata);
        if (0 != rp) {
            ERROR("Error while reading PageOccupancy metadata page");
            return;
        }

        readPageOccupancyInfo();

        // after reading the page occupancy info, set the number of hidden pages
        // used by rbfm
        m_fileHandle->setHiddenPagesUsed(1 + m_pageOccupancyMetadata[0]);

        return;
    }
    
    memset(m_pageOccupancyMetadata, 0, PAGE_SIZE);

    auto ap = m_fileHandle->appendPage(m_pageOccupancyMetadata);
    if (0 != ap) {
        ERROR("Failure while creating PageOccupancy metadata page");
        return;
    }

    // if we inserted the metadata page, then insert one more
    // page which actually contains the pageNum-availSpace data
    ap = m_fileHandle->appendPage(m_pageOccupancyMetadata);
    if (0 != ap) {
        ERROR("Failure while creating PageOccupancy first page");
        return;
    }

    // set the number of PageOccupancy pages, and their page num
    // in PageOccupancy metadata page
    m_pageOccupancyMetadata[0] = 1; // we have one page dedicated for page occupancy info
    m_pageOccupancyMetadata[1] = 1; // the page with pageNum = 1 has PageOccupancy info

    // set the number of hidden pages used
    // 1 for metadata page, and number of page occupancy info pages
    m_fileHandle->setHiddenPagesUsed(1 + m_pageOccupancyMetadata[0]);

    std::vector<PageOccupancy> pageOccupancyArr;
    m_pageOccupancyInfo[1] = pageOccupancyArr; // PageOccupancy info stored in pageNum=1
    return;
}

void PageSelector::serializePageOccupancyInfo(const std::vector<PageOccupancy>& pageOccupancyArr, void* serializedData) {
    void *dataPtr = serializedData;

    uint16_t szNumEntries = sizeof(uint32_t);
    uint16_t szPageOccupancy = sizeof(PageOccupancy);

    // Write the number of entries to the data block (first 4 bytes)
    uint32_t numEntries = static_cast<uint32_t>(pageOccupancyArr.size());

    memmove(dataPtr, (void*)(&numEntries), szNumEntries);
    dataPtr = (void*)( (char*)dataPtr + szNumEntries);

    // Write each PageOccupancy entry to the data block
    for (uint32_t i = 0; i < numEntries; i++) {
        const PageOccupancy& pageOcc = pageOccupancyArr[i];

        memmove(dataPtr, (void*)(&pageOcc), szPageOccupancy);
        dataPtr = (void*)( (char*)dataPtr + szPageOccupancy);
    }
}

void PageSelector::writePageOccupancyInfo() {
    assert(nullptr != m_pageOccupancyMetadata);
    assert(true == m_fileHandle->isActive());

    void *serializedData = malloc(PAGE_SIZE);
    assert(nullptr != serializedData);
    memset(serializedData, 0, PAGE_SIZE);

    // Iterate through all the page numbers present in metadata
    // and write the serialized PageOccupancy data to those pages
    for (int i = 1; i < m_pageOccupancyMetadata[0] + 1; i++) {
        uint32_t pageNum = m_pageOccupancyMetadata[i];

        // Get the serialized data block for the corresponding page
        assert(m_pageOccupancyInfo.find(pageNum) != m_pageOccupancyInfo.end());
        serializePageOccupancyInfo(m_pageOccupancyInfo[pageNum], serializedData);

        // Write the serialized data block to the specified page
        auto wp = m_fileHandle->writePage(pageNum, serializedData);
        if (0 != wp) {
            ERROR("Error while writing the PageOccupancy info to page with pageNum %d", pageNum);
            free(serializedData);
            return;
        }

        memset(serializedData, 0, PAGE_SIZE);
    }

    free(serializedData);
}

void PageSelector::writeMetadataToDisk() {
    assert(true == m_fileHandle->isActive());
    assert(nullptr != m_pageOccupancyMetadata);

    // first go through all the pageNums present in the metadata
    // and write each of that info as it's own page
    writePageOccupancyInfo();

    // then write the metadata page into the disk
    auto wp = m_fileHandle->writePage(PAGE_OCCUPANCY_METADATA_PAGE, (void*)m_pageOccupancyMetadata);
    if (0 != wp) {
        ERROR("Error while writing PageOccupancy metadata into the file");
        return;
    }
}

unsigned PageSelector::selectPage(const uint32_t& requiredBytes) {
    // go through all of the hidden pages
    // check if top page-info in that hidden page has data
    // if it's enough, then decrement the required amount
    // heapify the vector of page-occupancy-info
    unsigned pageNum = 0;
    bool pageFound = false;

    for (auto &pageInfo : m_pageOccupancyInfo) {
        if (!pageInfo.second.empty() &&
            pageInfo.second.front().availableSpace >= requiredBytes) {
            
            pageFound = true;
            pageNum = pageInfo.second.front().pageNum;

            pageInfo.second.front().availableSpace -= requiredBytes;
            heapify(pageInfo.second);
            break;
        }
    }
    if (pageFound) {
        return pageNum;
    }

    // if there are no pages with enough available space, then append
    // a new page. add new pages data into one of the hidden pages
    // return the page number
    pageNum = m_fileHandle->getNextPageNum();
    
    void *data = malloc(PAGE_SIZE);
    assert(nullptr != data);
    memset(data, 0, PAGE_SIZE);

    auto ap = m_fileHandle->appendPage(data);
    if (0 != ap) {
        assert(false);
        ERROR("Error while appending new PageOccupancy info file\n");
        free(data);
        return 0;
    }

    insertNewPageOccupancyInfo(pageNum, PAGE_SIZE - requiredBytes - (2*sizeof(unsigned short)));
    free(data);

    return pageNum;
}

void PageSelector::insertNewPageOccupancyInfo(const unsigned &pageNum, const unsigned &availableSpace) {
    PageOccupancy pageOcc;
    pageOcc.pageNum = pageNum;
    pageOcc.availableSpace = availableSpace;

    auto szNumEntries = sizeof(uint32_t);
    auto szPageOccupancy = sizeof(PageOccupancy);
    auto maxEntriesInVector = ( (PAGE_SIZE - szNumEntries) / szPageOccupancy );

    // find vector to which we can add this info and re-heapify
    // i.e, find a page which has pageNum-availaSpace info, and has space to
    // store another item of pageNum-availSpace info
    for (auto &pageInfo : m_pageOccupancyInfo) {
        if (pageInfo.second.size() < maxEntriesInVector) {
            pageInfo.second.push_back(pageOcc);
            heapify(pageInfo.second);
            return;
        }
    }

    unsigned createdPageNum = createPageForPageOccupancyInfo();
    std::vector<PageOccupancy> newVectorForNewPage;
    newVectorForNewPage.push_back(pageOcc);
    m_pageOccupancyInfo.insert(std::make_pair(createdPageNum, newVectorForNewPage));
}

unsigned PageSelector::createPageForPageOccupancyInfo() {
    void *data = malloc(PAGE_SIZE);
    assert(nullptr != data);
    memset(data, 0, PAGE_SIZE);

    unsigned newPageNum = m_fileHandle->getNextPageNum();
    auto ap = m_fileHandle->appendPage(data);
    if (0 != ap) {
        assert(false);
        ERROR("Error while appending new PageOccupancy info file\n");
        free(data);
        return 0;
    }

    // store the pageNum in PageOccupancy metadata
    m_pageOccupancyMetadata[0] += 1;
    m_pageOccupancyMetadata[m_pageOccupancyMetadata[0]] = newPageNum;

    // reset the hidden pages used by this layer
    m_fileHandle->setHiddenPagesUsed(1 + m_pageOccupancyMetadata[0]);

    free(data);
    return newPageNum;
}

}
