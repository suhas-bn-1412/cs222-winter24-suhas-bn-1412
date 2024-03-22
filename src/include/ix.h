#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <unordered_map>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute
#include "leafPage.h"
#include "nonLeafPage.h"
#include "pageSerializer.h"
#include "pageDeserializer.h"

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment
        PagedFileManager* _pagedFileManager = nullptr;
        PageDeserializer* deserializer = nullptr;
        PageSerializer* serializer = nullptr;
        std::unordered_map<std::string, bool> m_indexesCreated;

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

        RC intPrinter(IXFileHandle &ixFileHandle, PageNum node, std::ostream &out) const;
        RC floatPrinter(IXFileHandle &ixFileHandle, PageNum node, std::ostream &out) const;
        RC varcharPrinter(IXFileHandle &ixFileHandle, PageNum node, std::ostream &out) const;

        static unsigned int getKeySize(const void *key, const Attribute &attributeOfKey);

        /*
        * Compares a search key with RidAndKey (handling all types)
        * Return value:
        * 0   : both the keys are equal
        * < 0 : 'searchKey' < ridAndKeyPair.get___Key()
        * > 0 : 'searchKey' > ridAndKeyPair.get___Key()
        */
        static int keyCompare(const void *searchKey, const AttrType &searchKeyType,
                                     const RidAndKey &ridAndKeyPair);

        /*
        * Compares a search key with RidAndKey (handling all types)
        * Return value:
        * 0   : both the keys are equal
        * < 0 : 'searchKey' < pageNumAndKeyPair.get___Key()
        * > 0 : 'searchKey' > pageNumAndKeyPair.get___Key()
        */
        static int keyCompare(const void *searchKey, const RID searchRid, const AttrType& searchKeyType, const RidAndKey& ridAndKeyPair);

    protected:
       RC insertHelper(IXFileHandle& fileHandle, PageNum node, const RidAndKey& entry, PageNumAndKey& newChild);

        template<typename T>
        RC writePageToDisk(IXFileHandle& fileHandle, T& page, int pageNum);

    private:

        static unsigned int getLowerLevelNode(const void *searchKey, const Attribute &attribute, const void* pageData);

        static RC deleteFromPage(const void *targetKey, const RID &targetRid,
                                 const Attribute &targetKeyAttribute,
                                 PeterDB::LeafPage &leafPage, bool& continueToNextPage);

        /*
         * Compares a search key with PageNumAndKey (handling all types)
         * Return value:
         * 0   : both the keys are equal
         * < 0 : 'searchKey' < pageNumAndKeyPair.get___Key()
         * > 0 : 'searchKey' > pageNumAndKeyPair.get___Key()
         */
        static int keyCompare(const void *searchKey, const AttrType& searchKeyType, const PageNumAndKey &pageNumAndKeyPair);

        void loadPage(unsigned int pageNum, void *pageData, IXFileHandle &ixFileHandle) const;

        void writePage(const void *pageData, unsigned int pageNum, IXFileHandle &ixFileHandle) const;

        static bool isEqual(const RID &ridA, const RID& ridB);
    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        void init(IXFileHandle *ixFileHandle, unsigned int pageNumBegin,
                  const void* startKey, const bool shouldIncludeStartKey,
                  const void *endKey, bool shouldIncludeEndKey, const Attribute& keyAttribute);

        void loadLeafPage(PageNum pageNum);

        // Terminate index scan
        RC close();

    private:
        IXFileHandle *_ixFileHandle = nullptr;
        void *_pageData;
        LeafPage _currentLeafPage;
        unsigned int _nextElementPositionOnPage; //todo: perhaps special handling for scan-delete keys
        unsigned int _currentPageKeysCount;

        void * _endKey;
        AttrType _keyType;
        bool _shouldIncludeEndKey;

        void copyEndKey(const void *endKey, const Attribute &keyAttribute);

        int getNextLeafPage();

        bool atEndOfCurrentPage();

        bool isWithinRange(const RidAndKey &candidateRidAndKey);

        void copy(RID &destRid, void *destKey, const RidAndKey &srcRidAndKey);

        unsigned int
        getIndex(LeafPage leafPage, const void *searchKey, const bool shouldIncludeSearchKey,
                 const AttrType &keyType);

        bool wasPreviouslyReturnedEntryDeleted();
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        std::string _fileName;
        unsigned int _rootPageNum = 0;

        FileHandle _pfmFileHandle;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        void fetchRootNodePtrFromDisk();
        void writeRootNodePtrToDisk();
    };
}// namespace PeterDB
#endif // _ix_h_
