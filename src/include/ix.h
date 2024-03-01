#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute
#include "leafPage.h"
#include "nonLeafPage.h"

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

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

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    private:
        static PagedFileManager *_pagedFileManager;

        static unsigned int getLowerLevelNode(const void *searchKey, const Attribute &attribute, NonLeafPage &pageData);

        static RC deleteFromPage(const void *targetKey, const RID &targetRid, const Attribute &targetKeyAttribute, PeterDB::LeafPage &leafPage);

        /*
         * Compares a search key with PageNumAndKey (handling all types)
         * Return value:
         * 0   : both the keys are equal
         * < 0 : 'searchKey' < pageNumAndKeyPair.get___Key()
         * > 0 : 'searchKey' > pageNumAndKeyPair.get___Key()
         */
        static int keyCompare(const void *searchKey, const Attribute &searchKeyType, const PageNumAndKey &pageNumAndKeyPair);

        /*
        * Compares a search key with RidAndKey (handling all types)
        * Return value:
        * 0   : both the keys are equal
        * < 0 : 'searchKey' < pageNumAndKeyPair.get___Key()
        * > 0 : 'searchKey' > pageNumAndKeyPair.get___Key()
        */
        static int keyCompare(const void *searchKey, const RID searchRid, const Attribute searchKeyType, const RidAndKey ridAndKeyPair);
    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        FileHandle _pfmFileHandle;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
        // use pfm.

    };
}// namespace PeterDB
#endif // _ix_h_
