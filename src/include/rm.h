#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_map>

#include "src/include/rbfm.h"
#include "src/include/ix.h"
#include "src/include/catalogueConstants.h"
#include "attributeAndValue.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    class RelationManager;

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RM_ScanIterator();

        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        void reset();

        RC close();

        RC init(RelationManager* rm, RecordBasedFileManager* rbfm, const std::string& tableName);
        RC initRbfmsi(const std::string &conditionAttribute,
                      const CompOp compOp,
                      const void *value,
                      const std::vector<std::string> &attributeNames);

        bool m_initDone = false;
        RelationManager* m_rm = nullptr;
        RecordBasedFileManager* m_rbfm = nullptr;
        FileHandle m_fh;
        std::vector<Attribute> m_attrs;
        RBFM_ScanIterator m_rbfmsi;
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan

        IXFileHandle &getIxFileHandle();

        IX_ScanIterator &getIxScanIterator();

        void setIxScanIterator(const IX_ScanIterator &mIxScanIterator);

        void setIxFileHandle(const IXFileHandle &mIxFileHandle);

    private:
        IXFileHandle _m_ix_fileHandle;
        IX_ScanIterator _m_ix_scan_iterator;
    };

    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

        // given table name, creates fileHandle and Record descriptor
        RC getFileHandleAndAttributes(const std::string& tableName, FileHandle& fh, std::vector<Attribute>& attrs);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

        bool m_catalogCreated = false;
        RecordBasedFileManager *m_rbfm = nullptr;
        IndexManager *_m_ix = nullptr;
        std::unordered_map<std::string, bool> m_tablesCreated;

        // opens both Tables table and Attributes table
        RC openTablesAndAttributesFH(FileHandle& tableFileHandle, FileHandle& attributesFileHandle);

        void initTablesTable();

        void initAttributesTable();

        void buildAttributesForAttributesTable(int tableId,
                                               const std::vector<Attribute> &attributes,
                                               std::vector<std::vector<AttributeAndValue>>&);

        int computeNextTableId();

        void buildAndInsertAttributesIntoAttributesTable(const std::vector<Attribute> &attrs, int tid);

        static std::string buildIndexFilename(const std::string &tableName, const std::string &attributeName);

        static bool doesIndexExist(const std::string &tableName, const std::string &attributeName);

        Attribute getAttribute(const std::string &tableName, const std::string &attributeName);
    };

} // namespace PeterDB

#endif // _rm_h_
