#include "src/include/rm.h"
#include "src/include/ix.h"
#include "src/include/attributeAndValueSerializer.h"

#include <dirent.h>

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        if (nullptr == _relation_manager.m_rbfm) {
            _relation_manager.m_rbfm = &RecordBasedFileManager::instance();
        }
        if (_relation_manager.m_ix == nullptr) {
            _relation_manager.m_ix = &(IndexManager::instance());
        }
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    std::string getFileName(const std::string& tableName) {
        return tableName; // + ".bin"; we don't have a choice.. test cases expects this way
    }

    RC RelationManager::createCatalog() {
        if (m_catalogCreated) return 0;

        m_catalogCreated = true;
        INFO("Creating Catalogue\n");

        initTablesTable();
        initAttributesTable();

        m_tablesCreated[CatalogueConstants::TABLES_FILE_NAME] = true;
        m_tablesCreated[CatalogueConstants::ATTRIBUTES_FILE_NAME] = true;

        INFO("Created Catalogue\n");
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        if (!m_catalogCreated) return -1;

        for (auto &table : m_tablesCreated) {
            m_rbfm->destroyFile(getFileName(table.first));
        }

        m_tablesCreated.clear();

        m_catalogCreated = false;

        return 0;
    }

    RC RelationManager::createTable(const std::string &tablezName, const std::vector<Attribute> &attrs) {
        if (!m_catalogCreated) return -1;

        if (tablezName == CatalogueConstants::TABLES_FILE_NAME ||
            tablezName == CatalogueConstants::ATTRIBUTES_FILE_NAME) {
            return -1;
        }

        std::string tableFileName = getFileName(tablezName);
        if (0 != m_rbfm->createFile(tableFileName)) {
            ERROR("Error while creating the file for table %s\n", tableFileName);
            return -1;
        }

        // find next TID
        int tid = computeNextTableId();
        INFO("Creating table for tableName=%s; allottedTID=%d\n", tablezName.data(), tid);
        if (tid == -1) {
            return -1;
        }

        // ======= STEP 1
        // prepare and insert table details into "Tables" table
        std::vector<AttributeAndValue> tablesTableAttributeAndValues;
        tablesTableAttributeAndValues.push_back(AttributeAndValue{TablesAttributeConstants::TABLE_ID, &tid});
        tablesTableAttributeAndValues.push_back(AttributeAndValue{TablesAttributeConstants::TABLE_NAME, (void*) &tablezName});
        tablesTableAttributeAndValues.push_back(AttributeAndValue{TablesAttributeConstants::TABLE_FILENAME, &tableFileName});

        // prepare (serialize) DATA
        size_t tablesTableAttributeAndValuesDataSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(
                &tablesTableAttributeAndValues);
        void *tablesTableAttributeAndValuesData = malloc(tablesTableAttributeAndValuesDataSize);

        AttributeAndValueSerializer::serialize(tablesTableAttributeAndValues, tablesTableAttributeAndValuesData);

        // insert into table "Tables"
        RID rid1;
        FileHandle tablesFileHandle;
        m_rbfm->openFile(CatalogueConstants::TABLES_FILE_NAME, tablesFileHandle);
        m_rbfm->insertRecord(tablesFileHandle, CatalogueConstants::tablesTableAttributes,
                             tablesTableAttributeAndValuesData, rid1);
        m_rbfm->closeFile(tablesFileHandle);


        //============ STEP 2
        // prepare and insert table attribute details into "Attributes" table
        buildAndInsertAttributesIntoAttributesTable(attrs, tid);

        m_tablesCreated[tablezName] = true;
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if (!m_catalogCreated) return -1;

        if (tableName == CatalogueConstants::TABLES_FILE_NAME ||
            tableName == CatalogueConstants::ATTRIBUTES_FILE_NAME) {
            return -1;
        }

        auto it = m_tablesCreated.find(tableName);
        if (m_tablesCreated.end() == it) {
            ERROR("Delete table '%s' not possible, it was not created\n", tableName);
            return -1;
        }

        m_tablesCreated.erase(it);

        m_rbfm->destroyFile(getFileName(tableName));
        destroyIndex(tableName);
        return 0;
    }

    int RelationManager::computeNextTableId() {
        // go through table Tables and get the number of entries
        // next tableid = cur num of tables + 1

        // create fileHandle for accessing table Tables
        FileHandle tableFileHandle;
        if (0 != m_rbfm->openFile(CatalogueConstants::TABLES_FILE_NAME, tableFileHandle)) {
            ERROR("Error while opening Tables file");
            return -1;
        }

        // prepare conditions for scanning Tables table
        auto attrToReadFromTablesTable = std::vector<std::string>(1, TABLE_ATTR_NAME_ID);
        std::string conditionAttr = "";
        void* conditionValue = nullptr;

        RBFM_ScanIterator rbfmsi;
        auto scan = m_rbfm->scan(tableFileHandle,
                                 CatalogueConstants::tablesTableAttributes,
                                 conditionAttr,
                                 NO_OP,
                                 conditionValue,
                                 attrToReadFromTablesTable,
                                 rbfmsi);
        if (0 != scan) {
            ERROR("Error while trying to get a rbfm scan iterator for Tables table");
            m_rbfm->closeFile(tableFileHandle);
            return -1;
        }

        // using scan iterator for the table, read the entry for tabeName and read only the id
        // construct the data into which the id is written
        void* tableIdData = malloc(4 + 1 + 4); // num of attrs in the record, nullflag byte, bytes to store table-id data
        assert(nullptr != tableIdData);
        RID tableIdRid;

        int numTables = 0;
        while(RBFM_EOF != rbfmsi.getNextRecord(tableIdRid, tableIdData)) {
            numTables++;
        }
        rbfmsi.close();

        free(tableIdData);
        return numTables;
    }

    AttrType getAttrType(uint32_t attrType) {
        assert(attrType <= TypeVarChar);

        if (TypeInt == attrType) return TypeInt;
        if (TypeReal == attrType) return TypeReal;
        if (TypeVarChar == attrType) return TypeVarChar;

        assert(0);
        return TypeInt;
    }

    Attribute getAttributeFromData(void* data) {
        // we have data in the following format
        // first 1 byte = representing the nullFlags of all the attibutes
        // next 4 bytes = length of the varchar holding the attribute name = n
        // next n bytes = attribute name
        // next 4 bytes = attribute type
        // next 4 bytes = attribute length

        uint32_t strlen = *( (uint32_t*) ( (char*)data + 1) );
        std::string attr_name;
        attr_name.assign(( (char*)data + 1 + 4), strlen);

        uint32_t attr_type = *( (uint32_t*) ((char*)data + 1 + 4 + strlen) );
        uint32_t attr_length = *( (uint32_t*) ((char*)data + 1 + 4 + strlen + 4) );

        Attribute attr;
        attr.name = attr_name;
        attr.type = getAttrType(attr_type);
        attr.length = attr_length;
        return attr;
    }

    RC RelationManager::openTablesAndAttributesFH(FileHandle& tableFileHandle,
                                                  FileHandle& attributesFileHandle) {
        auto of = m_rbfm->openFile(CatalogueConstants::TABLES_FILE_NAME, tableFileHandle);
        if (0 != of) {
            ERROR("Error while opening %s file", CatalogueConstants::TABLES_FILE_NAME);
            return of;
        }

        of = m_rbfm->openFile(CatalogueConstants::ATTRIBUTES_TABLE_NAME, attributesFileHandle);
        if (0 != of) {
            ERROR("Error while opening %s file", CatalogueConstants::ATTRIBUTES_TABLE_NAME);
            m_rbfm->closeFile(tableFileHandle);
            return of;
        }

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        FileHandle tableFileHandle;
        FileHandle attributesFileHandle;

        if (0 != openTablesAndAttributesFH(tableFileHandle, attributesFileHandle)) {
            return -1;
        }

        // read table Tables and read id for which name = tableName

        // vector of attibutes to project from scan result
        auto attrToReadFromTablesTable = std::vector<std::string>(1, TABLE_ATTR_NAME_ID);

        // prepare condition value and attribute
        std::string conditionAttr = TABLE_ATTR_NAME_NAME;
 
        void* conditionValue = malloc(4 + tableName.size());
        assert(nullptr != conditionValue);
 
        uint32_t tablenamesz = tableName.size();
        memmove(conditionValue, &tablenamesz, 4);
        memmove( (void*)( (char*) conditionValue + 4), tableName.c_str(), tableName.size());
 
        RBFM_ScanIterator rbfmsi;
        auto scan = m_rbfm->scan(tableFileHandle,
                                 CatalogueConstants::tablesTableAttributes,
                                 conditionAttr,
                                 EQ_OP,
                                 conditionValue,
                                 attrToReadFromTablesTable,
                                 rbfmsi);
        if (0 != scan) {
            ERROR("Error while trying to get a rbfm scan iterator for Tables table");
            m_rbfm->closeFile(tableFileHandle);
            m_rbfm->closeFile(attributesFileHandle);
            free(conditionValue);
            return -1;
        }
        free(conditionValue);

        // using scan iterator for the table, read the entry for tabeName and read only the id
        // construct the data into which the id is written
        void* tableIdData = malloc(1 + 4); // nullflag byte, bytes to store table-id data
        assert(nullptr != tableIdData);
        RID tableIdRid;

        auto gn = rbfmsi.getNextRecord(tableIdRid, tableIdData);
        if (0 != gn) {
            ERROR("Error while getting table id for table %s, from Tables table", tableName);
            m_rbfm->closeFile(tableFileHandle);
            m_rbfm->closeFile(attributesFileHandle);
            free(tableIdData);
            return -1;
        }
        rbfmsi.close();
        m_rbfm->closeFile(tableFileHandle);

        // now tableIdData is as follows
        // first 1 byte = <nullflags>, representing theres only one byte used for nullflags
        // next 4 bytes = <table-id> representing the table id value for table with name=tableName

        // read the table Attributes and read for all the entries with table_id = id
        
        // attributes to project from attributes table
        std::vector<std::string> attrsToReadFromAttributesTable = {ATTRIBUTES_ATTR_NAME_ATTR_NAME,
                                                                   ATTRIBUTES_ATTR_NAME_ATTR_TYPE,
                                                                   ATTRIBUTES_ATTR_NAME_ATTR_LENGTH};

        // nullflags + length of varchar attr + varchar attr + int attr + int attr
        unsigned maxSpaceReq = 1 + 4 + ATTRIBUTE_NAME_MAX_LENGTH + 4 + 4;

        // prepare the arguments for scan function
        conditionValue = (void*)( (char*) tableIdData + 1);
        conditionAttr = ATTRIBUTES_ATTR_NAME_TABLE_ID;

        scan = m_rbfm->scan(attributesFileHandle,
                            CatalogueConstants::attributesTableAttributes,
                            conditionAttr,
                            EQ_OP,
                            conditionValue,
                            attrsToReadFromAttributesTable,
                            rbfmsi);
        if (0 != scan) {
            ERROR("Error while trying to get rbfm scan iterator for Attributes table");
            m_rbfm->closeFile(attributesFileHandle);
            free(tableIdData);
            return -1;
        }
        free(tableIdData);

        // allocate memory for data into which we read the records from the attributes table
        RID ridOfRecordInAttrsTable;
        void* data = malloc(maxSpaceReq);
        assert(nullptr != data);
        memset(data, 0, maxSpaceReq);

        while ( RBFM_EOF != rbfmsi.getNextRecord(ridOfRecordInAttrsTable, data)) {
            Attribute attr = getAttributeFromData(data);
            attrs.push_back(attr);
            memset(data, 0, maxSpaceReq);
        }
        rbfmsi.close();

        m_rbfm->closeFile(attributesFileHandle);
        free(data);

        return 0;
    }

    RC RelationManager::getFileHandleAndAttributes(const std::string& tableName,
                                                          FileHandle& fh,
                                                          std::vector<Attribute>& attrs) {
        if (0 != getAttributes(tableName, attrs)) {
            ERROR("Error while getting attributes for table %s", tableName);
            return -1;
        }

        std::string filename = getFileName(tableName);
        if (0 != m_rbfm->openFile(filename, fh)) {
            ERROR("Error while opening the file %s", filename);
            return -1;
        }

        return 0;
    }

    // 0 indexed..
    // in recordData, most significant bit represents the null flag bit
    // of the attr with attrNum=0
    bool isAttrNull(const void *recordData, const uint16_t &attrNum) {
        uint16_t q = (attrNum) / 8;
        uint16_t r = (attrNum) % 8;

        return (0 != (((char*)recordData)[q] & (1 << (7-r))));
    }

    void* getKeyFromRecord(const void* data, const std::vector<Attribute> attrs, const Attribute& attrToFetch) {

        int nullFlagSize = ((attrs.size() + 7) / 8);
        void* dataPtr = ((char*)data + nullFlagSize);

        int attrIdx = 0, attrSize = 0;
        for (auto &attr: attrs) {
            bool isNull = isAttrNull(data, attrIdx);

            if (!isNull) {
                attrSize = 4;
                if (TypeVarChar == attr.type) {
                    attrSize += *((uint32_t*)dataPtr);
                }

                if (attrToFetch.name == attr.name) {
                    void* key = malloc(attrSize);
                    assert(nullptr != key);

                    memmove(key, dataPtr, attrSize);
                    return key;
                }

                dataPtr = (void*) ((char*)dataPtr + attrSize);
            }
            attrIdx++;
        }
        return nullptr;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if (tableName == CatalogueConstants::TABLES_FILE_NAME ||
            tableName == CatalogueConstants::ATTRIBUTES_FILE_NAME) {
            return -1;
        }

        std::vector<Attribute> attrs;
        FileHandle fh;

        if (0 != getFileHandleAndAttributes(tableName, fh, attrs)) {
            ERROR("Error while getting filehandle and attributes for table %s", tableName);
            return -1;
        }

        if ( 0 != m_rbfm->insertRecord(fh, attrs, data, rid)) {
            ERROR("Error while inserting the record into table %s", tableName);
            m_rbfm->closeFile(fh);
            return -1;
        }
        m_rbfm->closeFile(fh);

        insertIntoIndex(tableName, attrs, data, rid);

        return 0;
    }

    void RelationManager::insertIntoIndex(const std::string& tableName,
                                          const std::vector<Attribute>& attrs,
                                          const void* recordData, const RID& rid) {
        // insert the entry into indexes created on this table
        std::vector<std::string> indexNames;
        getIndexNames(tableName, indexNames);

        std::string tableName_, attrName;
        Attribute attrDef;
        for (auto& indexFname: indexNames) {
            // get Attribute from index name, and create the index key
            // that needs to be inserted into the index file
            std::tie(tableName_, attrName) = getTableAndAttrFromIndexFileName(indexFname);
            assert(tableName == tableName_);

            attrDef = getAttributeDefn(tableName, attrName);

            void* key = getKeyFromRecord(recordData, attrs, attrDef);

            IXFileHandle ixFileHandle;
            m_ix->openFile(indexFname, ixFileHandle);
            m_ix->insertEntry(ixFileHandle, attrDef, key, rid);
            m_ix->closeFile(ixFileHandle);

            free(key);
        }
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if (tableName == CatalogueConstants::TABLES_FILE_NAME ||
            tableName == CatalogueConstants::ATTRIBUTES_FILE_NAME) {
            return -1;
        }

        std::vector<Attribute> attrs;
        FileHandle fh;

        if (0 != getFileHandleAndAttributes(tableName, fh, attrs)) {
            ERROR("Error while getting filehandle and attributes for table %s", tableName);
            return -1;
        }

        // before deleting the record, read the record to get the record data
        // this is used to delete the entries in the index files
        int maxSpaceRequired = 0;
        for (auto &attr: attrs) {
            maxSpaceRequired += 4;
            if (TypeVarChar == attr.type) maxSpaceRequired += attr.length;
        }
        void* data = malloc(maxSpaceRequired);
        assert(nullptr != data);
        memset(data, 0, maxSpaceRequired);

        assert(0 == readTuple(tableName, rid, data));

        if ( 0 != m_rbfm->deleteRecord(fh, attrs, rid)) {
            ERROR("Error while deleting the record from table %s", tableName);
            m_rbfm->closeFile(fh);
            return -1;
        }
        m_rbfm->closeFile(fh);

        deleteFromIndex(tableName, attrs, data, rid);

        free(data);
        return 0;
    }

    void RelationManager::deleteFromIndex(const std::string& tableName,
                                          const std::vector<Attribute>& attrs,
                                          const void* recordData, const RID& rid) {
        // insert the entry into indexes created on this table
        std::vector<std::string> indexNames;
        getIndexNames(tableName, indexNames);

        std::string tableName_, attrName;
        Attribute attrDef;
        for (auto& indexFname: indexNames) {
            // get Attribute from index name, and create the index key
            // that needs to be inserted into the index file
            std::tie(tableName_, attrName) = getTableAndAttrFromIndexFileName(indexFname);
            assert(tableName == tableName_);

            attrDef = getAttributeDefn(tableName, attrName);

            void* key = getKeyFromRecord(recordData, attrs, attrDef);

            IXFileHandle ixFileHandle;
            m_ix->openFile(indexFname, ixFileHandle);
            m_ix->deleteEntry(ixFileHandle, attrDef, key, rid);
            m_ix->closeFile(ixFileHandle);

            free(key);
        }
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if (tableName == CatalogueConstants::TABLES_FILE_NAME ||
            tableName == CatalogueConstants::ATTRIBUTES_FILE_NAME) {
            return -1;
        }

        std::vector<Attribute> attrs;
        FileHandle fh;

        if (0 != getFileHandleAndAttributes(tableName, fh, attrs)) {
            ERROR("Error while getting filehandle and attributes for table %s", tableName);
            return -1;
        }

        // before updating the record, read the record to get the record data
        // this is used to delete the entries in the index files
        int maxSpaceRequired = 0;
        for (auto &attr: attrs) {
            maxSpaceRequired += 4;
            if (TypeVarChar == attr.type) maxSpaceRequired += attr.length;
        }
        void* oldRecordData = malloc(maxSpaceRequired);
        assert(nullptr != data);
        memset(oldRecordData, 0, maxSpaceRequired);

        assert(0 == readTuple(tableName, rid, oldRecordData));

        if ( 0 != m_rbfm->updateRecord(fh, attrs, data, rid)) {
            ERROR("Error while updating the record in table %s", tableName);
            m_rbfm->closeFile(fh);
            free(oldRecordData);
            return -1;
        }
        m_rbfm->closeFile(fh);

        deleteFromIndex(tableName, attrs, oldRecordData, rid);
        insertIntoIndex(tableName, attrs, data, rid);

        free(oldRecordData);
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        std::vector<Attribute> attrs;
        FileHandle fh;

        if (0 != getFileHandleAndAttributes(tableName, fh, attrs)) {
            ERROR("Error while getting filehandle and attributes for table %s", tableName);
            return -1;
        }

        if (0 != m_rbfm->readRecord(fh, attrs, rid, data)) {
            ERROR("Error while reading the record from table %s", tableName);
            m_rbfm->closeFile(fh);
            return -1;
        }
        m_rbfm->closeFile(fh);
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return m_rbfm->printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid,
                                      const std::string &attributeName, void *data) {
        std::vector<Attribute> attrs;
        FileHandle fh;

        if (0 != getFileHandleAndAttributes(tableName, fh, attrs)) {
            ERROR("Error while getting filehandle and attributes for table %s", tableName);
            return -1;
        }

        if (0 != m_rbfm->readAttribute(fh, attrs, rid, attributeName, data)) {
            ERROR("Error while reading an attribute from table %s", tableName);
            m_rbfm->closeFile(fh);
            return -1;
        }
        m_rbfm->closeFile(fh);
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        if (m_tablesCreated.end() == m_tablesCreated.find(tableName)) {
            ERROR("Scan: Table %s not found\n", tableName);
            return -1;
        }
        if (0 != rm_ScanIterator.init(this, m_rbfm, tableName)) {
            return -1;
        }
        return rm_ScanIterator.initRbfmsi(conditionAttribute, compOp, value, attributeNames);
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    void RM_ScanIterator::reset() {
        m_initDone = false;
        m_rm = nullptr;
        m_rbfm = nullptr;
        m_attrs.clear();
    }

    RC RM_ScanIterator::init(RelationManager *rm, RecordBasedFileManager *rbfm, const std::string &tableName) {
        reset(); // in case of reusing the same iterator object
                 // we need to reset the info inside the object, so that
                 // it wont be corrupt
        m_initDone = true;
        m_rm = rm;
        m_rbfm = rbfm;

        if (0 != m_rm->getFileHandleAndAttributes(tableName, m_fh, m_attrs)) {
            ERROR("Error while initialising a scan iterator. Failed while creating file handle");
            return -1;
        }
        return 0;
    }

    RC RM_ScanIterator::initRbfmsi(const std::string &conditionAttribute,
                                   const CompOp compOp,
                                   const void *value,
                                   const std::vector<std::string> &attributeNames) {
        assert(m_initDone == true);

        if (0 != m_rbfm->scan(m_fh, m_attrs, conditionAttribute, compOp, value, attributeNames, m_rbfmsi)) {
            ERROR("Error while init'ing RBFM scan iterator");
            return -1;
        }
        return 0;
   }

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        assert(true == m_initDone);

        if (RBFM_EOF != m_rbfmsi.getNextRecord(rid, data)) {
            return 0;
        }
        return RM_EOF;
    }

    RC RM_ScanIterator::close() {
        if (m_initDone) {
            m_initDone = false;
            m_rbfmsi.close();
            return m_rbfm->closeFile(m_fh);
        }
        return 0;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    void RelationManager::retrospectivelyInsertExistingKeysIntoIndex(const std::string &table_name,
        const std::string &attribute_name) {
        const std::string indexFileName = buildIndexFilename(table_name, attribute_name);
        IXFileHandle ixFileHandle;
        m_ix->openFile(indexFileName, ixFileHandle);

        // prepare attr names of table = tableName
        Attribute indexAttribute;
        std::vector<Attribute> attributes;
        getAttributes(table_name, attributes);
        for (const auto &attribute: attributes) {
            if (strcmp(attribute.name.c_str(), attribute_name.c_str()) == 0) {
                indexAttribute = attribute;
                break;
            }
        }

        std::vector<std::string> attributeNames;
        attributeNames.push_back(attribute_name);

        // scan for existing records
        RM_ScanIterator scan_iter;
        void *recordData = malloc(30);
        scan(table_name, attribute_name, NO_OP, nullptr, attributeNames, scan_iter);

        RID recordRid;
        RC scanRC;
        do {
            scanRC = scan_iter.getNextTuple(recordRid, recordData);
            if (scanRC == RM_EOF) {
                break;
            }
            m_ix->insertEntry(ixFileHandle, indexAttribute, recordData, recordRid);
            INFO("Added rid to index\n");
        } while (true);

        scan_iter.close();
        m_ix->closeFile(ixFileHandle);
        free(recordData);
    }


    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
        INFO("Creaitng index for tableName=%s on attribute=%s\n",
             tableName, attributeName);
        // check if index already exists (currently, just check for filename on disk)
        if (doesIndexExist(tableName, attributeName)) {
            ERROR("Index for table=%s, attribute=%s already exists",
                  tableName.data(), attributeName.data());
            return -1;
        }

        // IndexFilename format: <tableName>_<attrName>_index
        const std::string indexFileName = buildIndexFilename(tableName, attributeName);
        m_ix->createFile(indexFileName);

        // bulk load previously inserted tuples
        retrospectivelyInsertExistingKeysIntoIndex(tableName, attributeName);

        return 0;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
        // check if index even exists (currently, just check for filename on disk)
        if (!doesIndexExist(tableName, attributeName)) {
            ERROR("Index for table=%s, attribute=%s does not even exist",
                  tableName.data(), attributeName.data());
            return -1;
        }

        const std::string indexFileName = buildIndexFilename(tableName, attributeName);
        return m_ix->destroyFile(indexFileName);
    }

    void RelationManager::destroyIndex(const std::string &tableName) {
        std::vector<Attribute> tableAttributes;
        getAttributes(tableName, tableAttributes);
        for (const auto &tableAttribute: tableAttributes) {
            // if an index exists for this (tableName, attrName), it shall be destroyed
            // if one does not exist, destoryIndex(...) will simply return -1 which is OK
            destroyIndex(tableName, tableAttribute.name);
        }
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                                  const std::string &attributeName,
                                  const void *lowKey,
                                  const void *highKey,
                                  bool lowKeyInclusive,
                                  bool highKeyInclusive,
                                  RM_IndexScanIterator &rm_IndexScanIterator) {
        /*
         * wrapper around ix.scan()
         */

        // check if index even exists (currently, just check for filename on disk)
        if (!doesIndexExist(tableName, attributeName)) {
            ERROR("Index for table=%s, attribute=%s does not even exist",
                  tableName.data(), attributeName.data());
            return -1;
        }

        // open the file associated with this index
        const std::string indexFilename = buildIndexFilename(tableName, attributeName);
        IXFileHandle &ixFileHandle = rm_IndexScanIterator.getIxFileHandle();
        m_ix->openFile(indexFilename, ixFileHandle);

        rm_IndexScanIterator.getIxScanIterator().init(&ixFileHandle,
                                                      ixFileHandle._rootPageNum,
                                                      lowKey,
                                                      lowKeyInclusive,
                                                      highKey,
                                                      highKeyInclusive,
                                                      getAttributeDefn(tableName, attributeName));
        return 0;
    }

    Attribute RelationManager::getAttributeDefn(const std::string &tableName, const std::string &attributeName) {
        std::vector<Attribute> tableAttributes;
        getAttributes(tableName, tableAttributes);
        for (const Attribute &attribute: tableAttributes) {
            if (strcmp(attribute.name.data(), attributeName.data()) == 0) {
                return attribute;
            }
        }
        ERROR("Table=%s contains no attribute named %s\n",
              tableName, attributeName);
        return {};
    }

    void RelationManager::initTablesTable() {
        INFO("Initializing \"Tables\"\n");
        // Create a file for table "Tables"
        m_rbfm->createFile(CatalogueConstants::TABLES_FILE_NAME);

        // Open the file for table "Tables"
        FileHandle tablesFileHandle;
        m_rbfm->openFile(CatalogueConstants::TABLES_FILE_NAME, tablesFileHandle);

        // Initialize table "Tables" by inserting 2 rows (records)
        // corresponding to tableId-tableName-filename of "Tables" and "Attributes"

        // PREPARE AND INSERT ROW #1

        // Fetch attributes
        std::vector<AttributeAndValue> tablesTableAttributeAndValues;
        CatalogueConstantsBuilder::buildTablesTableAttributeAndValues(tablesTableAttributeAndValues);
        size_t tablesTableAttributeAndValuesDataSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(
                &tablesTableAttributeAndValues);
        void *tablesTableAttributeAndValuesData = malloc(tablesTableAttributeAndValuesDataSize);

        // prepare DATA
        AttributeAndValueSerializer::serialize(tablesTableAttributeAndValues, tablesTableAttributeAndValuesData);

        // insert into table "Tables"
        RID rid1;
        m_rbfm->insertRecord(tablesFileHandle, CatalogueConstants::tablesTableAttributes,
                             tablesTableAttributeAndValuesData, rid1);

        // PREPARE AND INSERT ROW #1

        // Fetch attributes
        std::vector<AttributeAndValue> attributesTableAttributeAndValues;
        CatalogueConstantsBuilder::buildAttributesTableAttributeAndValues(attributesTableAttributeAndValues);
        size_t attributesTableAttributeAndValuesDataSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(
                &attributesTableAttributeAndValues);
        void *attributesTableAttributeAndValuesData = malloc(attributesTableAttributeAndValuesDataSize);

        // prepare DATA
        AttributeAndValueSerializer::serialize(attributesTableAttributeAndValues, attributesTableAttributeAndValuesData);

        // insert into table "Tables"
        RID rid;
        m_rbfm->insertRecord(tablesFileHandle, CatalogueConstants::tablesTableAttributes,
                             attributesTableAttributeAndValuesData, rid);

        m_rbfm->closeFile(tablesFileHandle);
    }

    void RelationManager::initAttributesTable() {
        INFO("Initializing \"Attributes\"\n");
        // Create a file for table "Attributes"
        m_rbfm->createFile(CatalogueConstants::ATTRIBUTES_FILE_NAME);


        // ================= PREPARE AND INSERT ATTRS FOR "TABLES" TABLE INTO "ATTRIBUTES"
        int tid = 0;
        std::vector<std::vector<AttributeAndValue>> tablesTableAttributesForAttributesTable;
        buildAttributesForAttributesTable(tid, CatalogueConstants::tablesTableAttributes, tablesTableAttributesForAttributesTable);

        // Open the file for table "Attributes"
        FileHandle attributesTblFileHandle;
        m_rbfm->openFile(CatalogueConstants::ATTRIBUTES_FILE_NAME, attributesTblFileHandle);
        for (auto attributeAndValues: tablesTableAttributesForAttributesTable) {
            RID rid;

            size_t serializedSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(&attributeAndValues);
            void *serializedData = malloc(serializedSize);
            AttributeAndValueSerializer::serialize(attributeAndValues, serializedData);
            m_rbfm->insertRecord(attributesTblFileHandle,
                                 CatalogueConstants::attributesTableAttributes, serializedData, rid);
            free(serializedData);
        }
        m_rbfm->closeFile(attributesTblFileHandle);


        // ================= PREPARE AND INSERT ATTRS FOR "TABLES" TABLE INTO "ATTRIBUTES"
        tid = 1;
        std::vector<std::vector<AttributeAndValue>> attributesTableAttributeForAttributesTable;
        buildAttributesForAttributesTable(tid, CatalogueConstants::attributesTableAttributes, attributesTableAttributeForAttributesTable);

        // Open the file for table "Attributes"
        m_rbfm->openFile(CatalogueConstants::ATTRIBUTES_FILE_NAME, attributesTblFileHandle);
        for (auto attributeAndValues: attributesTableAttributeForAttributesTable) {
            RID rid;

            size_t serializedSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(&attributeAndValues);
            void *serializedData = malloc(serializedSize);
            AttributeAndValueSerializer::serialize(attributeAndValues, serializedData);
            m_rbfm->insertRecord(attributesTblFileHandle,
                                 CatalogueConstants::attributesTableAttributes, serializedData, rid);
            free(serializedData);
        }
        m_rbfm->closeFile(attributesTblFileHandle);
    }

    void RelationManager::buildAttributesForAttributesTable(int tableId,
                                                            const std::vector<Attribute> &attributes,
                                                            std::vector<std::vector<AttributeAndValue>> &attrsAndValuesForAttrsTable) {
        int attributePosition = 1;
        for (const Attribute &attribute: attributes) {
            std::vector<AttributeAndValue> attrsAndValues;

            // tid
            attrsAndValues.push_back(AttributeAndValue{AttributesAttributeConstants::TABLE_ID, &tableId});

            // attr name
            attrsAndValues.push_back(AttributeAndValue{AttributesAttributeConstants::ATTRIBUTE_NAME,(void*) &(attribute.name)});

            // todo: recheck conversion
            // attr type
            attrsAndValues.push_back(AttributeAndValue{AttributesAttributeConstants::ATTRIBUTE_TYPE, (void*) &(attribute.type)});

            // attr length
            attrsAndValues.push_back(AttributeAndValue{AttributesAttributeConstants::ATTRIBUTE_LENGTH, (void*) &(attribute.length)});

            // attr position
            attrsAndValues.push_back(AttributeAndValue{AttributesAttributeConstants::ATTRIBUTE_POSITION, (void*) &attributePosition});

            attrsAndValuesForAttrsTable.push_back(attrsAndValues);
            attributePosition++;
        }
    }

    void RelationManager::buildAndInsertAttributesIntoAttributesTable(const std::vector<Attribute> &attrs, int tid) {
        // 1. build AttributesAndValues for attributes table
        std::vector<std::vector<AttributeAndValue>> attributesForAttributesTable;
        buildAttributesForAttributesTable(tid, attrs, attributesForAttributesTable);

        // 2. Insert AttributesAndValues into "Attributes" table

        // Open the file for table "Attributes"
        FileHandle attributesTblFileHandle;
        m_rbfm->openFile(CatalogueConstants::ATTRIBUTES_FILE_NAME, attributesTblFileHandle);
        for (auto attributeAndValues: attributesForAttributesTable) {
            RID rid;

            size_t serializedSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(&attributeAndValues);
            void *serializedData = malloc(serializedSize);
            AttributeAndValueSerializer::serialize(attributeAndValues, serializedData);
            m_rbfm->insertRecord(attributesTblFileHandle,
                                 CatalogueConstants::attributesTableAttributes, serializedData, rid);
            free(serializedData);
        }
        m_rbfm->closeFile(attributesTblFileHandle);
    }

    std::string RelationManager::buildIndexFilename(const std::string &tableName, const std::string &attributeName) {
        return tableName + "_" + attributeName + "_index" + INDEX_FILETYPE;
    }

    std::pair<std::string, std::string> RelationManager::getTableAndAttrFromIndexFileName(const std::string& indexFname) {
        size_t firstUnderscore = indexFname.find('_');
        size_t lastUnderscore = indexFname.rfind('_');

        if (firstUnderscore == std::string::npos || lastUnderscore == std::string::npos || firstUnderscore == lastUnderscore) {
            // Format does not match expected pattern
            throw std::invalid_argument("Filename does not match expected format: tableName_attributeName_index");
        }

        std::string tableName = indexFname.substr(0, firstUnderscore);
        std::string attributeName = indexFname.substr(firstUnderscore + 1, lastUnderscore - firstUnderscore - 1);

        return {tableName, attributeName};
    }

    bool RelationManager::doesIndexExist(const std::string &tableName, const std::string &attributeName) {
        const std::string indexFilename = buildIndexFilename(tableName, attributeName);
        return file_exists(indexFilename);
    }

    void RelationManager::getIndexNames(const std::string& tableName, std::vector<std::string>& indexFiles) {
        DIR* dir;
        struct dirent* ent;

        std::string directory = "."; // Current directory; change as needed
        if ((dir = opendir(directory.c_str())) != nullptr) {
            while ((ent = readdir(dir)) != nullptr) {
                std::string filename = ent->d_name;
                // Check if the filename starts with tableName and ends with "_index"
                if (filename.rfind(tableName + "_", 0) == 0 && filename.find("_index") != std::string::npos) {
                    indexFiles.push_back(filename);
                }
            }
            closedir(dir);
        } else {
            assert(0);
        }
    }

    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

    IX_ScanIterator &RM_IndexScanIterator::getIxScanIterator() {
        return m_ix_scan_iterator;
    }

    void RM_IndexScanIterator::setIxScanIterator(const IX_ScanIterator &mIxScanIterator) {
        m_ix_scan_iterator = mIxScanIterator;
    }

    void RM_IndexScanIterator::setIxFileHandle(const IXFileHandle &mIxFileHandle) {
        m_ix_fileHandle = mIxFileHandle;
    }

    IXFileHandle &RM_IndexScanIterator::getIxFileHandle(){
        return m_ix_fileHandle;
    }

} // namespace PeterDB
