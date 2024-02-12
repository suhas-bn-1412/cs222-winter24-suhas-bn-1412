#include "src/include/rm.h"
#include "src/include/catalogueConstants.h"
#include "src/include/attributeAndValueSerializer.h"
#include "src/include/attributesAttributeConstants.h"
#include "src/include/tablesAttributeConstants.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        if (nullptr == _relation_manager.m_rbfm) {
            _relation_manager.m_rbfm = &RecordBasedFileManager::instance();
        }
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    std::string getFileName(const std::string& tableName) {
        return tableName + ".bin";
    }

    RC RelationManager::createCatalog() {
        initTablesTable();
        initAttributesTable();
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        m_rbfm->destroyFile(CatalogueConstants::TABLES_FILE_NAME);
        m_rbfm->destroyFile(CatalogueConstants::ATTRIBUTES_FILE_NAME);
    }

    RC RelationManager::createTable(const std::string &tablezName, const std::vector<Attribute> &attrs) {
        // find next TID
        int tid = computeNextTableId();
        std::string tableFileName = buildFilename(tablezName);

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

        AttributeAndValueSerializer::serialize(&tablesTableAttributeAndValues, tablesTableAttributeAndValuesData);

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

        return 0;
    }


    RC RelationManager::deleteTable(const std::string &tableName) {
        m_rbfm->destroyFile(getFileName(tableName));
        return 0;
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
        // first 4 bytes = num of fields present in the returned data = attrsToReadFromAttributesTable.size()
        // next 1 bytes = representing the nullFlags of all the attibutes
        // next 4 bytes = length of the varchar holding the attribute name = n
        // next n bytes = attribute name
        // next 4 bytes = attribute type
        // next 4 bytes = attribute length
        assert(3 == *( (uint32_t*) data ) );

        uint32_t strlen = *( (uint32_t*) ( (char*)data + 5) );
        std::string attr_name;
        attr_name.assign(( (char*)data + 4 + 1 + 4), strlen);

        uint32_t attr_type = *( (uint32_t*) ((char*)data + 4 + 1 + 4 + strlen) );
        uint32_t attr_length = *( (uint32_t*) ((char*)data + 4 + 1 + 4 + strlen + 4) );

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
            tableFileHandle.closeFile();
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
        memmove( (void*)( (char*) conditionValue + 4), &tableName, tableName.size());

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
            tableFileHandle.closeFile();
            attributesFileHandle.closeFile();
            free(conditionValue);
            return -1;
        }
        free(conditionValue);

        // using scan iterator for the table, read the entry for tabeName and read only the id
        // construct the data into which the id is written
        void* tableIdData = malloc(4 + 1 + 4); // num of attrs in the record, nullflag byte, bytes to store table-id data
        assert(nullptr != tableIdData);
        RID tableIdRid;

        auto gn = rbfmsi.getNextRecord(tableIdRid, tableIdData);
        if (0 != gn) {
            ERROR("Error while getting table id for table %s, from Tables table", tableName);
            tableFileHandle.closeFile();
            attributesFileHandle.closeFile();
            free(tableIdData);
            return -1;
        }
        tableFileHandle.closeFile();

        // now tableIdData is as follows
        // first 4 bytes =  1, representing there is only one attribute projected out of the scan
        // next 1 byte = <nullflags>, representing theres only one byte used for nullflags
        // next 4 bytes = <table-id> representing the table id value for table with name=tableName
        assert(1 == * ( (uint32_t*) tableIdData ) );

        // read the table Attributes and read for all the entries with table_id = id

        // attributes to project from attributes table
        std::vector<std::string> attrsToReadFromAttributesTable = {ATTRIBUTES_ATTR_NAME_ATTR_NAME,
                                                                   ATTRIBUTES_ATTR_NAME_ATTR_TYPE,
                                                                   ATTRIBUTES_ATTR_NAME_ATTR_LENGTH};

        // num fields + nullflags + length of varchar attr + varchar attr + int attr + int attr
        unsigned maxSpaceReq = 4 + 1 + 4 + ATTRIBUTE_NAME_MAX_LENGTH + 4 + 4;

        // prepare the arguments for scan function
        conditionValue = (void*)( (char*) tableIdData + 5);
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
            attributesFileHandle.closeFile();
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

        attributesFileHandle.closeFile();
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
        // first 4 bytes = num of fields present in the returned data = attrsToReadFromAttributesTable.size()
        // next 1 bytes = representing the nullFlags of all the attibutes
        // next 4 bytes = length of the varchar holding the attribute name = n
        // next n bytes = attribute name
        // next 4 bytes = attribute type
        // next 4 bytes = attribute length
        assert(3 == *( (uint32_t*) data ) );

        uint32_t strlen = *( (uint32_t*) ( (char*)data + 5) );
        std::string attr_name;
        attr_name.assign(( (char*)data + 4 + 1 + 4), strlen);

        uint32_t attr_type = *( (uint32_t*) ((char*)data + 4 + 1 + 4 + strlen) );
        uint32_t attr_length = *( (uint32_t*) ((char*)data + 4 + 1 + 4 + strlen + 4) );

        Attribute attr;
        attr.name = attr_name;
        attr.type = getAttrType(attr_type);
        attr.length = attr_length;
        return attr;
    }

    RC RelationManager::createTablesAndAttributesFH(FileHandle& tableFileHandle,
                                                    FileHandle& attributesFileHandle) {
        auto of = m_rbfm->openFile(CatalogueConstants::TABLES_FILE_NAME, tableFileHandle);
        if (0 != of) {
            ERROR("Error while opening %s file", CatalogueConstants::TABLES_FILE_NAME);
            return of;
        }

        of = m_rbfm->openFile(CatalogueConstants::TABLES_FILE_NAME, attributesFileHandle);
        if (0 != of) {
            ERROR("Error while opening %s file", CatalogueConstants::TABLES_FILE_NAME);
            tableFileHandle.closeFile();
            return of;
        }

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        FileHandle tableFileHandle;
        FileHandle attributesFileHandle;

        if (0 != createTablesAndAttributesFH(tableFileHandle, attributesFileHandle)) {
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
        memmove( (void*)( (char*) conditionValue + 4), &tableName, tableName.size());

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
            tableFileHandle.closeFile();
            attributesFileHandle.closeFile();
            free(conditionValue);
            return -1;
        }
        free(conditionValue);

        // using scan iterator for the table, read the entry for tabeName and read only the id
        // construct the data into which the id is written
        void* tableIdData = malloc(4 + 1 + 4); // num of attrs in the record, nullflag byte, bytes to store table-id data
        assert(nullptr != tableIdData);
        RID tableIdRid;

        auto gn = rbfmsi.getNextRecord(tableIdRid, tableIdData);
        if (0 != gn) {
            ERROR("Error while getting table id for table %s, from Tables table", tableName);
            tableFileHandle.closeFile();
            attributesFileHandle.closeFile();
            free(tableIdData);
            return -1;
        }
        tableFileHandle.closeFile();

        // now tableIdData is as follows
        // first 4 bytes =  1, representing there is only one attribute projected out of the scan
        // next 1 byte = <nullflags>, representing theres only one byte used for nullflags
        // next 4 bytes = <table-id> representing the table id value for table with name=tableName
        assert(1 == * ( (uint32_t*) tableIdData ) );

        // read the table Attributes and read for all the entries with table_id = id

        // attributes to project from attributes table
        std::vector<std::string> attrsToReadFromAttributesTable = {ATTRIBUTES_ATTR_NAME_ATTR_NAME,
                                                                   ATTRIBUTES_ATTR_NAME_ATTR_TYPE,
                                                                   ATTRIBUTES_ATTR_NAME_ATTR_LENGTH};

        // num fields + nullflags + length of varchar attr + varchar attr + int attr + int attr
        unsigned maxSpaceReq = 4 + 1 + 4 + ATTRIBUTE_NAME_MAX_LENGTH + 4 + 4;

        // prepare the arguments for scan function
        conditionValue = (void*)( (char*) tableIdData + 5);
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
            attributesFileHandle.closeFile();
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

        attributesFileHandle.closeFile();
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
        std::string filename = getFileName(tableName);
        if (0 != m_rbfm->openFile(filename, fh)) {
            ERROR("Error while opening the file %s", filename);
            return -1;
        }

        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        std::vector<Attribute> attrs;
        FileHandle fh;

        if (0 != getFileHandleAndAttributes(tableName, fh, attrs)) {
            ERROR("Error while getting filehandle and attributes for table %s", tableName);
            return -1;
        }

        if ( 0 != m_rbfm->insertRecord(fh, attrs, data, rid)) {
            ERROR("Error while inserting the record into table %s", tableName);
            fh.closeFile();
            return -1;
        }
        fh.closeFile();
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        std::vector<Attribute> attrs;
        FileHandle fh;

        if (0 != getFileHandleAndAttributes(tableName, fh, attrs)) {
            ERROR("Error while getting filehandle and attributes for table %s", tableName);
            return -1;
        }
        std::vector<Attribute> attrs;
        FileHandle fh;

        if ( 0 != m_rbfm->deleteRecord(fh, attrs, rid)) {
            ERROR("Error while deleting the record from table %s", tableName);
            fh.closeFile();
            return -1;
        }
        fh.closeFile();
        return 0;
    }

        if ( 0 != m_rbfm->deleteRecord(fh, attrs, rid)) {
            ERROR("Error while deleting the record from table %s", tableName);
            fh.closeFile();
            return -1;
        }
        fh.closeFile();
        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        std::vector<Attribute> attrs;
        FileHandle fh;

        if (0 != getFileHandleAndAttributes(tableName, fh, attrs)) {
            ERROR("Error while getting filehandle and attributes for table %s", tableName);
            return -1;
        }

        if ( 0 != m_rbfm->updateRecord(fh, attrs, data, rid)) {
            ERROR("Error while updating the record in table %s", tableName);
            fh.closeFile();
            return -1;
        }
        fh.closeFile();
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
            fh.closeFile();
            return -1;
        }
        fh.closeFile();
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
            fh.closeFile();
            return -1;
        }
        fh.closeFile();
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return -1;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }

    void RelationManager::initTablesTable() {
        // Create a file for table "Tables"
        m_rbfm->createFile(CatalogueConstants::TABLES_FILE_NAME);

        // Open the file for table "Tables"
        FileHandle tablesFileHandle;
        m_rbfm->openFile(CatalogueConstants::TABLES_FILE_NAME, tablesFileHandle);

        // Initialize table "Tables" by inserting 2 rows (records)
        // corresponding to tableId-tableName-filename of "Tables" and "Attributes"

        // PREPARE AND INSERT ROW #1

        // Fetch attributes
        std::vector<AttributeAndValue> tablesTableAttributeAndValues = CatalogueConstantsBuilder::buildTablesTableAttributeAndValues();
        size_t tablesTableAttributeAndValuesDataSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(
                &tablesTableAttributeAndValues);
        void *tablesTableAttributeAndValuesData = malloc(tablesTableAttributeAndValuesDataSize);

        // prepare DATA
        AttributeAndValueSerializer::serialize(&tablesTableAttributeAndValues, tablesTableAttributeAndValuesData);

        // insert into table "Tables"
        RID rid1;
        m_rbfm->insertRecord(tablesFileHandle, CatalogueConstants::tablesTableAttributes,
                             tablesTableAttributeAndValuesData, rid1);

        // PREPARE AND INSERT ROW #1

        // Fetch attributes
        std::vector<AttributeAndValue> attributesTableAttributeAndValues = CatalogueConstantsBuilder::buildAttributesTableAttributeAndValues();
        size_t attributesTableAttributeAndValuesDataSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(
                &attributesTableAttributeAndValues);
        void *attributesTableAttributeAndValuesData = malloc(attributesTableAttributeAndValuesDataSize);

        // prepare DATA
        AttributeAndValueSerializer::serialize(&attributesTableAttributeAndValues, attributesTableAttributeAndValuesData);

        // insert into table "Tables"
        RID rid;
        m_rbfm->insertRecord(tablesFileHandle, CatalogueConstants::tablesTableAttributes,
                             attributesTableAttributeAndValuesData, rid);

        m_rbfm->closeFile(tablesFileHandle);
    }

    void RelationManager::initAttributesTable() {
        // Create a file for table "Attributes"
        m_rbfm->createFile(CatalogueConstants::ATTRIBUTES_FILE_NAME);


        // ================= PREPARE AND INSERT ATTRS FOR "TABLES" TABLE INTO "ATTRIBUTES"
        int tid = 0;
        std::vector<std::vector<AttributeAndValue>> tablesTableAttributesForAttributesTable =
                buildAttributesForAttributesTable(tid, CatalogueConstants::tablesTableAttributes);

        // Open the file for table "Attributes"
        FileHandle attributesTblFileHandle;
        m_rbfm->openFile(CatalogueConstants::ATTRIBUTES_FILE_NAME, attributesTblFileHandle);
        for (auto attributeAndValues: tablesTableAttributesForAttributesTable) {
            RID rid;

            size_t serializedSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(&attributeAndValues);
            void *serializedData = malloc(serializedSize);
            AttributeAndValueSerializer::serialize(&attributeAndValues, serializedData);
            m_rbfm->insertRecord(attributesTblFileHandle,
                                 CatalogueConstants::attributesTableAttributes, serializedData, rid);
            free(serializedData);
        }
        m_rbfm->closeFile(attributesTblFileHandle);


        // ================= PREPARE AND INSERT ATTRS FOR "TABLES" TABLE INTO "ATTRIBUTES"
        tid = 1;
        std::vector<std::vector<AttributeAndValue>> attributesTableAttributeForAttributesTable =
                buildAttributesForAttributesTable(tid, CatalogueConstants::attributesTableAttributes);

        // Open the file for table "Attributes"
        m_rbfm->openFile(CatalogueConstants::ATTRIBUTES_FILE_NAME, attributesTblFileHandle);
        for (auto attributeAndValues: attributesTableAttributeForAttributesTable) {
            RID rid;

            size_t serializedSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(&attributeAndValues);
            void *serializedData = malloc(serializedSize);
            AttributeAndValueSerializer::serialize(&attributeAndValues, serializedData);
            m_rbfm->insertRecord(attributesTblFileHandle,
                                 CatalogueConstants::attributesTableAttributes, serializedData, rid);
            free(serializedData);
        }
        m_rbfm->closeFile(attributesTblFileHandle);
    }

    std::vector<std::vector<AttributeAndValue>>
    RelationManager::buildAttributesForAttributesTable(int tableId, const std::vector<Attribute> &attributes) {
        std::vector<std::vector<AttributeAndValue>> attrsAndValuesForAttrsTable;
        int attributePosition = 0;
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
        return attrsAndValuesForAttrsTable;
    }

    void RelationManager::buildAndInsertAttributesIntoAttributesTable(const std::vector<Attribute> &attrs, int tid) {
        // 1. build AttributesAndValues for attributes table
        std::vector<std::vector<AttributeAndValue>> attributesForAttributesTable =
                buildAttributesForAttributesTable(tid, attrs);

        // 2. Insert AttributesAndValues into "Attributes" table

        // Open the file for table "Attributes"
        FileHandle attributesTblFileHandle;
        m_rbfm->openFile(CatalogueConstants::ATTRIBUTES_FILE_NAME, attributesTblFileHandle);
        for (auto attributeAndValues: attributesForAttributesTable) {
            RID rid;

            size_t serializedSize = AttributeAndValueSerializer::computeSerializedDataLenBytes(&attributeAndValues);
            void *serializedData = malloc(serializedSize);
            AttributeAndValueSerializer::serialize(&attributeAndValues, serializedData);
            m_rbfm->insertRecord(attributesTblFileHandle,
                                 CatalogueConstants::attributesTableAttributes, serializedData, rid);
            free(serializedData);
        }
        m_rbfm->closeFile(attributesTblFileHandle);
    }

    int RelationManager::computeNextTableId() {
        //todo:
        return 0;
    }

    std::string RelationManager::buildFilename(const std::string &tableName) {
        return tableName + ".bin";
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB