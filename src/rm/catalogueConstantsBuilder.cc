#include "src/include/catalogueConstantsBuilder.h"

namespace PeterDB {

    const Attribute TablesAttributeConstants::TABLE_ID = Attribute {TABLE_ATTR_NAME_ID, AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH};
    const Attribute TablesAttributeConstants::TABLE_NAME = Attribute {TABLE_ATTR_NAME_NAME, AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH};
    const Attribute TablesAttributeConstants::TABLE_FILENAME = Attribute {TABLE_ATTR_NAME_FNAME, AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH};

    const Attribute AttributesAttributeConstants::TABLE_ID = Attribute{ATTRIBUTES_ATTR_NAME_TABLE_ID,
                                                                       AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH};
    const Attribute AttributesAttributeConstants::ATTRIBUTE_NAME = Attribute{ATTRIBUTES_ATTR_NAME_ATTR_NAME,
                                                                             AttrType::TypeVarChar,
                                                                             ATTRIBUTE_NAME_MAX_LENGTH};
    const Attribute AttributesAttributeConstants::ATTRIBUTE_TYPE = Attribute{ATTRIBUTES_ATTR_NAME_ATTR_TYPE,
                                                                             AttrType::TypeInt,
                                                                             INTEGER_ATTRIBUTE_LENGTH};
    const Attribute AttributesAttributeConstants::ATTRIBUTE_LENGTH = Attribute{ATTRIBUTES_ATTR_NAME_ATTR_LENGTH,
                                                                               AttrType::TypeInt,
                                                                               INTEGER_ATTRIBUTE_LENGTH};
    const Attribute AttributesAttributeConstants::ATTRIBUTE_POSITION = Attribute{ATTRIBUTES_ATTR_NAME_POSITION,
                                                                                 AttrType::TypeInt,
                                                                                 INTEGER_ATTRIBUTE_LENGTH};

    // Attributes and values to insert into "Tables" table
    void CatalogueConstantsBuilder::buildTablesTableAttributeAndValues(std::vector<AttributeAndValue> &attributesAndValues) {
        int tableId = 0;
        std::string tableName = "Tables";
        std::string tableFileName = "Tables";
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_ID, &tableId));
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_NAME, &tableName));
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_FILENAME, &tableFileName));
    }

// Attributes and values to insert into "Tables" table
    void CatalogueConstantsBuilder::buildAttributesTableAttributeAndValues(std::vector<AttributeAndValue> &attributesAndValues) {
        int tableId = 1;
        std::string tableName = "Columns";
        std::string tableFileName = "Columns";
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_ID, &tableId));
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_NAME, &tableName));
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_FILENAME, &tableFileName));
    }
}
