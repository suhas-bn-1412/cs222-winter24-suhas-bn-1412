#include "src/include/catalogueConstantsBuilder.h"
#include "src/include/tablesAttributeConstants.h"

namespace PeterDB {
    std::vector<Attribute> CatalogueConstantsBuilder::buildTablesTableAttributes() {
        std::vector<Attribute> attributes;
        attributes.push_back(TablesAttributeConstants::TABLE_ID);
        attributes.push_back(TablesAttributeConstants::TABLE_NAME);
        attributes.push_back(TablesAttributeConstants::TABLE_FILENAME);
        attributes.push_back({TABLE_ATTR_NAME_ID,
                              AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH});
        attributes.push_back({TABLE_ATTR_NAME_NAME,
                              AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH});
        attributes.push_back({TABLE_ATTR_NAME_FNAME,
                              AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH});
        return attributes;
    }

    // Attributes and values to insert into "Tables" table
    std::vector<AttributeAndValue> CatalogueConstantsBuilder::buildTablesTableAttributeAndValues() {
        std::vector<AttributeAndValue> attributesAndValues;
        int tableId = 0;
        std::string tableName = "Tables";
        std::string tableFileName = "Tables.bin";
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_ID, &tableId));
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_NAME, &tableName));
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_FILENAME, &tableFileName));
        return attributesAndValues;
    }

    std::vector<Attribute> CatalogueConstantsBuilder::buildAttributesTableAttributes() {
        std::vector<Attribute> attributes;
        attributes.push_back({ATTRIBUTES_ATTR_NAME_TABLE_ID,
                              AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH});
        attributes.push_back({ATTRIBUTES_ATTR_NAME_ATTR_NAME,
                              AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH});
        attributes.push_back({ATTRIBUTES_ATTR_NAME_ATTR_TYPE,
                              AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH});
        attributes.push_back({ATTRIBUTES_ATTR_NAME_ATTR_LENGTH,
                              AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH});
        attributes.push_back({ATTRIBUTES_ATTR_NAME_POSITION,
                              AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH});
        return attributes;
    }

// Attributes and values to insert into "Tables" table
    std::vector<AttributeAndValue> CatalogueConstantsBuilder::buildAttributesTableAttributeAndValues() {
        std::vector<AttributeAndValue> attributesAndValues;
        int tableId = 1;
        std::string tableName = "Attributes";
        std::string tableFileName = "Attributes.bin";
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_ID, &tableId));
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_NAME, &tableName));
        attributesAndValues.push_back(AttributeAndValue(TablesAttributeConstants::TABLE_FILENAME, &tableFileName));
        return attributesAndValues;
    }
}
