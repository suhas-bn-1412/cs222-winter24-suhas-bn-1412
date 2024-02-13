#include "src/include/catalogueConstants.h"
#include "src/include/attributesAttributeConstants.h"

namespace PeterDB {

    const std::string CatalogueConstants::TABLES_TABLE_NAME = "Tables";
    const std::string CatalogueConstants::ATTRIBUTES_TABLE_NAME = "Columns";
    const std::string CatalogueConstants::TABLES_FILE_NAME = "Tables";
    const std::string CatalogueConstants::ATTRIBUTES_FILE_NAME = "Columns";

    const unsigned int CatalogueConstants::TABLES_TABLE_ID = 0;
    const unsigned int CatalogueConstants::ATTRIBUTES_TABLE_ID = 1;

        const std::vector<Attribute> CatalogueConstants::tablesTableAttributes ({
            Attribute {TABLE_ATTR_NAME_ID, AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH},
            Attribute {TABLE_ATTR_NAME_NAME, AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH},
            Attribute {TABLE_ATTR_NAME_FNAME, AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH},
        });
        const std::vector<Attribute> CatalogueConstants::attributesTableAttributes ({
            {ATTRIBUTES_ATTR_NAME_TABLE_ID, AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH},
            {ATTRIBUTES_ATTR_NAME_ATTR_NAME, AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH},
            {ATTRIBUTES_ATTR_NAME_ATTR_TYPE, AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH},
            {ATTRIBUTES_ATTR_NAME_ATTR_LENGTH, AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH},
            {ATTRIBUTES_ATTR_NAME_POSITION, AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH},
        });
}
