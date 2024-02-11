#include "src/include/catalogueConstantsBuilder.h"

namespace PeterDB {
    std::vector<Attribute> CatalogueConstantsBuilder::buildTablesTableAttributes() {
        std::vector<Attribute> attributes;
        attributes.push_back({TABLE_ATTR_NAME_ID,
                              AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH});
        attributes.push_back({TABLE_ATTR_NAME_NAME,
                              AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH});
        attributes.push_back({TABLE_ATTR_NAME_FNAME,
                              AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH});
        return attributes;
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
}
