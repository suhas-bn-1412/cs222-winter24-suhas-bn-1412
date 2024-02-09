#include "src/include/catalogueConstantsBuilder.h"

namespace PeterDB {
    std::vector<Attribute> CatalogueConstantsBuilder::buildTablesTableAttributes() {
        std::vector<Attribute> attributes;
        attributes.push_back({"id", AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH});
        attributes.push_back({"name", AttrType::TypeVarChar, 4});
        attributes.push_back({"file_name", AttrType::TypeVarChar, 9});
        return attributes;
    }

    std::vector<Attribute> CatalogueConstantsBuilder::buildAttributesTableAttributes() {
        std::vector<Attribute> attributes;
        attributes.push_back({"table_id", AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH});
        attributes.push_back({"attr_name", AttrType::TypeVarChar, 9});
        attributes.push_back({"attr_type", AttrType::TypeVarChar, 9});
        attributes.push_back({"position", AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH});
        return attributes;
    }
}
