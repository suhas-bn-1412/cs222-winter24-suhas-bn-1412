#ifndef _catalogue_constants_builder_h_
#define _catalogue_constants_builder_h_

#include "rbfm.h"
#include "attributeAndValue.h"
#include <vector>

#define INTEGER_ATTRIBUTE_LENGTH 4
#define ATTRIBUTE_NAME_MAX_LENGTH 20
#define TABLE_ATTR_NAME_ID "id"
#define TABLE_ATTR_NAME_NAME "name"
#define TABLE_ATTR_NAME_FNAME "file_name"
#define ATTRIBUTES_ATTR_NAME_TABLE_ID "table_id"
#define ATTRIBUTES_ATTR_NAME_ATTR_NAME "attr_name"
#define ATTRIBUTES_ATTR_NAME_ATTR_TYPE "attr_type"
#define ATTRIBUTES_ATTR_NAME_ATTR_LENGTH "attr_length"
#define ATTRIBUTES_ATTR_NAME_POSITION "position"

namespace PeterDB {

    class CatalogueConstantsBuilder {
    public:
        static std::vector <Attribute> buildTablesTableAttributes();
        static std::vector <Attribute> buildAttributesTableAttributes();

        // Attributes and values to insert into "Tables" table
        static std::vector<AttributeAndValue> buildTablesTableAttributeAndValues();
        static std::vector<AttributeAndValue> buildAttributesTableAttributeAndValues();
    };
}

#endif
