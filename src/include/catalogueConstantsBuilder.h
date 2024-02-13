#ifndef _catalogue_constants_builder_h_
#define _catalogue_constants_builder_h_

#include "rbfm.h"
#include "attributeAndValue.h"
#include <vector>

#define INTEGER_ATTRIBUTE_LENGTH 4
#define ATTRIBUTE_NAME_MAX_LENGTH 50
#define TABLE_ATTR_NAME_ID "table-id"
#define TABLE_ATTR_NAME_NAME "table-name"
#define TABLE_ATTR_NAME_FNAME "file-name"
#define ATTRIBUTES_ATTR_NAME_TABLE_ID "table-id"
#define ATTRIBUTES_ATTR_NAME_ATTR_NAME "column-name"
#define ATTRIBUTES_ATTR_NAME_ATTR_TYPE "column-type"
#define ATTRIBUTES_ATTR_NAME_ATTR_LENGTH "column-length"
#define ATTRIBUTES_ATTR_NAME_POSITION "column-position"

namespace PeterDB {

    class TablesAttributeConstants {
    public:
        static const Attribute TABLE_ID;
        static const Attribute TABLE_NAME;
        static const Attribute TABLE_FILENAME;
    };

    class CatalogueConstantsBuilder {
    public:
        // Attributes and values to insert into "Tables" table
        static void buildTablesTableAttributeAndValues(std::vector<AttributeAndValue>&);
        static void buildAttributesTableAttributeAndValues(std::vector<AttributeAndValue>&);
    };

    class AttributesAttributeConstants {
    public:
        static const Attribute TABLE_ID;
        static const Attribute ATTRIBUTE_NAME;
        static const Attribute ATTRIBUTE_TYPE;
        static const Attribute ATTRIBUTE_LENGTH;
        static const Attribute ATTRIBUTE_POSITION;
    };
}

#endif
