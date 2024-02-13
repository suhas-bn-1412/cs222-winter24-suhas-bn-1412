#ifndef _attributes_attribute_constants_h_
#define _attributes_attribute_constants_h_

#include "rbfm.h"

// todo: consolidate with CatalogConstantsBuilder definition
#define INTEGER_ATTRIBUTE_LENGTH 4
#define ATTRIBUTE_NAME_MAX_LENGTH 20
#define ATTRIBUTES_ATTR_NAME_TABLE_ID "table-id"
#define ATTRIBUTES_ATTR_NAME_ATTR_NAME "column-name"
#define ATTRIBUTES_ATTR_NAME_ATTR_TYPE "column-type"
#define ATTRIBUTES_ATTR_NAME_ATTR_LENGTH "column-length"
#define ATTRIBUTES_ATTR_NAME_POSITION "column-position"

namespace PeterDB {
    // Constants class to define the attributes of the "Attributes" table
    class AttributesAttributeConstants {
    public:
        static const Attribute TABLE_ID;
        static const Attribute ATTRIBUTE_NAME;
        static const Attribute ATTRIBUTE_TYPE;
        static const Attribute ATTRIBUTE_LENGTH;
        static const Attribute ATTRIBUTE_POSITION;
    };

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
}
#endif