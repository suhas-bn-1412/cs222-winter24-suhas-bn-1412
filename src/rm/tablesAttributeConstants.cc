#include "src/include/tablesAttributeConstants.h"

namespace PeterDB {

    const Attribute TablesAttributeConstants::TABLE_ID = Attribute {TABLE_ATTR_NAME_ID, AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH};
    const Attribute TablesAttributeConstants::TABLE_NAME = Attribute {TABLE_ATTR_NAME_NAME, AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH};
    const Attribute TablesAttributeConstants::TABLE_FILENAME = Attribute {TABLE_ATTR_NAME_FNAME, AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH};
}
