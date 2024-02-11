#ifndef _tables_attribute_constants_h_
#define _tables_attribute_constants_h_

#include "rbfm.h"

#define INTEGER_ATTRIBUTE_LENGTH 4
#define ATTRIBUTE_NAME_MAX_LENGTH 20
#define TABLE_ATTR_NAME_ID "id"
#define TABLE_ATTR_NAME_NAME "name"
#define TABLE_ATTR_NAME_FNAME "file_name"

namespace PeterDB {
    class TablesAttributeConstants {
    public:
        static const Attribute TABLE_ID;
        static const Attribute TABLE_NAME;
        static const Attribute TABLE_FILENAME;
    };

    const Attribute TABLE_ID = {TABLE_ATTR_NAME_ID, AttrType::TypeInt, INTEGER_ATTRIBUTE_LENGTH};
    const Attribute TABLE_NAME = {TABLE_ATTR_NAME_NAME, AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH};
    const Attribute TABLE_FILENAME = {TABLE_ATTR_NAME_FNAME, AttrType::TypeVarChar, ATTRIBUTE_NAME_MAX_LENGTH};
}

#endif