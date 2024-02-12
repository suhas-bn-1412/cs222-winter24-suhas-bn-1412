#ifndef _catalogue_constants_h_
#define _catalogue_constants_h_

#include "catalogueConstantsBuilder.h"
#include "rbfm.h"
#include <vector>

namespace PeterDB {

    class CatalogueConstants {
    public:
        static const std::string TABLES_TABLE_NAME;
        static const std::string ATTRIBUTES_TABLE_NAME;
        static const std::string TABLES_FILE_NAME;
        static const std::string ATTRIBUTES_FILE_NAME;

        static const unsigned int TABLES_TABLE_ID;
        static const unsigned int ATTRIBUTES_TABLE_ID;

        static const std::vector<Attribute> tablesTableAttributes;
        static const std::vector<Attribute> attributesTableAttributes;
    };

}

#endif
