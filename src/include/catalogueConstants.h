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

    const std::string CatalogueConstants::TABLES_TABLE_NAME = "Tables";
    const std::string CatalogueConstants::ATTRIBUTES_TABLE_NAME = "Attributes";
    const std::string CatalogueConstants::TABLES_FILE_NAME = "Tables.bin";
    const std::string CatalogueConstants::ATTRIBUTES_FILE_NAME = "Attributes.bin";

    const unsigned int CatalogueConstants::TABLES_TABLE_ID = 0;
    const unsigned int CatalogueConstants::ATTRIBUTES_TABLE_ID = 1;

    const std::vector<Attribute> CatalogueConstants::tablesTableAttributes = CatalogueConstantsBuilder::buildTablesTableAttributes();
    const std::vector<Attribute> CatalogueConstants::attributesTableAttributes = CatalogueConstantsBuilder::buildAttributesTableAttributes();
}

#endif
