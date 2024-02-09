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

        static const std::vector<Attribute> tablesTableAttributes;
        static const std::vector<Attribute> attributesTableAttributes;
    };

    const std::string CatalogueConstants::TABLES_TABLE_NAME = "Tables";
    const std::string CatalogueConstants::ATTRIBUTES_TABLE_NAME = "Attributes";

    const std::vector<Attribute> CatalogueConstants::tablesTableAttributes = CatalogueConstantsBuilder::buildTablesTableAttributes();
    const std::vector<Attribute> CatalogueConstants::attributesTableAttributes = CatalogueConstantsBuilder::buildAttributesTableAttributes();
}

#endif