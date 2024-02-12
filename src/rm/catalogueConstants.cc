#include "src/include/catalogueConstants.h"

namespace PeterDB {

    const std::string CatalogueConstants::TABLES_TABLE_NAME = "Tables";
    const std::string CatalogueConstants::ATTRIBUTES_TABLE_NAME = "Attributes";
    const std::string CatalogueConstants::TABLES_FILE_NAME = "Tables.bin";
    const std::string CatalogueConstants::ATTRIBUTES_FILE_NAME = "Attributes.bin";

    const unsigned int CatalogueConstants::TABLES_TABLE_ID = 0;
    const unsigned int CatalogueConstants::ATTRIBUTES_TABLE_ID = 1;

    const std::vector<Attribute> CatalogueConstants::tablesTableAttributes = CatalogueConstantsBuilder::buildTablesTableAttributes();
    const std::vector<Attribute> CatalogueConstants::attributesTableAttributes = CatalogueConstantsBuilder::buildAttributesTableAttributes();
}
