#ifndef _catalogue_constants_builder_h_
#define _catalogue_constants_builder_h_

#include "rbfm.h"
#include <vector>

#define INTEGER_ATTRIBUTE_LENGTH 4

namespace PeterDB {

    class CatalogueConstantsBuilder {
    public:
        static std::vector <Attribute> buildTablesTableAttributes();
        static std::vector <Attribute> buildAttributesTableAttributes();
    };
}

#endif