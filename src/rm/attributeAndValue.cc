#include "src/include/attributeAndValue.h"
#include "src/include/varcharSerDes.h"

namespace PeterDB {

    AttributeAndValue::AttributeAndValue(const Attribute &attribute, void *value) {
        m_attribute = attribute;
        if (m_attribute.type == TypeVarChar) {
            auto *string = (std::string*) value;
            m_value = malloc(VarcharSerDes::computeSerializedSize(*string));
            assert(m_value != nullptr);
            VarcharSerDes::serialize(*string, m_value);
        } else {
            m_value = malloc(m_attribute.length);
            assert(m_value != nullptr);
            memcpy(m_value, value, m_attribute.length);
        }
    }

    AttributeAndValue::~AttributeAndValue() {
        free(m_value);
    }

     Attribute AttributeAndValue::getAttribute() const {
        return m_attribute;
    }

    void *AttributeAndValue::getValue() const {
        return m_value;
    }
}