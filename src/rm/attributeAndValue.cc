#include "src/include/attributeAndValue.h"

namespace PeterDB {

    AttributeAndValue::AttributeAndValue(const Attribute &attribute, void *value) {
        m_attribute = attribute;
        // we ensure enough capacity for the max length that the attribute value can have
        // if the attribute type is a varchar, the attr value must have its
        // actual length encoded into its first 4 bytes
        m_value = malloc(m_attribute.length);
        assert(m_value != nullptr);
        memcpy(m_value, value, m_attribute.length);
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