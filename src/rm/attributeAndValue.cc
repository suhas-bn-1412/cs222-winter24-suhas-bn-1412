#include "src/include/attributeAndValue.h"
#include "src/include/varcharSerDes.h"

namespace PeterDB {

    AttributeAndValue::AttributeAndValue(const Attribute &attribute, void *value) {
        m_attribute = attribute;
        if (m_attribute.type == TypeVarChar) {
            auto *string = (std::string*) value;
            m_value.reset(malloc(VarcharSerDes::computeSerializedSize(*string)), free);
            assert(m_value.get() != nullptr);
            VarcharSerDes::serialize(*string, m_value.get());
        } else {
            m_value.reset(malloc(m_attribute.length), free);
            assert(m_value.get() != nullptr);
            memcpy(m_value.get(), value, m_attribute.length);
        }
    }

    AttributeAndValue::~AttributeAndValue() {
        //  free(m_value);
    }

     Attribute AttributeAndValue::getAttribute() const {
        return m_attribute;
    }

    void *AttributeAndValue::getValue() const {
        return m_value.get();
    }
}
