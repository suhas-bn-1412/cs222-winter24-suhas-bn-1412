#ifndef _attribute_and_value_h
#define _attribute_and_value_h

#include "rbfm.h"

typedef char byte;

namespace PeterDB {
    class AttributeAndValue {
    public:
        AttributeAndValue(const Attribute &attribute, void *value);

        ~AttributeAndValue();

        Attribute getAttribute() const;

        void *getValue() const;

    private:
        Attribute m_attribute;
        void* m_value;
    };
}

#endif