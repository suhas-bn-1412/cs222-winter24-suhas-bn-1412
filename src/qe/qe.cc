#include "src/include/qe.h"

#include <src/include/varcharSerDes.h>

#include "src/include/ValueSerializer.h"
#include "src/include/ValueDeserializer.h"

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition): m_condition(condition) {
        m_input_iter = input;
        m_input_iter->getAttributes(m_input_attributes);
        m_input_data = malloc(m_input_attributes.size());
        assert(m_input_data != nullptr);
    }

    Filter::~Filter() {
        assert(m_input_data != nullptr);
        free(m_input_data);
    }

    RC Filter::getNextTuple(void *data) {
        // 1. fetch the next tuple from the chained input iterator
        const RC inputIterRc = m_input_iter->getNextTuple(m_input_data);
        if (inputIterRc == QE_EOF) {
            return QE_EOF;
        }

        // 2. deserialize the input tuple
        std::vector<Value> inputTupleValues;
        ValueDeserializer::deserialize(m_input_data, m_input_attributes, inputTupleValues);

        // 3. extract the value that corresponds to the filter condition's LHS attribute
        const unsigned int filterAttributePosition = getAttributePosition(m_condition.lhsAttr);
        const Value& valueToTest = inputTupleValues.at(filterAttributePosition);

        // 4. verify that the value for the filtered-on attribute is non-null
        if (valueToTest.data != nullptr) {
            // 5. compare the current record's value with the condition
            if (isSatisfiying(m_condition, valueToTest)) {
                // 6. return the current tuple back to the caller
                // we can either seriaize our vector<value> or simply let the input iter do it for us
                ValueSerializer::serialize(inputTupleValues, data);
                return 0;
            }
        }

        // 6. Else, attempt to get the subsequent (filtered) tuple instead
        return getNextTuple(data);
    }

    unsigned int Filter::getAttributePosition(const std::string &attributeName) const {
        for (int position = 0; position < m_input_attributes.size(); ++position) {
            const auto &attribute = m_input_attributes.at(position);
            if (strcmp(attribute.name.data(), attributeName.data()) == 0) {
                return position;
            }
        }
        ERROR("Unknown attribute with name=%s\n", attributeName);
        assert(1);
        return -1;
    }

    bool Filter::isSatisfiying(const Condition &condition, const Value &value) {
        switch (condition.op) {
            case EQ_OP:
                return compareAttributeValues(condition, value) == 0;
            case LT_OP:
                return compareAttributeValues(condition, value) < 0;
            case LE_OP:
                return compareAttributeValues(condition, value) <= 0;
            case GT_OP:
                return compareAttributeValues(condition, value) > 0;
            case GE_OP:
                return compareAttributeValues(condition, value) >= 0;
            case NE_OP:
                return compareAttributeValues(condition, value) != 0;
            case NO_OP: return true;
        }
    }

    int Filter::compareAttributeValues(const Condition &condition, const Value &lhsValue) {
        switch (lhsValue.type) {
            case TypeInt: {
                int lhsVal, rhsVal;
                memcpy((void *) &lhsVal, lhsValue.data, sizeof(lhsVal));
                memcpy((void *) &rhsVal, condition.rhsValue.data, sizeof(rhsVal));
                if (lhsVal == rhsVal) {
                    return 0;
                } else if (lhsVal < rhsVal) {
                    return -1;
                } else {
                    return 1;
                }
            }
            break;
            case TypeReal: {
                float lhsVal, rhsVal;
                memcpy((void *) &lhsVal, lhsValue.data, sizeof(lhsVal));
                memcpy((void *) &rhsVal, condition.rhsValue.data, sizeof(rhsVal));
                if (lhsVal == rhsVal) {
                    return 0;
                } else if (lhsVal < rhsVal) {
                    return -1;
                } else {
                    return 1;
                }
            }
            break;
            case TypeVarChar: {
                const std::string lhsVal = VarcharSerDes::deserialize(lhsValue.data);
                const std::string rhsVal = VarcharSerDes::deserialize(condition.rhsValue.data);
                return strcmp(lhsVal.c_str(), rhsVal.c_str());
            }
            default: ERROR("Unhandled attribute type");
            assert(1);
        }
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        return m_input_iter->getAttributes(attrs);
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        m_iterator = input;
        m_projectedAttrNames = attrNames;

        m_allAttributes.clear();
        m_projectedAttrs.clear();

        Attribute dummyAttr;

        for (auto &attrName: attrNames) {
            m_projectedAttrs[attrName] = std::make_pair(dummyAttr, 0);
        }

        m_iterator->getAttributes(m_allAttributes);

        int idxOfProjectedAttrInRecord = 0;
        for (auto &attr: m_allAttributes) {
            if (m_projectedAttrs.end() != m_projectedAttrs.find(attr.name)) {
                m_projectedAttrs[attr.name] = std::make_pair(attr, idxOfProjectedAttrInRecord);
            }
            m_maxSpaceRequired += 4;
            if (TypeVarChar == attr.type) {
                m_maxSpaceRequired += attr.length;
            }
            idxOfProjectedAttrInRecord++;
        }

        for (auto &attrName: attrNames) {
            m_projectedAttrDefs.push_back(m_projectedAttrs[attrName].first);
        }

        // create the data which is used in every getNextTuple
        // should be free'd in destructor
        m_tupleData = malloc(m_maxSpaceRequired);
        assert(nullptr != m_tupleData);
        memset(m_tupleData, 0, m_maxSpaceRequired);
    }

    Project::~Project() {
        m_iterator = nullptr;
        free(m_tupleData);
        m_tupleData = nullptr;
    }

    RC Project::getNextTuple(void *data) {
        // using the iterator get the next tuple
        // then transform that data into vector of Value
        // then only pick the projected attributes and put into new vector of Value
        // then serialise that vector of Values into void* data

        if (m_eof) {
            return QE_EOF;
        }

        if (QE_EOF == m_iterator->getNextTuple(m_tupleData)) {
            m_eof = true;
            return QE_EOF;
        }

        // serialize the data into vector of values
        std::vector<Value> all_tuples;
        ValueDeserializer::deserialize(m_tupleData, m_projectedAttrDefs, all_tuples);

        std::vector<Value> projectedAttrValues;
        for (auto attrName: m_projectedAttrNames) {
            projectedAttrValues.emplace_back(all_tuples[m_projectedAttrs[attrName].second]);
        }

        ValueSerializer::serialize(projectedAttrValues, data);

        return 0;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = m_projectedAttrDefs;
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        m_leftIn = leftIn;
        m_rightIn = rightIn;
        m_condition = condition;

        m_bufferSize = (numPages-2) * PAGE_SIZE;
        m_bufferSpace = malloc(m_bufferSize);
        assert(nullptr != m_bufferSpace);
        memset(m_bufferSpace, 0, m_bufferSize);

        m_overflowTupleSize = 0;
        m_overflowTuple = malloc(PAGE_SIZE);
        assert(nullptr != m_overflowTuple);
        memset(m_overflowTuple, 0, PAGE_SIZE);

        m_attrs.clear();
        m_leftIn->getAttributes(m_leftAttrs);
        m_rightIn->getAttributes(m_rightAttrs);
        m_attrs.insert(m_attrs.end(), m_leftAttrs.begin(), m_leftAttrs.end());
        m_attrs.insert(m_attrs.end(), m_rightAttrs.begin(), m_rightAttrs.end());

        m_lhsAttrIdx = 0;
        m_rhsAttrIdx = 0;
        int i=0;

        for (auto &lhs_attr: m_leftAttrs) {
            if (condition.lhsAttr == lhs_attr.name) {
                m_lhsAttrIdx = i;
                m_compAttrType = lhs_attr.type;
                break;
            }
            i++;
        }

        i=0;
        for (auto& rhs_attr: m_rightAttrs) {
            if (condition.rhsAttr == rhs_attr.name) {
                m_rhsAttrIdx = i;
                if (m_compAttrType != rhs_attr.type) {
                    assert(-1);
                }
            }
            i++;
        }

        rightTupleData = malloc(PAGE_SIZE);
        assert(nullptr != rightTupleData);
        memset(rightTupleData, 0, PAGE_SIZE);

        // Preload tuples from leftIn into the buffer
        loadBuffer();
    }

    BNLJoin::~BNLJoin() {
        if (nullptr != m_bufferSpace) {
            free(m_bufferSpace);
            m_bufferSpace = nullptr;
        }
        if (nullptr != m_overflowTuple) {
            free(m_overflowTuple);
            m_overflowTuple = nullptr;
        }
        if (nullptr != rightTupleData) {
            free(rightTupleData);
            rightTupleData = nullptr;
        }
    }

    RC BNLJoin::getNextTuple(void *data) {
        if (m_buffer.empty()) {
            loadBuffer();
        }

        if (m_buffer.empty()) {
            return QE_EOF; // No more tuples to process
        }

        while (!m_buffer.empty()) {
            while (m_rightIn->getNextTuple(rightTupleData) != QE_EOF) {
                if (matchTuples((char*)m_bufferSpace + m_buffer.front(), rightTupleData)) {
                    combineTuples((char*)m_bufferSpace + m_buffer.front(), rightTupleData, data);
                    return 0;
                }
            }

            m_buffer.pop();
            m_rightIn->setIterator(); // Reset rightIn iterator to scan from the beginning
        }

        return QE_EOF; // No more tuples to process
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = m_attrs;
        return 0;
    }

    void BNLJoin::loadBuffer() {
        void* tupleData = malloc(PAGE_SIZE);

        // Clear the buffer for new data
        m_buffer = std::queue<unsigned>();
        unsigned bufferEnd = 0;

        if (m_overflowTupleSize) {
            memmove(m_bufferSpace, m_overflowTuple, m_overflowTupleSize);
            m_buffer.push(bufferEnd);
            bufferEnd += m_overflowTupleSize;

            memset(m_overflowTuple, 0, PAGE_SIZE);
            m_overflowTupleSize = 0;
        }

        while (!m_leftInEOF) {
            if (m_leftIn->getNextTuple(tupleData) != QE_EOF) {
                m_leftInEOF = true;
                break;
            }

            unsigned tupleSize = ValueDeserializer::calculateTupleSize(tupleData, m_leftAttrs);

            // Check if the tuple fits in the bufferSpace
            if (bufferEnd + tupleSize > m_bufferSize) {
                memmove(m_overflowTuple, tupleData, tupleSize);
                m_overflowTupleSize = tupleSize;
                break;
            }

            // Copy tuple to bufferSpace
            memmove((char*)m_bufferSpace + bufferEnd, tupleData, tupleSize);

            // Store the offset and update bufferEnd
            m_buffer.push(bufferEnd);
            bufferEnd += tupleSize;
        }

        free(tupleData);
    }

    bool BNLJoin::matchTuples(void* leftTuple, void* rightTuple) {
        std::vector<Value> leftTupleVals;
        std::vector<Value> rightTupleVals;

        ValueDeserializer::deserialize(leftTuple, m_leftAttrs, leftTupleVals);
        ValueDeserializer::deserialize(rightTuple, m_rightAttrs, rightTupleVals);

        return (leftTupleVals[m_lhsAttrIdx] == rightTupleVals[m_rhsAttrIdx]);
    }

    void BNLJoin::combineTuples(void* leftTuple, void* rightTuple, void* outTuple) {
        std::vector<Value> leftTupleVals;
        std::vector<Value> rightTupleVals;
        std::vector<Value> combinedVals;

        ValueDeserializer::deserialize(leftTuple, m_leftAttrs, leftTupleVals);
        ValueDeserializer::deserialize(rightTuple, m_rightAttrs, rightTupleVals);

        combinedVals.insert(combinedVals.end(), leftTupleVals.begin(), leftTupleVals.end());
        combinedVals.insert(combinedVals.end(), rightTupleVals.begin(), rightTupleVals.end());

        ValueSerializer::serialize(combinedVals, outTuple);
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    }

    INLJoin::~INLJoin() {
    }

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {
    }

    GHJoin::~GHJoin() {
    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
    }

    Aggregate::~Aggregate() {
    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB
