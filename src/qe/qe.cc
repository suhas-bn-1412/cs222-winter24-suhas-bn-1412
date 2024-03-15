#include "src/include/qe.h"
#include "src/include/ValueSerializer.h"
#include "src/include/ValueDeserializer.h"

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        m_input_iter = input;
        m_condition = condition;
    }

    Filter::~Filter() {

    }

    RC Filter::getNextTuple(void *data) {


        return -1;
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

        for (auto& attrName: attrNames) {
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

    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
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
