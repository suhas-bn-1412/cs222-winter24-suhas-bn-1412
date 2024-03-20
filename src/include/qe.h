#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <queue>
#include <string>
#include <limits>
#include <float.h>

#include "rm.h"
#include "ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data = nullptr;   // value

        Value() {}

        Value(AttrType t, void* d) {
            type = t;

            if (nullptr != d) {
                int dataLen = 4;
                if (TypeVarChar == t) {
                    dataLen += *((uint32_t*)d);
                }

                data = malloc(dataLen);
                assert(nullptr != data);
                memmove(data, d, dataLen);
            }
        }

        // copy constructor
        Value(const Value& other) {
            type = other.type;

            if (nullptr != other.data) {
                int dataLen = 4;
                if (TypeVarChar == type) {
                    dataLen += *((uint32_t*)other.data);
                }

                data = malloc(dataLen);
                assert(nullptr != data);
                memcpy(data, other.data, dataLen);
            }
        }

        Value& operator=(const Value& other) {
            type = other.type;

            if (nullptr != other.data) {
                int dataLen = 4;
                if (TypeVarChar == type) {
                    dataLen += *((uint32_t*)other.data);
                }

                data = malloc(dataLen);
                assert(nullptr != data);
                memmove(data, other.data, dataLen);
            }
            return *this;
        }

        bool operator==(const Value& other) {
            assert(type == other.type);

            switch (type) {
                case TypeInt:
                    return (*((uint32_t*)data) == *((uint32_t*)other.data));
                case TypeReal:
                    return (*((float*)data) == *((float*)other.data));
                case TypeVarChar:
                {
                    uint32_t len1 = * ( (uint32_t*) data);
                    uint32_t len2 = * ( (uint32_t*) other.data);
                    if (len1 != len2) return false;

                    char* s1 = new char[len1 + 1];
                    char* s2 = new char[len2 + 1];

                    std::memcpy(s1, (char*)data + 4, len1);
                    std::memcpy(s2, (char*)other.data + 4, len2);

                    s1[len1] = '\0'; // Null-terminate the strings
                    s2[len2] = '\0';

                    int result = strcmp(s1, s2);

                    delete[] s1;
                    delete[] s2;

                    return (0 == result);
                }
                default:
                    assert(0);
            }
            assert(0);
            return false;
        }

        ~Value() {
            if (nullptr != data) {
                free(data);
                data = nullptr;
            }
        }

    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;


            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }

            return 0;
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
        // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        static size_t getTupleDataLengthMax(PeterDB::Iterator * input_iterator);

        unsigned int getAttributePosition(const std::string & attributeName) const;

        static int compareAttributeValues(const Condition & condition, const Value & lhsValue);

        static bool isSatisfiying(const Condition & condition, const Value & value);

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

    private:
        Iterator *m_input_iter;
        std::vector<Attribute> m_input_attributes;
        void *m_input_data;
        const Condition &m_condition;
    };

    class Project : public Iterator {
        // Projection operator
    private:
        bool m_eof = false;
        Iterator* m_iterator = nullptr;
        std::vector<std::string> m_projectedAttrNames;
        std::vector<Attribute> m_allAttributes;
        std::vector<Attribute> m_projectedAttrDefs;

        // store projected attribute definition and where to find the projected attribute
        // in the tuple which we get from iterator.getNextTuple
        std::unordered_map<std::string, std::pair<Attribute, int>> m_projectedAttrs;

        int m_maxSpaceRequired = 0;
        void* m_tupleData = nullptr;

    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
    private:
        Iterator *m_leftIn;
        TableScan *m_rightIn;
        Condition m_condition;

        int m_lhsAttrIdx = 0;
        int m_rhsAttrIdx = 0;
        AttrType m_compAttrType;
        std::vector<Attribute> m_attrs;
        std::vector<Attribute> m_leftAttrs, m_rightAttrs;

        std::queue<unsigned> m_buffer;
        unsigned m_bufferSize = 0;
        unsigned m_overflowTupleSize = 0;

        void* m_bufferSpace = nullptr;
        void* m_overflowTuple = nullptr;
        void* rightTupleData = nullptr;

        bool m_leftInEOF = false;

        void loadBuffer();
        bool matchTuples(void* leftTuple, void* rightTuple);
        void combineTuples(void* leftTuple, void* rightTuple, void* outTuple);
    
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    typedef struct AggOutput {
        float cnt = 0;
        float min = FLT_MAX;
        float max = FLT_MIN;
        float sum = 0;

        AggOutput() {}

        double getVal(AggregateOp op) {
            switch (op) {
                case COUNT:
                    return cnt;
                case MIN:
                    return min;
                case MAX:
                    return max;
                case SUM:
                    return sum;
                case AVG:
                    return (sum/cnt);
            }
            return 0;
        }
    } AggOutput;

    class Aggregate : public Iterator {
        // Aggregation operator
    private:
        Iterator* m_iterator = nullptr;
        std::vector<Attribute> m_attrs;

        AggregateOp m_op;
        bool m_eof = false;

        Attribute m_aggAttr;
        unsigned m_aggAttrIdx = 0;

        bool m_groupBy = false;
        Attribute m_groupAttr;
        unsigned m_groupAttrIdx = 0;

        void* m_tupleData = nullptr;

        std::unordered_map<int, AggOutput> m_aggOpInt;
        std::unordered_map<float, AggOutput> m_aggOpReal;
        std::unordered_map<std::string, AggOutput> m_aggOpVarchar;

        void fetchAndStoreData();

    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };
} // namespace PeterDB

#endif // _qe_h_
