#ifndef _page_ser_des_constants_h_
#define _page_ser_des_constants_h_

#include <cstddef>
#include "rbfm.h"

namespace PeterDB {
    class PageSerDesConstants {
    public:
        /*
         * fields are written to/ read from the very end of each page
        *  this way, any field addition/ deletion/ modification shall not affect
        * the page's data handling logic
        *
        * fields are defined in order of last to first.
        * _freeByteCount
        * isLeafPage
        * _keyType
        * _nextPageNum
        * numKeys
        */
        static const size_t FREEBYTE_COUNT_VAR_SIZE = sizeof(unsigned int);
        static const size_t IS_LEAF_PAGE_VAR_SIZE = sizeof(bool);
        static const size_t KEY_TYPE_VAR_SIZE = sizeof(int);
        static const size_t NEXT_PAGE_NUM_VAR_SIZE = sizeof(int);
        static const size_t NUM_KEYS_VAR_SIZE = sizeof(unsigned int);

        static const unsigned int FREEBYTE_COUNT_OFFSET = PAGE_SIZE - FREEBYTE_COUNT_VAR_SIZE;
        static const unsigned int IS_LEAF_PAGE_OFFSET = FREEBYTE_COUNT_OFFSET - IS_LEAF_PAGE_VAR_SIZE;
        static const unsigned int KEY_TYPE_OFFSET = IS_LEAF_PAGE_OFFSET - KEY_TYPE_VAR_SIZE;
        static const unsigned int NEXT_PAGE_NUM_OFFSET = KEY_TYPE_OFFSET - NEXT_PAGE_NUM_VAR_SIZE;
        static const unsigned int NUM_KEYS_OFFSET = NEXT_PAGE_NUM_OFFSET - NUM_KEYS_VAR_SIZE;
    };
}

#endif
