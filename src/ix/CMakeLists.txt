add_library(ix ix.cc
    nonLeafPage.cc leafPage.cc
)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)