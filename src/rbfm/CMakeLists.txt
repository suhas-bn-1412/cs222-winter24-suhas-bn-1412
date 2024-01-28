add_library(rbfm rbfm.cc page.cc recordTransformer.cc slot.cc)
add_dependencies(rbfm pfm googlelog)
target_link_libraries(rbfm pfm glog)