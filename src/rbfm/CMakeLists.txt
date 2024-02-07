add_library(rbfm page.cc recordTransformer.cc slot.cc record.cc pageSelector.cc rbfm.cc)
add_dependencies(rbfm pfm googlelog)
target_link_libraries(rbfm pfm glog)
