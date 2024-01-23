add_library(rbfm rbfm.cc page.cc recordTransformer.cc)
add_dependencies(rbfm pfm util googlelog)
target_link_libraries(rbfm pfm glog)
