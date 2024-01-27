add_library(pfm pfm.cc)
add_dependencies(pfm googlelog util)
target_link_libraries(pfm glog util)