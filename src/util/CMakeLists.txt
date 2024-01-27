add_library(util util.cc)
add_dependencies(util googlelog)
target_link_libraries(util glog)