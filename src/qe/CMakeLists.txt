add_library(qe qe.cc
        ValueSerializer.cc
        ValueDeserializer.cc
)
add_dependencies(qe ix rm googlelog)
target_link_libraries(qe ix rm glog)