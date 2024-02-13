add_library(rm
        rm.cc
        catalogueConstants.cc
        catalogueConstantsBuilder.cc
        attributeAndValue.cc
        attributeAndValueSerializer.cc
        varcharSerDes.cc
)
add_dependencies(rm rbfm ix googlelog)
target_link_libraries(rm rbfm ix glog)
