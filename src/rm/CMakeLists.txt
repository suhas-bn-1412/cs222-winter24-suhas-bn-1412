add_library(rm
        rm.cc
        tablesAttributeConstants.cc
        attributesAttributeConstants.cc
        catalogueConstants.cc
        catalogueConstantsBuilder.cc
        attributeAndValue.cc
        attributeAndValueSerializer.cc
        varcharSerDes.cc
)
add_dependencies(rm rbfm ix googlelog)
target_link_libraries(rm rbfm ix glog)