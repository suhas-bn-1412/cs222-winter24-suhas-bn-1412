add_library(ix ix.cc
        nonLeafPage.cc
        leafPage.cc
        pageSerializer.cc
        pageDeserializer.cc
        pageSerDesConstants.cc
        varcharSerDes.cc
)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)