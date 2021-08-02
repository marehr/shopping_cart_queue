cmake_minimum_required (VERSION 3.8)

add_api_test (convert_fastq_test.cpp)
target_use_datasources (convert_fastq_test FILES in.fastq)

# workaround for app-template
add_library ("${PROJECT_NAME}_lib" INTERFACE)
target_include_directories ("${PROJECT_NAME}_lib" INTERFACE "${CMAKE_SOURCE_DIR}/include")

add_subdirectory (scq)
