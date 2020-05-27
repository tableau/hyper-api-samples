# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# Unlicensed use of the contents of this file is prohibited. Please refer to
# the NOTICES.txt file for further details.
#
# -----------------------------------------------------------------------------

# This is an example CMake project that builds and tests the included C++ example code.
#
# To configure this project, run
#
#    cmake <optional CMake arguments> -S "<directory of this file>"
#
# You can optionally override the directory where the Tableau Hyper API for C++ is searched by specifying
#
#       -D CMAKE_PREFIX_PATH:PATH="<directory of Tableau Hyper API for C++>/share/cmake"
#
# By default, the `hyperd` executable is searched in the `bin/hyper/` directory of the `tableauhyperapi-cxx` package.
# You can override the directory by specifying
#
#    -D HYPER_PATH:PATH="<directory of hyperd>"
#
# To build and test the examples, run
#
#    cmake --build .
#    ctest

cmake_minimum_required(VERSION 3.1)

# If CMAKE_PREFIX_PATH is not specified, provide a convenient default, so that the `find_package()` below can find
# the Hyper API. This default assumes this `CMakeLists.txt` to be in its original location under `examples/`.
if (NOT DEFINED CMAKE_PREFIX_PATH)
    get_filename_component(HAPI_PACKAGE_LOCATION "${CMAKE_SOURCE_DIR}/.." REALPATH)
    set(CMAKE_PREFIX_PATH "${HAPI_PACKAGE_LOCATION}/share/cmake" CACHE PATH "CMake prefix path" FORCE)
endif ()

project("Tableau Hyper API for C++ Examples" LANGUAGES CXX)

find_package(tableauhyperapi-cxx REQUIRED CONFIG)

# Determine some directories in the `tableauhyperapi-c` package for use below.
get_filename_component(tableauhyperapi-c_BINARY_DIR "${tableauhyperapi-c_DIR}/../../bin" ABSOLUTE)
get_filename_component(tableauhyperapi-c_LIBRARY_DIR "${tableauhyperapi-c_DIR}/../../lib" ABSOLUTE)
if (WIN32)
    set(tableauhyperapi-c_DYLIB_DIR "${tableauhyperapi-c_BINARY_DIR}")
else ()
    set(tableauhyperapi-c_DYLIB_DIR "${tableauhyperapi-c_LIBRARY_DIR}")
endif ()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

enable_testing()

# Copy over the superstore CSV dataset.
file(COPY "${CMAKE_SOURCE_DIR}/data/sample_extracts/" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/data")
file(COPY "${CMAKE_SOURCE_DIR}/data/superstore_normalized/" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/data")
if (WIN32)
    # On Windows, the Hyper API is copied into the binary directory so the examples can pick it up during execution.
    # On Posix systems, the Hyper API path is already compiled into the RPATH of the examples.
    file(COPY "${tableauhyperapi-c_DYLIB_DIR}/" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}")
endif ()

# -----------------------------------------------------------------------------
# `create_hyper_file_from_csv.cpp`

add_executable(create_hyper_file_from_csv create_hyper_file_from_csv.cpp)
target_link_libraries(create_hyper_file_from_csv PRIVATE Tableau::tableauhyperapi-cxx)
add_test(
        NAME create_hyper_file_from_csv
        COMMAND ${CMAKE_COMMAND} -E env HYPER_PATH=${HYPER_PATH} $<TARGET_FILE:create_hyper_file_from_csv>
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# -----------------------------------------------------------------------------
# `delete_data_in_existing_hyper_file.cpp`

add_executable(delete_data_in_existing_hyper_file delete_data_in_existing_hyper_file.cpp)
target_link_libraries(delete_data_in_existing_hyper_file PRIVATE Tableau::tableauhyperapi-cxx)
add_test(
        NAME delete_data_in_existing_hyper_file
        COMMAND ${CMAKE_COMMAND} -E env HYPER_PATH=${HYPER_PATH} $<TARGET_FILE:delete_data_in_existing_hyper_file>
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# -----------------------------------------------------------------------------
# `insert_data_into_multiple_tables.cpp`

add_executable(insert_data_into_multiple_tables insert_data_into_multiple_tables.cpp)
target_link_libraries(insert_data_into_multiple_tables PRIVATE Tableau::tableauhyperapi-cxx)
add_test(
        NAME insert_data_into_multiple_tables
        COMMAND ${CMAKE_COMMAND} -E env HYPER_PATH=${HYPER_PATH} $<TARGET_FILE:insert_data_into_multiple_tables>
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# -----------------------------------------------------------------------------
# `insert_data_into_single_table.cpp`

add_executable(insert_data_into_single_table insert_data_into_single_table.cpp)
target_link_libraries(insert_data_into_single_table PRIVATE Tableau::tableauhyperapi-cxx)
add_test(
        NAME insert_data_into_single_table
        COMMAND ${CMAKE_COMMAND} -E env HYPER_PATH=${HYPER_PATH} $<TARGET_FILE:insert_data_into_single_table>
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# -----------------------------------------------------------------------------
# `insert_data_into_single_table.cpp`

add_executable(insert_data_with_expressions insert_data_with_expressions.cpp)
target_link_libraries(insert_data_with_expressions PRIVATE Tableau::tableauhyperapi-cxx)
add_test(
        NAME insert_data_with_expressions
        COMMAND ${CMAKE_COMMAND} -E env HYPER_PATH=${HYPER_PATH} $<TARGET_FILE:insert_data_with_expressions>
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# -----------------------------------------------------------------------------
# `insert_spatial_data_to_a_hyper_file.cpp`

add_executable(insert_spatial_data_to_a_hyper_file insert_spatial_data_to_a_hyper_file.cpp)
target_link_libraries(insert_spatial_data_to_a_hyper_file PRIVATE Tableau::tableauhyperapi-cxx)
add_test(
        NAME insert_spatial_data_to_a_hyper_file
        COMMAND ${CMAKE_COMMAND} -E env HYPER_PATH=${HYPER_PATH} $<TARGET_FILE:insert_spatial_data_to_a_hyper_file>
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# -----------------------------------------------------------------------------
# `read_and_print_data_from_existing_hyper_file.cpp`

add_executable(read_and_print_data_from_existing_hyper_file read_and_print_data_from_existing_hyper_file.cpp)
target_link_libraries(read_and_print_data_from_existing_hyper_file PRIVATE Tableau::tableauhyperapi-cxx)
add_test(
        NAME read_and_print_data_from_existing_hyper_file
        COMMAND ${CMAKE_COMMAND} -E env HYPER_PATH=${HYPER_PATH} $<TARGET_FILE:read_and_print_data_from_existing_hyper_file>
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# -----------------------------------------------------------------------------
# `update_data_in_existing_hyper_file.cpp`

add_executable(update_data_in_existing_hyper_file update_data_in_existing_hyper_file.cpp)
target_link_libraries(update_data_in_existing_hyper_file PRIVATE Tableau::tableauhyperapi-cxx)
add_test(
        NAME update_data_in_existing_hyper_file
        COMMAND ${CMAKE_COMMAND} -E env HYPER_PATH=${HYPER_PATH} $<TARGET_FILE:update_data_in_existing_hyper_file>
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

