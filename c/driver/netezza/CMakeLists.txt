# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# set(ENV{PKG_CONFIG_PATH} ${REPOSITORY_ROOT}/c/driver/netezza/)
# find_package(PkgConfig)
# pkg_check_modules(LIBPQ REQUIRED libnzpq)
# link_directories(nz_lib)
add_library(nzpq STATIC IMPORTED)
set_target_properties(nzpq PROPERTIES IMPORTED_LOCATION ${REPOSITORY_ROOT}/c/driver/netezza/nz_lib/libnzpq.a)
set_target_properties(nzpq PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${REPOSITORY_ROOT}/c/driver/netezza/nz_include)

add_arrow_lib(adbc_driver_netezza
              SOURCES
                  connection.cc
                  error.cc
                  database.cc
                  netezza.cc
                  result_helper.cc
                  statement.cc
	                postgres_copy_reader.cc

              OUTPUTS
                  ADBC_LIBRARIES
              CMAKE_PACKAGE_NAME
                  AdbcDriverNetezza
              PKG_CONFIG_NAME
                  adbc-driver-netezza
              SHARED_LINK_FLAGS
                  ${ADBC_LINK_FLAGS}
              SHARED_LINK_LIBS
                  adbc_driver_common
                  nanoarrow
                  nzpq
              STATIC_LINK_LIBS
                  adbc_driver_common
                  nanoarrow
                  # nz_lib/libnzpq.a
)
variable_watch(ADBC_LIBRARIES)
foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING)
  target_include_directories(${LIB_TARGET} SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}
                                     ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/driver/netezza/nz_include
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver)
  #target_link_libraries(${LIB_TARGET}, ${REPOSITORY_ROOT}/c/driver/netezza/nz_lib/libnzpq.a)
endforeach()


if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_netezza_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_netezza_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_netezza_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-netezza
                SOURCES
                postgres_type_test.cc
                postgres_copy_reader_test.cc
                netezza_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                nanoarrow
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-netezza-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-netezza-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}
                                     ${REPOSITORY_ROOT}/c/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver)
  adbc_configure_target(adbc-driver-netezza-test)
endif()

if(ADBC_BUILD_BENCHMARKS)
  find_package(benchmark REQUIRED)
  # TODO: should add_benchmark be linking benchmark::benchmark for us?
  add_benchmark(netezza_benchmark
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                nanoarrow
                ${TEST_LINK_LIBS}
                benchmark::benchmark)
  # add_benchmark replaces _ with - when creating target
  target_include_directories(netezza-benchmark
                             PRIVATE ${REPOSITORY_ROOT} ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver)
endif()
