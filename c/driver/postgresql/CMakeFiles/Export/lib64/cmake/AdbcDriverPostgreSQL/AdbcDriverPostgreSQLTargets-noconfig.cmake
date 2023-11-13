#----------------------------------------------------------------
# Generated CMake target import file.
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "AdbcDriverPostgreSQL::adbc_driver_postgresql_shared" for configuration ""
set_property(TARGET AdbcDriverPostgreSQL::adbc_driver_postgresql_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS NOCONFIG)
set_target_properties(AdbcDriverPostgreSQL::adbc_driver_postgresql_shared PROPERTIES
  IMPORTED_LOCATION_NOCONFIG "${_IMPORT_PREFIX}/lib64/libadbc_driver_postgresql.so.8.0.0"
  IMPORTED_SONAME_NOCONFIG "libadbc_driver_postgresql.so.8"
  )

list(APPEND _IMPORT_CHECK_TARGETS AdbcDriverPostgreSQL::adbc_driver_postgresql_shared )
list(APPEND _IMPORT_CHECK_FILES_FOR_AdbcDriverPostgreSQL::adbc_driver_postgresql_shared "${_IMPORT_PREFIX}/lib64/libadbc_driver_postgresql.so.8.0.0" )

# Import target "AdbcDriverPostgreSQL::adbc_driver_postgresql_static" for configuration ""
set_property(TARGET AdbcDriverPostgreSQL::adbc_driver_postgresql_static APPEND PROPERTY IMPORTED_CONFIGURATIONS NOCONFIG)
set_target_properties(AdbcDriverPostgreSQL::adbc_driver_postgresql_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_NOCONFIG "CXX"
  IMPORTED_LOCATION_NOCONFIG "${_IMPORT_PREFIX}/lib64/libadbc_driver_postgresql.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS AdbcDriverPostgreSQL::adbc_driver_postgresql_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_AdbcDriverPostgreSQL::adbc_driver_postgresql_static "${_IMPORT_PREFIX}/lib64/libadbc_driver_postgresql.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
