file(REMOVE_RECURSE
  "libadbc_driver_postgresql.pdb"
  "libadbc_driver_postgresql.so"
  "libadbc_driver_postgresql.so.8"
  "libadbc_driver_postgresql.so.8.0.0"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/adbc_driver_postgresql_shared.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
