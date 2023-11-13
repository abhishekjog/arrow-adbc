file(REMOVE_RECURSE
  "libadbc_driver_postgresql.a"
  "libadbc_driver_postgresql.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/adbc_driver_postgresql_static.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
