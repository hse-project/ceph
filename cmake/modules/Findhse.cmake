# find libhse_kvdb
# Once done, this will define
#
# HSE_FOUND - system has libhse_kvdb
# HSE_INCLUDE_DIR - the libhse_kvdb include directories
# HSE_LIBRARIES - link these to use libhse_kvdb

# find_package(PkgConfig)
# if(PKG_CONFIG_FOUND)
  # pkg_check_modules(HSE REQUIRED QUIET hse-1>=1.10.0)
# else()
  # find_path(HSE_INCLUDE_DIR hse/hse.h
    # PATH_SUFFIXES hse-1
    # HINTS
      # ENV HSE_INCLUDE_DIR)
  # find_library(HSE_LIBRARIES
    # NAMES hse-1
      # HINTS
        # ENV HSE_LIB_DIR)
# endif()

find_path(HSE_INCLUDE_DIR hse/hse.h
  HINTS
    ENV HSE_INCLUDE_DIR)
find_library(HSE_LIBRARIES
  NAMES hse_kvdb
    HINTS
      ENV HSE_LIB_DIR)

find_package_handle_standard_args(HSE
  FOUND_VAR HSE_FOUND
  REQUIRED_VARS HSE_LIBRARIES HSE_INCLUDE_DIR)
