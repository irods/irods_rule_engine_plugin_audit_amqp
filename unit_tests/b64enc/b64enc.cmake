set(IRODS_TEST_TARGET test_b64enc)

set(
  IRODS_TEST_SOURCE_FILES
  "${CMAKE_CURRENT_LIST_DIR}/b64enc.cpp"
)

set(
  IRODS_TEST_LINK_LIBRARIES
  nlohmann_json::nlohmann_json
  )

set(
  IRODS_TEST_INCLUDE_PATH
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/include"
)

