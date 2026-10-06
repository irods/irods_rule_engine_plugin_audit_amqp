set(IRODS_TEST_TARGET test_config_load)

set(
  IRODS_TEST_SOURCE_FILES
  "${CMAKE_CURRENT_LIST_DIR}/config_load.cpp"
)

set(
  IRODS_TEST_LINK_LIBRARIES
  "-Wl,--no-as-needed"
  ${IRODS_EXTERNALS_FULLPATH_QPID_PROTON}/lib/libqpid-proton-cpp.so
  ${IRODS_EXTERNALS_FULLPATH_QPID_PROTON}/lib/libqpid-proton-core.so
  ${IRODS_EXTERNALS_FULLPATH_QPID_PROTON}/lib/libqpid-proton-proactor.so
)

set(
  IRODS_TEST_LINK_OBJLIBRARIES
  ${IRODS_PLUGIN_FULL_NAME}_obj
)

set(
  IRODS_TEST_INCLUDE_PATH
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/include"
  "${IRODS_EXTERNALS_FULLPATH_QPID_PROTON}/include"
)
