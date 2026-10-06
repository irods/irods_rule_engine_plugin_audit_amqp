cmake_minimum_required(VERSION 3.12...3.18 FATAL_ERROR)
if (POLICY CMP0109)
  # find_program requires permission to execute but not to read
  cmake_policy(SET CMP0109 NEW)
endif()
if (POLICY CMP0121)
  # Detect invalid indices in list()
  cmake_policy(SET CMP0121 NEW)
endif()
if (POLICY CMP0125)
  # Consistent behavior for cache variables managed by find_*()
  cmake_policy(SET CMP0125 NEW)
endif()
if (POLICY CMP0130)
  # Diagnose condition evaluation errors in while()
  cmake_policy(SET CMP0130 NEW)
endif()
if (POLICY CMP0132)
  # Consistent handling of compiler environment variables
  cmake_policy(SET CMP0132 NEW)
endif()
if (POLICY CMP0140)
  # Check validity of return() params
  cmake_policy(SET CMP0140 NEW)
endif()

set(IRODS_MINIMUM_VERSION "4.90.0")
set(IRODS_MAXIMUM_VERSION "6.0.0")
find_package(IRODS "${IRODS_MINIMUM_VERSION}...<${IRODS_MAXIMUM_VERSION}" REQUIRED)

set(IRODS_PLUGIN_VERSION "5.1.0")

set(IRODS_PACKAGE_REVISION "0")

set(IRODS_RULE_ENGINE_PLUGIN_NAME "audit_amqp")
set(IRODS_PLUGIN_FULL_NAME "irods_rule_engine_plugin-${IRODS_RULE_ENGINE_PLUGIN_NAME}")
set(PLUGIN irods_rule_engine_plugin-audit_amqp)

include(IrodsCXXCompiler)
set(CMAKE_CXX_STANDARD ${IRODS_CXX_STANDARD})
set(CMAKE_MODULE_LINKER_FLAGS_INIT "-Wl,--enable-new-dtags -Wl,--as-needed -Wl,-z,defs")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-Wl,--enable-new-dtags -Wl,--as-needed -Wl,-z,defs")
set(CMAKE_MODULE_LINKER_FLAGS_RELEASE_INIT "-Wl,--gc-sections -Wl,-z,combreloc")
set(CMAKE_SHARED_LINKER_FLAGS_RELEASE_INIT "-Wl,--gc-sections -Wl,-z,combreloc")
include(IrodsRunpathDefaults)

include(IrodsExternals)

IRODS_MACRO_CHECK_DEPENDENCY_SET_FULLPATH_ADD_TO_IRODS_PACKAGE_DEPENDENCIES_LIST(QPID_PROTON qpid-proton0.36.0-3)

string(REPLACE ";" ", " ${IRODS_PLUGIN_FULL_NAME}_PACKAGE_DEPENDENCIES_STRING "${IRODS_PACKAGE_DEPENDENCIES_LIST}")
unset(IRODS_PACKAGE_DEPENDENCIES_LIST)

project("${IRODS_PLUGIN_FULL_NAME}"
  VERSION "${IRODS_PLUGIN_VERSION}"
  LANGUAGES CXX)

include(${IRODS_TARGETS_PATH})

if (NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build {Debug, Release}." FORCE)
  message(STATUS "Setting unspecified CMAKE_BUILD_TYPE to '${CMAKE_BUILD_TYPE}'")
endif()

include(GNUInstallDirs)
include(CheckCXXSymbolExists)
include(ObjectTargetHelpers)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto SSL)
find_package(nlohmann_json "3.6.1" REQUIRED)
find_package(fmt "8.1.1" REQUIRED)

set(
  ${IRODS_PLUGIN_FULL_NAME}_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/amqp_config.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/amqp_sender.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/audit_config.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/proton_error_event.cpp"
)
set(
  ${IRODS_PLUGIN_FULL_NAME}_HEADERS
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/amqp_config.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/amqp_sender.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/audit_amqp.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/audit_config.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/audit_b64enc.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/proton_error_event.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/proton_formatters.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/proton_object_type.hpp"
)
set(
  ${IRODS_PLUGIN_FULL_NAME}_HEADERS_GENERATORS
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/audit_amqp_version.hpp.in"
)
set(
  ${IRODS_PLUGIN_FULL_NAME}_HEADERS_GENERATED
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/audit_amqp_version.hpp"
)

set_source_files_properties(
  "${${IRODS_PLUGIN_FULL_NAME}_HEADERS_GENERATORS}"
  PROPERTIES
  HEADER_FILE_ONLY ON
)
set_source_files_properties(
  "${${IRODS_PLUGIN_FULL_NAME}_HEADERS_GENERATED}"
  PROPERTIES
  GENERATED ON
)

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/private/audit_amqp_version.hpp.in"
  "${CMAKE_CURRENT_BINARY_DIR}/include/irods/private/audit_amqp_version.hpp"
  @ONLY
)

add_library(
  ${IRODS_PLUGIN_FULL_NAME}_obj
  OBJECT
  "${${IRODS_PLUGIN_FULL_NAME}_SOURCES}"
  "${${IRODS_PLUGIN_FULL_NAME}_HEADERS}"
  "${${IRODS_PLUGIN_FULL_NAME}_HEADERS_GENERATORS}"
  "${${IRODS_PLUGIN_FULL_NAME}_HEADERS_GENERATED}"
)
target_compile_options(${IRODS_PLUGIN_FULL_NAME}_obj PRIVATE -Wno-write-strings)
target_compile_definitions(${IRODS_PLUGIN_FULL_NAME}_obj PRIVATE ${IRODS_COMPILE_DEFINITIONS} ${IRODS_COMPILE_DEFINITIONS_PRIVATE} IRODS_ENABLE_SYSLOG)
target_include_directories(
  ${IRODS_PLUGIN_FULL_NAME}_obj
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
  PRIVATE
  ${IRODS_INCLUDE_DIRS}
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
  ${IRODS_EXTERNALS_FULLPATH_QPID_PROTON}/include
  )

target_link_libraries(
  ${IRODS_PLUGIN_FULL_NAME}_obj
  PUBLIC
  irods_server
  irods_common
  nlohmann_json::nlohmann_json
  fmt::fmt
  PRIVATE
  ${IRODS_EXTERNALS_FULLPATH_QPID_PROTON}/lib/libqpid-proton-cpp.so
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_url.so
  ${CMAKE_DL_LIBS}
  )

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_definitions(${IRODS_PLUGIN_FULL_NAME}_obj PRIVATE IRODS_AUDIT_EXTRA_TRACE)
endif()

check_cxx_symbol_exists(__cpp_lib_filesystem "version" HAVE_STD_FILESYSTEM)
if (NOT HAVE_STD_FILESYSTEM)
  target_link_libraries(
    ${IRODS_PLUGIN_FULL_NAME}_obj
    PRIVATE
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
    )
endif()

add_library(${IRODS_PLUGIN_FULL_NAME} MODULE)
target_link_objects(
	${IRODS_PLUGIN_FULL_NAME}
	PRIVATE
	${IRODS_PLUGIN_FULL_NAME}_obj
)

install(TARGETS ${IRODS_PLUGIN_FULL_NAME} LIBRARY DESTINATION ${IRODS_PLUGINS_DIRECTORY}/rule_engines)

install(
  FILES
  ${CMAKE_SOURCE_DIR}/packaging/test_audit_plugin.py
  ${CMAKE_SOURCE_DIR}/packaging/audit_plugin_message_worker.py
  ${CMAKE_SOURCE_DIR}/packaging/message_broker_consumer.py
  ${CMAKE_SOURCE_DIR}/packaging/queue_listener.py
  DESTINATION ${IRODS_HOME_DIRECTORY}/scripts/irods/test
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

install(
  FILES ${CMAKE_SOURCE_DIR}/scripts/add_audit_rule_engine_to_rule_engines.py
  DESTINATION ${IRODS_HOME_DIRECTORY}/scripts
  )

add_subdirectory(unit_tests)

set(PLUGIN_PACKAGE_NAME irods-rule-engine-plugin-audit-amqp)

include(IrodsCPackCommon)

list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}/scripts")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}/scripts/irods")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_PACKAGING_INSTALL_PREFIX}${IRODS_HOME_DIRECTORY}/scripts/irods/test")

set(CPACK_PACKAGE_VERSION ${IRODS_PLUGIN_VERSION})
set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_COMPONENT_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_COMPONENTS_GROUPING IGNORE)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "The integrated Rule-Oriented Data System")

set(CPACK_DEB_COMPONENT_INSTALL OFF)
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS OFF)
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION ON)
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_SOURCE_DIR}/packaging/postinst;")
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)

set(CPACK_RPM_COMPONENT_INSTALL OFF)
set(CPACK_RPM_PACKAGE_LICENSE "BSD-3-Clause")
set(CPACK_RPM_PACKAGE_AUTOREQ 0)
set(CPACK_RPM_PACKAGE_AUTOPROV 0)
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_SOURCE_DIR}/packaging/postinst")
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)

set(CPACK_ARCHIVE_COMPONENT_INSTALL OFF)

set(CPACK_DEBIAN_PACKAGE_NAME ${PLUGIN_PACKAGE_NAME})
set(CPACK_DEBIAN_PACKAGE_DEPENDS "${IRODS_PACKAGE_DEPENDENCIES_STRING}, ${${IRODS_PLUGIN_FULL_NAME}_PACKAGE_DEPENDENCIES_STRING}, irods-runtime (= ${IRODS_VERSION})")

set(CPACK_RPM_PACKAGE_NAME ${PLUGIN_PACKAGE_NAME})
set(CPACK_RPM_PACKAGE_REQUIRES "${IRODS_PACKAGE_DEPENDENCIES_STRING}, ${${IRODS_PLUGIN_FULL_NAME}_PACKAGE_DEPENDENCIES_STRING}, irods-runtime = ${IRODS_VERSION}")


if (NOT CPACK_GENERATOR)
  set(CPACK_GENERATOR ${IRODS_CPACK_GENERATOR} CACHE STRING "CPack generator to use, e.g. {DEB, RPM, TGZ}." FORCE)
  message(STATUS "Setting unspecified CPACK_GENERATOR to ${CPACK_GENERATOR}. This is the correct setting for normal builds.")
endif()

include(CPack)
