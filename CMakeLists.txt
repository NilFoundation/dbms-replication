#---------------------------------------------------------------------------#
# Copyright (c) 2018-2020 Mikhail Komarov <nemo@nil.foundation>
#
# Distributed under the Boost Software License, Version 1.0
# See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt
#---------------------------------------------------------------------------#

cmake_minimum_required(VERSION 2.8.12)

cmake_policy(SET CMP0028 NEW)
cmake_policy(SET CMP0042 NEW)
cmake_policy(SET CMP0048 NEW)
cmake_policy(SET CMP0057 NEW)
cmake_policy(SET CMP0076 NEW)

list(APPEND CMAKE_MODULE_PATH
        "${CMAKE_CURRENT_LIST_DIR}/cmake"
        "${CMAKE_CURRENT_LIST_DIR}/cmake/packages"
        "${CMAKE_CURRENT_LIST_DIR}/cmake/modules/share/modules/cmake")

include(CMConfig)
include(CMSetupVersion)

if (NOT CMAKE_WORKSPACE_NAME OR NOT ("${CMAKE_WORKSPACE_NAME}" STREQUAL "dbms"))
    cm_workspace(dbms)
endif ()

macro(cm_find_package NAME)
    foreach (ITERATOR ${CMAKE_WORKSPACE_LIST})
        if (NOT "${NAME}" MATCHES "^${ITERATOR}_.*$" AND NOT "${NAME}" STREQUAL CM)
            find_package(${ARGV})
        else ()
            set(${ARGV0}_FOUND ON CACHE BOOL "")
        endif ()
    endforeach ()
endmacro()

cm_project(replication WORKSPACE_NAME ${CMAKE_WORKSPACE_NAME} LANGUAGES ASM C CXX)

cm_find_package(CM)
include(CMDeploy)

option(BUILD_TESTS "Build unit tests" FALSE)

list(APPEND ${CURRENT_PROJECT_NAME}_PUBLIC_HEADERS)

list(APPEND ${CURRENT_PROJECT_NAME}_UNGROUPED_SOURCES)

list(APPEND ${CURRENT_PROJECT_NAME}_HEADERS ${${CURRENT_PROJECT_NAME}_PUBLIC_HEADERS})

list(APPEND ${CURRENT_PROJECT_NAME}_SOURCES ${${CURRENT_PROJECT_NAME}_UNGROUPED_SOURCES})

cm_setup_version(VERSION 0.1.0 PREFIX ${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME})

add_library(${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME} STATIC
        src/replicated_log/agency_log_specification.cpp
        src/replicated_log/agency_specification_inspectors.cpp
        src/replicated_log/algorithms.cpp
        src/replicated_log/inmemory_log.cpp
        src/replicated_log/log_common.cpp
        src/replicated_log/log_core.cpp
        src/replicated_log/log_entries.cpp
        src/replicated_log/log_interfaces.cpp
        src/replicated_log/log_status.cpp
        src/replicated_log/network_messages.cpp
        src/replicated_log/supervision_action.cpp
        src/replicated_log/wait_for_bag.cpp
        src/replicated_log/types.cpp
        src/replicated_state/agency_specification.cpp
        src/replicated_state/replicated_state_feature.cpp
        src/replicated_state/replicated_state_token.cpp
        src/replicated_state/state_common.cpp
        src/replicated_state/state_status.cpp
        src/replicated_state/update_replicated_state.cpp
        src/state_machines/black_hole/black_hole_state_machine.cpp
        src/state_machines/black_hole/black_hole_state_machine_feature.cpp
        src/state_machines/document/document_state_machine.cpp
        src/state_machines/document/document_state_machine_feature.cpp
        src/state_machines/prototype/prototype_log_entry.cpp
        src/state_machines/prototype/prototype_leader_state.cpp
        src/state_machines/prototype/prototype_core.cpp
        src/state_machines/prototype/prototype_follower_state.cpp
        src/state_machines/prototype/prototype_state_machine.cpp
        src/agency_collection_specification.cpp
        src/version.cpp)

target_include_directories(${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME} PUBLIC
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        "$<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>"

        INTERFACE ${Boost_INCLUDE_DIRS})

target_link_libraries(${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME} immer)
target_link_libraries(${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME} velocypack)
target_link_libraries(${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME} taocpp::json)
target_link_libraries(${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME} fmt::fmt)

#cm_deploy(TARGETS ${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME}
#          INCLUDE include
#          NAMESPACE ${CMAKE_WORKSPACE_NAME}::)

if (BUILD_REPLICATION_TESTS)
    add_subdirectory(test)
endif ()
