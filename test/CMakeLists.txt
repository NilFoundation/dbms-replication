include(CMTest)

if (NOT Boost_UNIT_TEST_FRAMEWORK_FOUND)
    find_package(Boost REQUIRED COMPONENTS unit_test_framework)
endif ()


cm_test_link_libraries(${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME}
        dbms_replication
        dbms_replication2

        ${Boost_LIBRARIES})

set(CURRENT_PROJECT_NAME replication)

macro(define_replication_test source_file)
    get_filename_component(test_name ${source_file} NAME)
    set(target_name ${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME}_${test_name}_test)

    cm_test(NAME ${target_name} SOURCES ${source_file}.cpp)

    target_include_directories(${target_name} PRIVATE
            "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
            "$<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>"

            ${Boost_INCLUDE_DIRS})

    target_sources(${target_name} PRIVATE
            mocks/fake_failure_oracle.cpp
            mocks/fake_replicated_log.cpp
            mocks/mock_state_persistor_interface.cpp
            mocks/replicated_log_metrics_mock.cpp
            mocks/persisted_log.cpp
            replicated_log/test_helper.cpp
            )

    set_target_properties(${target_name} PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED TRUE)

    target_compile_definitions(${target_name} PRIVATE
            $<$<STREQUAL:$<UPPER_CASE:$<TARGET_PROPERTY:BUILD_SHARED_LIBS>>,
            TRUE>:BOOST_TEST_DYN_LINK>)
endmacro()

set(TESTS_NAMES
        replicated_state/prototype_state_machine
        replicated_log/inmemory_log)

foreach (TEST_NAME ${TESTS_NAMES})
    define_replication_test(${TEST_NAME})
endforeach ()