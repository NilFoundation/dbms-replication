include(CMTest)

if (Boost_FOUND AND BUILD_SHARED_LIBS)
    set(Boost_USE_STATIC_LIBS FALSE)
else ()
    set(Boost_USE_STATIC_LIBS TRUE)
endif ()

cm_test_link_libraries(${CMAKE_WORKSPACE_NAME}_${CURRENT_PROJECT_NAME}

        ${Boost_LIBRARIES})

macro(define_replication_test name)
    cm_test(NAME ${name} SOURCES ${name}.cpp)

    target_include_directories(${name} PRIVATE
            "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
            "$<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>"

            ${Boost_INCLUDE_DIRS})

    set_target_properties(${name} PROPERTIES CXX_STANDARD 14 CXX_STANDARD_REQUIRED TRUE)

    target_compile_definitions(${name} PRIVATE
            $<$<STREQUAL:$<UPPER_CASE:$<TARGET_PROPERTY:BUILD_SHARED_LIBS>>,
            TRUE>:BOOST_TEST_DYN_LINK>)
endmacro()

set(TESTS_NAMES index)

foreach (TEST_NAME ${TESTS_NAMES})
    define_replication_test(${TEST_NAME})
endforeach ()