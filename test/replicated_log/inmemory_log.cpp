//---------------------------------------------------------------------------//
// Copyright (c) 2018-2022 Mikhail Komarov <nemo@nil.foundation>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the Server Side Public License, version 1,
// as published by the author.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// Server Side Public License for more details.
//
// You should have received a copy of the Server Side Public License
// along with this program. If not, see
// <https://github.com/NilFoundation/dbms/blob/master/LICENSE_1_0.txt>.
//---------------------------------------------------------------------------//
#define BOOST_AUTO_TEST_MAIN

#include <boost/test/included/unit_test.hpp>
#include <boost/test/data/test_case.hpp>

#include <immer/flex_vector_transient.hpp>

#include "containers/enumerate.h"

#include <nil/dbms/replication/replicated_log/inmemory_log.hpp>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::replicated_log;

namespace std {
    template<typename T>
    std::ostream &operator<<(std::ostream &os, const std::optional<T> &opt) {
        return opt ? os << "opt.value()" : os << "std::nullopt";
    }

    template<typename T1, typename T2>
    std::ostream &operator<<(std::ostream &os, const std::unordered_map<T1, T2> &opt) {
        std::size_t i = 0;
        for (const auto &p : opt) {
            os << "element " << i << ": key: " << p.first << " value: " << p.second << std::endl;
            i++;
        }
        return os;
    }

    template<typename T1, typename T2>
    std::ostream &operator<<(std::ostream &os, const std::map<T1, T2> &opt) {
        std::size_t i = 0;
        for (const auto &p : opt) {
            os << "element " << i << ": key: " << p.first << " value: " << p.second << std::endl;
            i++;
        }
        return os;
    }

    std::ostream &operator<<(std::ostream &os, const std::nullopt_t opt) {
        return os << "std::nullopt";
    }

}    // namespace std

struct TestInMemoryLog : inmemory_log {
    explicit TestInMemoryLog(log_type log) : inmemory_log(std::move(log)) {
    }
    explicit TestInMemoryLog(log_type log, log_index first) : inmemory_log(std::move(log), first) {
    }
    TestInMemoryLog() : inmemory_log(log_type {}) {
    }
};

auto const log_ranges = {log_range(log_index {1}, log_index {15}), log_range(log_index {1}, log_index {1234}),
                         log_range(log_index {1}, log_index {1}), log_range(log_index {5}, log_index {18}),
                         log_range(log_index {76}, log_index {76})};

auto const SliceRanges = {log_range(log_index {4}, log_index {6}), log_range(log_index {1}, log_index {8}),
                          log_range(log_index {100}, log_index {120}), log_range(log_index {18}, log_index {18})};

using TermDistribution = std::map<log_term, std::size_t>;
auto Distributions = {TermDistribution {
                          {log_term {1}, 5},
                      },
                      TermDistribution {
                          {log_term {1}, 5},
                          {log_term {2}, 18},
                      },
                      TermDistribution {
                          {log_term {1}, 5},
                          {log_term {2}, 18},
                      },
                      TermDistribution {
                          {log_term {1}, 5},
                          {log_term {2}, 18},
                          {log_term {3}, 18},
                      },
                      TermDistribution {
                          {log_term {1}, 5},
                          {log_term {2}, 18},
                          {log_term {3}, 18},
                      }};

static auto createLogForRangeSingleTerm(log_range range, log_term term = log_term {1}) -> TestInMemoryLog {
    auto transient = inmemory_log::log_type::transient_type {};
    for (auto i : range) {
        transient.push_back(inmemory_log_entry({term, log_index {i}, log_payload::create_from_string("foo")}));
    }
    return TestInMemoryLog(transient.persistent(), range.from);
}

BOOST_DATA_TEST_CASE(first_last_next, log_ranges, range) {
    auto const term = log_term {1};
    auto const log = createLogForRangeSingleTerm(range, term);
    auto [from, to] = range;

    BOOST_CHECK_EQUAL(!range.empty(), log.get_first_entry().has_value());
    BOOST_CHECK_EQUAL(!range.empty(), log.get_last_entry().has_value());
    BOOST_CHECK_EQUAL(log.get_next_index(), to);

    BOOST_CHECK_EQUAL(log.get_index_range(), range);

    if (!range.empty()) {
        {
            auto memtry = log.get_first_entry().value();
            BOOST_CHECK_EQUAL(memtry.entry().logIndex(), from);
        }
        {
            auto memtry = log.get_last_entry().value();
            BOOST_CHECK_EQUAL(memtry.entry().logIndex() + 1, to);
            BOOST_CHECK_EQUAL(log.get_last_index() + 1, to);
            BOOST_CHECK_EQUAL(log.back().entry().logIndex() + 1, to);

            BOOST_CHECK_EQUAL(memtry.entry().logTerm(), term);
            BOOST_CHECK_EQUAL(log.get_last_term(), term);
            BOOST_CHECK_EQUAL(log.back().entry().logTerm(), term);
        }
    }
}

BOOST_DATA_TEST_CASE(get_entry_by_index, log_ranges, range) {
    auto const log = createLogForRangeSingleTerm(range);
    auto const tests = {log_index {1}, log_index {12}, log_index {45}};
    for (auto idx : tests) {
        auto memtry = log.get_entry_by_index(idx);
        BOOST_CHECK_EQUAL(range.contains(idx), memtry.has_value());
        if (range.contains(idx)) {
            auto entry = memtry->entry();
            BOOST_CHECK_EQUAL(entry.logIndex(), idx);
        }
    }
}

BOOST_DATA_TEST_CASE(empty, log_ranges, range) {
    auto const log = createLogForRangeSingleTerm(range);
    BOOST_CHECK_EQUAL(range.empty(), log.empty());
}

BOOST_DATA_TEST_CASE(append_in_place, log_ranges, range) {
    auto log = createLogForRangeSingleTerm(range);

    auto memtry = inmemory_log_entry({log_term {1}, range.to, log_payload::create_from_string("foo")});
    log.append_in_place(logger_context(Logger::FIXME), std::move(memtry));
    {
        auto result = log.get_entry_by_index(range.to);
        BOOST_ASSERT(result.has_value());
        BOOST_CHECK_EQUAL(result->entry().logIndex(), range.to);
    }
    {
        auto result = log.get_last_entry();
        BOOST_ASSERT(result.has_value());
        BOOST_CHECK_EQUAL(result->entry().logIndex(), range.to);
    }
}

static auto getPersistedEntriesVector(log_index first, std::size_t length, log_term term = log_term {1}) {
    auto result = inmemory_log::log_type_persisted::transient_type {};
    for (auto idx : log_range(first, first + length)) {
        result.push_back(persisting_log_entry {term, idx, log_payload::create_from_string("foo")});
    }
    return result.persistent();
}

BOOST_DATA_TEST_CASE(append_peristed_entries, boost::unit_test::data::xrange(10) * log_ranges, length, range) {
    auto const log = createLogForRangeSingleTerm(range, log_term {1});
    auto const toAppend = getPersistedEntriesVector(range.to, length, log_term {2});

    auto const newLog = log.append(logger_context(Logger::FIXME), toAppend);
    for (auto idx : log_range(range.from, range.to + length)) {
        auto memtry = newLog.get_entry_by_index(idx);
        BOOST_ASSERT(memtry.has_value());
        auto const expectedTerm = range.contains(idx) ? log_term {1} : log_term {2};
        BOOST_CHECK_EQUAL(memtry->entry().logIndex(), idx);
        BOOST_CHECK_EQUAL(memtry->entry().logTerm(), expectedTerm);
    }
}

BOOST_DATA_TEST_CASE(slice, log_ranges *SliceRanges, range, testRange) {
    auto const log = createLogForRangeSingleTerm(range);

    auto s = log.slice(testRange.from, testRange.to);
    auto const expectedRange = intersect(testRange, range);

    BOOST_CHECK_EQUAL(s.size(), expectedRange.count());
    for (auto const &[idx, e] : enumerate(s)) {
        BOOST_CHECK_EQUAL(e.entry().logIndex(), expectedRange.from + idx);
    }
}

BOOST_DATA_TEST_CASE(get_iterator_range, log_ranges *SliceRanges, range, testRange) {
    auto const log = createLogForRangeSingleTerm(range);

    auto const expectedRange = intersect(range, testRange);
    auto iter = log.get_iterator_range(testRange.from, testRange.to);
    auto [from, to] = iter->range();
    if (expectedRange.empty()) {
        BOOST_CHECK_EQUAL(from, to);

    } else {
        BOOST_CHECK_EQUAL(from, expectedRange.from);
        BOOST_CHECK_EQUAL(to, expectedRange.to);

        for (auto idx : expectedRange) {
            auto value = iter->next();
            BOOST_ASSERT(value.has_value());
            BOOST_CHECK_EQUAL(value->logIndex(), idx);
        }
    }
    BOOST_CHECK_EQUAL(iter->next(), std::nullopt);
}

BOOST_DATA_TEST_CASE(get_iterator_from, log_ranges *SliceRanges, range, testRange_input) {
    auto const log = createLogForRangeSingleTerm(range);
    auto testRange = testRange_input;
    testRange.to = range.to;

    auto const expectedRange = intersect(range, testRange);
    auto iter = log.get_iterator_from(testRange.from);

    for (auto idx : expectedRange) {
        auto value = iter->next();
        BOOST_ASSERT(value.has_value());
        BOOST_CHECK_EQUAL(value->logIndex(), idx);
    }
    BOOST_CHECK_EQUAL(iter->next(), std::nullopt);
}

BOOST_DATA_TEST_CASE(release, log_ranges *SliceRanges, range, testRange_input) {
    auto const log = createLogForRangeSingleTerm(range);
    auto testRange = testRange_input;
    testRange.to = range.to;
    auto const expectedRange = intersect(range, testRange);
    if (!expectedRange.empty()) {
        auto newLog = log.release(testRange.from);
        BOOST_CHECK_EQUAL(newLog.get_index_range(), expectedRange);
    }
}

static auto createLogForDistribution(log_index first, TermDistribution const &dist) -> TestInMemoryLog {
    auto transient = inmemory_log::log_type::transient_type {};
    auto next = first;
    for (auto [term, length] : dist) {
        for (auto idx : log_range(next, next + length)) {
            transient.push_back(inmemory_log_entry({term, idx, log_payload::create_from_string("foo")}));
        }
        next = next + length;
    }
    return TestInMemoryLog(transient.persistent());
}

static auto getTermBounds(log_index first, TermDistribution const &dist, log_term wanted) -> std::optional<log_range> {
    auto next = first;
    for (auto [term, length] : dist) {
        if (term == wanted) {
            return log_range {next, next + length};
        }
        next = next + length;
    }

    return std::nullopt;
}

auto range1 = {log_term {1}, log_term {2}, log_term {3}};
auto range2 = {log_index {1}, log_index {10}};
BOOST_DATA_TEST_CASE(first_index_of_term, range1 *range2 *Distributions, term, first, dist) {
    auto log = createLogForDistribution(first, dist);

    auto range = getTermBounds(first, dist, term);
    auto firstInTerm = log.get_first_index_of_term(term);
    auto lastInTerm = log.get_last_index_of_term(term);

    BOOST_CHECK_EQUAL(range.has_value(), firstInTerm.has_value());
    BOOST_CHECK_EQUAL(range.has_value(), lastInTerm.has_value());

    if (range.has_value()) {
        BOOST_CHECK_EQUAL(range->from, *firstInTerm);
        BOOST_CHECK_EQUAL(range->to, *lastInTerm + 1);
    }
}