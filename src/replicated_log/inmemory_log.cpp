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

#include <nil/dbms/replication/logger_context.hpp>
#include <nil/dbms/replication/replicated_log/inmemory_log.hpp>
#include <nil/dbms/replication/replicated_log/log_core.hpp>
#include <nil/dbms/replication/replicated_log/persisted_log.hpp>
#include <nil/dbms/replication/replicated_log/replicated_log_iterator.hpp>

#include <basics/exceptions.h>
#include <basics/string_utils.h>
#include <basics/application_exit.h>
#include <basics/debugging.h>
#include <containers/immer_memory_policy.h>

#include <iterator>

#if (_MSC_VER >= 1)
// suppress warnings:
#pragma warning(push)
// conversion from 'size_t' to 'immer::detail::rbts::count_t', possible loss of
// data
#pragma warning(disable : 4267)
// result of 32-bit shift implicitly converted to 64 bits (was 64-bit shift
// intended?)
#pragma warning(disable : 4334)
#endif
#include <immer/flex_vector.hpp>
#include <immer/flex_vector_transient.hpp>
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

using namespace nil::dbms;
using namespace nil::dbms::replication;

auto replicated_log::inmemory_log::get_last_index() const noexcept -> log_index {
    return get_last_term_index_pair().index;
}

auto replicated_log::inmemory_log::get_last_term_index_pair() const noexcept -> term_index_pair {
    if (_log.empty()) {
        return {};
    }
    return _log.back().entry().logTermIndexPair();
}

auto replicated_log::inmemory_log::get_last_term() const noexcept -> log_term {
    return get_last_term_index_pair().term;
}

auto replicated_log::inmemory_log::get_next_index() const noexcept -> log_index {
    return _first + _log.size();
}

auto replicated_log::inmemory_log::get_entry_by_index(log_index idx) const noexcept
    -> std::optional<inmemory_log_entry> {
    if (_first + _log.size() <= idx || idx < _first) {
        return std::nullopt;
    }

    auto const &e = _log.at(idx.value - _first.value);
    ADB_PROD_ASSERT(e.entry().logIndex() == idx) << "idx = " << idx << ", entry = " << e.entry().logIndex();
    return e;
}

auto replicated_log::inmemory_log::slice(log_index from, log_index to) const -> log_type {
    from = std::max(from, _first);
    to = std::max(to, _first);
    ADB_PROD_ASSERT(from <= to) << "from = " << from << ", to = " << to << ", _first = " << _first;
    auto res = _log.take(to.value - _first.value).drop(from.value - _first.value);
    ADB_PROD_ASSERT(res.size() <= to.value - from.value) << "res.size() = " << res.size() << ", to = " << to.value
                                                         << ", from = " << from.value << ", first = " << _first;
    return res;
}

auto replicated_log::inmemory_log::get_first_index_of_term(log_term term) const noexcept -> std::optional<log_index> {
    auto it = std::lower_bound(_log.begin(), _log.end(), term,
                               [](auto const &entry, auto const &term) { return term > entry.entry().logTerm(); });

    if (it != _log.end() && it->entry().logTerm() == term) {
        return it->entry().logIndex();
    } else {
        return std::nullopt;
    }
}

auto replicated_log::inmemory_log::get_last_index_of_term(log_term term) const noexcept -> std::optional<log_index> {
    // Note that we're using reverse iterators
    auto it = std::lower_bound(_log.rbegin(), _log.rend(), term, [](auto const &entry, auto const &term) {
        // Note that this is flipped
        return entry.entry().logTerm() > term;
    });

    // Using `.base()` here is required, because immer's reverse iterators
    // are not properly C++20--compatible. Otherwise we get:
    // '__x.base() != __y.base()' would be invalid: ISO C++20 considers use of overloaded operator '!='
    if (it.base() != _log.rend().base() && it->entry().logTerm() == term) {
        return it->entry().logIndex();
    } else {
        return std::nullopt;
    }
}

auto replicated_log::inmemory_log::release(log_index stop) const -> replicated_log::inmemory_log {
    auto [from, to] = get_index_range();
    auto newLog = slice(stop, to);
    return inmemory_log(newLog);
}

replicated_log::inmemory_log::inmemory_log(log_type log) :
    _log(std::move(log)), _first(_log.empty() ? log_index {1} : _log.front().entry().logIndex()) {
}

replicated_log::inmemory_log::inmemory_log(log_type log, log_index first) : _log(std::move(log)), _first(first) {
    TRI_ASSERT(_log.empty() || first == _log.front().entry().logIndex())
        << " log.empty = " << std::boolalpha << _log.empty() << " first = " << first
        << " log.front.idx = " << (!_log.empty() ? _log.front().entry().logIndex().value : 0);
}

#if (_MSC_VER >= 1)
// suppress false positive warning:
#pragma warning(push)
// function assumed not to throw an exception but does
#pragma warning(disable : 4297)
#endif
replicated_log::inmemory_log::inmemory_log(replicated_log::inmemory_log &&other) noexcept try :
    _log(std::move(other._log)), _first(other._first) {
    other._first = log_index {1};
    // Note that immer::flex_vector is currently not nothrow move-assignable,
    // though it probably does not throw any exceptions. However, we *need* this
    // to be noexcept, otherwise we cannot keep the persistent and in-memory state
    // in sync.
    //
    // So even if flex_vector could actually throw here, we deliberately add the
    // noexcept!
    //
    // The try/catch is *only* for logging, but *must* terminate (e.g. by
    // rethrowing) the process if an exception is caught.
} catch (std::exception const &ex) {
    LOG_TOPIC("33563", FATAL, Logger::REPLICATION2)
        << "Caught an exception when moving an InMemoryLog. This is fatal, as "
           "consistency of persistent and in-memory state can no longer be "
           "guaranteed. The process will terminate now. The exception was: "
        << ex.what();
    FATAL_ERROR_ABORT();
} catch (...) {
    LOG_TOPIC("9771c", FATAL, Logger::REPLICATION2)
        << "Caught an exception when moving an InMemoryLog. This is fatal, as "
           "consistency of persistent and in-memory state can no longer be "
           "guaranteed. The process will terminate now.";
    FATAL_ERROR_ABORT();
}
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

auto replicated_log::inmemory_log::operator=(replicated_log::inmemory_log &&other) noexcept
    -> replicated_log::inmemory_log &try {
    // Note that immer::flex_vector is currently not nothrow move-assignable,
    // though it probably does not throw any exceptions. However, we *need* this
    // to be noexcept, otherwise we cannot keep the persistent and in-memory state
    // in sync.
    //
    // So even if flex_vector could actually throw here, we deliberately add the
    // noexcept!
    //
    // The try/catch is *only* for logging, but *must* terminate (e.g. by
    // rethrowing) the process if an exception is caught.
    _log = std::move(other._log);
    _first = other._first;
    other._first = log_index {1};
    return *this;
} catch (std::exception const &ex) {
    LOG_TOPIC("bf5c5", FATAL, Logger::REPLICATION2)
        << "Caught an exception when moving an InMemoryLog. This is fatal, as "
           "consistency of persistent and in-memory state can no longer be "
           "guaranteed. The process will terminate now. The exception was: "
        << ex.what();
    FATAL_ERROR_ABORT();
} catch (...) {
    LOG_TOPIC("2c084", FATAL, Logger::REPLICATION2)
        << "Caught an exception when moving an InMemoryLog. This is fatal, as "
           "consistency of persistent and in-memory state can no longer be "
           "guaranteed. The process will terminate now.";
    FATAL_ERROR_ABORT();
}

auto replicated_log::inmemory_log::get_iterator_from(log_index fromIdx) const -> std::unique_ptr<LogIterator> {
    // if we want to have read from log entry 1 onwards, we have to drop
    // no entries, because log entry 0 does not exist.
    auto log = _log.drop(fromIdx.saturated_decrement(_first.value).value);
    return std::make_unique<replicated_log_iterator>(std::move(log));
}

auto replicated_log::inmemory_log::get_memtry_iterator_from(log_index fromIdx) const
    -> std::unique_ptr<typed_log_iterator<inmemory_log_entry>> {
    // if we want to have read from log entry 1 onwards, we have to drop
    // no entries, because log entry 0 does not exist.
    auto log = _log.drop(fromIdx.saturated_decrement(_first.value).value);
    return std::make_unique<in_memory_log_iterator>(std::move(log));
}

auto replicated_log::inmemory_log::get_memtry_iterator_range(log_index fromIdx, log_index toIdx) const
    -> std::unique_ptr<typed_log_iterator<inmemory_log_entry>> {
    auto log =
        _log.take(toIdx.saturated_decrement(_first.value).value).drop(fromIdx.saturated_decrement(_first.value).value);
    return std::make_unique<in_memory_log_iterator>(std::move(log));
}

auto replicated_log::inmemory_log::get_internal_iterator_from(log_index fromIdx) const
    -> std::unique_ptr<persisted_log_iterator> {
    // if we want to have read from log entry 1 onwards, we have to drop
    // no entries, because log entry 0 does not exist.
    auto log = _log.drop(fromIdx.saturated_decrement(_first.value).value);
    return std::make_unique<in_memory_persisted_log_iterator>(std::move(log));
}

auto replicated_log::inmemory_log::get_internal_iterator_range(log_index fromIdx, log_index toIdx) const
    -> std::unique_ptr<persisted_log_iterator> {
    auto log =
        _log.take(toIdx.saturated_decrement(_first.value).value).drop(fromIdx.saturated_decrement(_first.value).value);
    return std::make_unique<in_memory_persisted_log_iterator>(std::move(log));
}

auto replicated_log::inmemory_log::get_iterator_range(log_index fromIdx, log_index toIdx) const
    -> std::unique_ptr<LogRangeIterator> {
    auto log =
        _log.take(toIdx.saturated_decrement(_first.value).value).drop(fromIdx.saturated_decrement(_first.value).value);
    return std::make_unique<replicated_log_iterator>(std::move(log));
}

void replicated_log::inmemory_log::append_in_place(logger_context const &logContext, inmemory_log_entry entry) {
    if (get_next_index() != entry.entry().logIndex()) {
        using namespace basics::StringUtils;
        auto message = concatT(
            "Trying to append a log entry with "
            "mismatching log index. Last log index is ",
            get_last_index(), ", but the new entry has ", entry.entry().logIndex());
        LOG_CTX("e2775", ERR, logContext) << message;
        basics::abortOrThrow(TRI_ERROR_INTERNAL, std::move(message), ADB_HERE);
    }
    _log = _log.push_back(std::move(entry));
}

auto replicated_log::inmemory_log::append(logger_context const &logContext, log_type entries) const -> inmemory_log {
    ADB_PROD_ASSERT(entries.empty() || get_next_index() == entries.front().entry().logIndex())
        << std::boolalpha << "entries.empty() = " << entries.empty()
        << ", front = " << entries.front().entry().logIndex() << ", get_next_index = " << get_next_index();
    auto transient = _log.transient();
    transient.append(std::move(entries).transient());
    return inmemory_log {std::move(transient).persistent(), _first};
}

auto replicated_log::inmemory_log::append(logger_context const &logContext, log_type_persisted const &entries) const
    -> inmemory_log {
    ADB_PROD_ASSERT(entries.empty() || get_next_index() == entries.front().logIndex())
        << std::boolalpha << "entries.empty() = " << entries.empty() << ", front = " << entries.front().logIndex()
        << ", get_next_index = " << get_next_index();
    auto transient = _log.transient();
    for (auto const &entry : entries) {
        transient.push_back(inmemory_log_entry(entry));
    }
    return inmemory_log {std::move(transient).persistent(), _first};
}

auto replicated_log::inmemory_log::take_snapshot_up_to_and_including(log_index until) const -> inmemory_log {
    ADB_PROD_ASSERT(_first <= (until + 1)) << "first = " << _first << " until = " << until;
    return inmemory_log {_log.take(until.value - _first.value + 1), _first};
}

auto replicated_log::inmemory_log::copy_flex_vector() const -> log_type {
    return _log;
}

auto replicated_log::inmemory_log::back() const noexcept -> log_type::const_reference {
    return _log.back();
}

auto replicated_log::inmemory_log::empty() const noexcept -> bool {
    return _log.empty();
}

auto replicated_log::inmemory_log::get_last_entry() const noexcept -> std::optional<inmemory_log_entry> {
    if (_log.empty()) {
        return std::nullopt;
    }
    return _log.back();
}

auto replicated_log::inmemory_log::get_first_entry() const noexcept -> std::optional<inmemory_log_entry> {
    if (_log.empty()) {
        return std::nullopt;
    }
    return _log.front();
}

auto replicated_log::inmemory_log::dump(replicated_log::inmemory_log::log_type const &log) -> std::string {
    auto builder = velocypack::Builder();
    auto stream = std::stringstream();
    stream << "[";
    bool first = true;
    for (auto const &it : log) {
        if (first) {
            first = false;
        } else {
            stream << ", ";
        }
        it.entry().toVelocyPack(builder);
        stream << builder.toJson();
        builder.clear();
    }
    stream << "]";

    return stream.str();
}

auto replicated_log::inmemory_log::dump() const -> std::string {
    return dump(_log);
}

auto replicated_log::inmemory_log::get_index_range() const noexcept -> log_range {
    return {_first, _first + _log.size()};
}

auto replicated_log::inmemory_log::get_first_index() const noexcept -> log_index {
    return _first;
}

auto replicated_log::inmemory_log::loadFromLogCore(replicated_log::log_core const &core) -> replicated_log::inmemory_log {
    auto iter = core.read(log_index {0});
    auto log = log_type::transient_type {};
    while (auto entry = iter->next()) {
        log.push_back(inmemory_log_entry(std::move(entry).value()));
    }
    return inmemory_log {log.persistent()};
}
