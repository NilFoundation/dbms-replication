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

#pragma once

#include <nil/dbms/replication/logger_context.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/log_entries.hpp>

#include <containers/immer_memory_policy.h>
#include <velocypack/Builder.h>

#include <optional>

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

#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

namespace nil {
    namespace dbms {
        namespace replication {
            namespace log {
                struct log_core;

                class log_iterator;

                /**
                 * @brief The ephemeral part of the replicated log held in memory. Can hold more
                 * recent entries than the corresponding persisted log, while the latter is
                 * catching up. On startup (or currently, on creation of a leader or follower
                 * instance), this is restored from the persisted log.
                 */
                struct in_memory_log {
                public:
                    template<typename T>
                    using log_type_t = ::immer::flex_vector<T, nil::dbms::immer::dbms_memory_policy>;
                    using log_type = log_type_t<in_memory_logEntry>;
                    using log_type_persisted = log_type_t<persisting_log_entry>;

                private:
                    log_type _log{};
                    log_index _first{1};

                public:
                    in_memory_log() = default;

                    explicit in_memory_log(log_type log);

                    in_memory_log(in_memory_log &&other) noexcept;

                    in_memory_log(in_memory_log const &) = default;

                    auto operator=(in_memory_log &&other) noexcept -> in_memory_log &;

                    auto operator=(in_memory_log const &) -> in_memory_log & = default;

                    ~in_memory_log() noexcept = default;

                    [[nodiscard]] auto getLastterm_index_pair() const noexcept -> term_index_pair;

                    [[nodiscard]] auto getLastIndex() const noexcept -> log_index;

                    [[nodiscard]] auto getLastTerm() const noexcept -> log_term;

                    [[nodiscard]] auto getLastEntry() const noexcept -> std::optional<in_memory_logEntry>;

                    [[nodiscard]] auto getFirstEntry() const noexcept -> std::optional<in_memory_logEntry>;

                    [[nodiscard]] auto getFirstIndex() const noexcept -> log_index;

                    [[nodiscard]] auto getNextIndex() const noexcept -> log_index;

                    [[nodiscard]] auto getEntryByIndex(log_index idx) const noexcept -> std::optional<in_memory_logEntry>;

                    [[nodiscard]] auto slice(log_index from, log_index to) const -> log_type;

                    [[nodiscard]] auto getFirstIndexOfTerm(log_term term) const noexcept -> std::optional<log_index>;

                    [[nodiscard]] auto getLastIndexOfTerm(log_term term) const noexcept -> std::optional<log_index>;

                    [[nodiscard]] auto getIndexRange() const noexcept -> log_range;

                    // @brief Unconditionally accesses the last element
                    [[nodiscard]] auto back() const noexcept -> decltype(_log)::const_reference;

                    [[nodiscard]] auto empty() const noexcept -> bool;

                    [[nodiscard]] auto release(log_index stop) const -> in_memory_log;

                    void appendInPlace(logger_context const &logContext, in_memory_logEntry entry);

                    [[nodiscard]] auto
                    append(logger_context const &logContext, log_type entries) const -> in_memory_log;

                    [[nodiscard]] auto append(logger_context const &logContext, log_type_persisted const &entries) const
                    -> in_memory_log;

                    [[nodiscard]] auto getIteratorFrom(log_index fromIdx) const -> std::unique_ptr<LogIterator>;

                    [[nodiscard]] auto
                    getInternalIteratorFrom(log_index fromIdx) const -> std::unique_ptr<persisted_logIterator>;

                    [[nodiscard]] auto getInternalIteratorRange(log_index fromIdx, log_index toIdx) const
                    -> std::unique_ptr<persisted_logIterator>;

                    [[nodiscard]] auto getMemtryIteratorFrom(log_index fromIdx) const
                    -> std::unique_ptr <TypedLogIterator<in_memory_logEntry>>;

                    [[nodiscard]] auto getMemtryIteratorRange(log_index fromIdx, log_index toIdx) const
                    -> std::unique_ptr <TypedLogIterator<in_memory_logEntry>>;

                    // get an iterator for range [from, to).
                    [[nodiscard]] auto getIteratorRange(log_index fromIdx, log_index toIdx) const
                    -> std::unique_ptr<log_rangeIterator>;

                    [[nodiscard]] auto takeSnapshotUpToAndIncluding(log_index until) const -> in_memory_log;

                    [[nodiscard]] auto copyFlexVector() const -> log_type;

                    // helpful for debugging
                    [[nodiscard]] static auto dump(log_type const &log) -> std::string;

                    [[nodiscard]] auto dump() const -> std::string;

                    [[nodiscard]] static auto loadFromlog_core(log_core const &) -> in_memory_log;

                protected:
                    explicit in_memory_log(log_type log, log_index first);
                };

            }
        }
    }
}    // namespace nil::dbms::replication::log
