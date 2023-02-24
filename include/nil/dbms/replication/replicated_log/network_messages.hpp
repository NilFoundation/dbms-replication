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

#include <basics/error_code.h>
#include <containers/immer_memory_policy.h>

#include <string>
#include <ostream>

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

#include "log_common.hpp"
#include "log_entries.hpp"
#include "types.hpp"

namespace nil::dbms {
    class Result;
}

namespace nil::dbms::replication::replicated_log {

    struct message_id {
        constexpr message_id() noexcept : value {0} {
        }
        constexpr explicit message_id(std::uint64_t value) noexcept : value {value} {
        }

        bool operator==(const message_id &rhs) const {
            return value == rhs.value;
        }
        bool operator!=(const message_id &rhs) const {
            return !(rhs == *this);
        }

        bool operator<(const message_id &rhs) const {
            return value < rhs.value;
        }
        bool operator>(const message_id &rhs) const {
            return rhs < *this;
        }
        bool operator<=(const message_id &rhs) const {
            return !(rhs < *this);
        }
        bool operator>=(const message_id &rhs) const {
            return !(*this < rhs);
        }

        friend auto operator++(message_id &id) -> message_id &;
        friend auto operator<<(std::ostream &os, message_id id) -> std::ostream &;
        friend auto to_string(message_id id) -> std::string;

        [[nodiscard]] explicit operator velocypack::Value() const noexcept;

    private:
        std::uint64_t value;
    };

    auto operator++(message_id &id) -> message_id &;
    auto operator<<(std::ostream &os, message_id id) -> std::ostream &;
    auto to_string(message_id id) -> std::string;

#if (defined(__GNUC__) && !defined(__clang__))
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
    struct append_entries_result {
        log_term const logTerm;
        ErrorCode const errorCode;
        append_entries_error_reason reason;
        message_id messageId;

        std::optional<term_index_pair> conflict;

        [[nodiscard]] auto isSuccess() const noexcept -> bool;

        append_entries_result(log_term term, message_id id, term_index_pair conflict,
                              append_entries_error_reason reason) noexcept;
        append_entries_result(log_term, message_id) noexcept;
        append_entries_result(log_term logTerm, ErrorCode errorCode, append_entries_error_reason reason,
                              message_id) noexcept;
        void toVelocyPack(velocypack::Builder &builder) const;
        static auto fromVelocyPack(velocypack::Slice slice) -> append_entries_result;

        static auto withConflict(log_term, message_id, term_index_pair conflict) noexcept -> append_entries_result;
        static auto withRejection(log_term, message_id, append_entries_error_reason) noexcept -> append_entries_result;
        static auto withPersistenceError(log_term, message_id, Result const &) noexcept -> append_entries_result;
        static auto withOk(log_term, message_id) noexcept -> append_entries_result;
    };
#if (defined(__GNUC__) && !defined(__clang__))
#pragma GCC diagnostic pop
#endif

    struct append_entries_request {
        using EntryContainer = ::immer::flex_vector<inmemory_log_entry, nil::dbms::immer::dbms_memory_policy>;

        log_term leaderTerm;
        ParticipantId leaderId;
        term_index_pair prevLogEntry;
        log_index leaderCommit;
        log_index lowestIndexToKeep;
        message_id messageId;
        EntryContainer entries {};
        bool waitForSync = false;

        append_entries_request() = default;
        append_entries_request(log_term leaderTerm, ParticipantId leaderId, term_index_pair prevLogEntry,
                               log_index leaderCommit, log_index lowestIndexToKeep, message_id messageId,
                               bool waitForSync, EntryContainer entries);
        ~append_entries_request() noexcept = default;

        append_entries_request(append_entries_request &&other) noexcept;
        append_entries_request(append_entries_request const &other) = default;
        auto operator=(append_entries_request &&other) noexcept -> append_entries_request &;
        auto operator=(append_entries_request const &other) -> append_entries_request & = default;

        void toVelocyPack(velocypack::Builder &builder) const;
        static auto fromVelocyPack(velocypack::Slice slice) -> append_entries_request;
    };

}    // namespace nil::dbms::replication::replicated_log

namespace nil::dbms {
    template<>
    struct velocypack::Extractor<replication::replicated_log::message_id> {
        static auto extract(velocypack::Slice slice) -> replication::replicated_log::message_id {
            return replication::replicated_log::message_id {slice.getNumericValue<uint64_t>()};
        }
    };
}    // namespace nil::dbms
