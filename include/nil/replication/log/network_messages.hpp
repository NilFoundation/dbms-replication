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

#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/log_entries.hpp>
#include <nil/dbms/replication/log/types.hpp>

namespace nil::dbms {
    class Result;
}

namespace nil::dbms::replication::log {

    struct MessageId {
        constexpr MessageId() noexcept : value {0} {
        }
        constexpr explicit MessageId(std::uint64_t value) noexcept : value {value} {
        }

        bool operator==(const MessageId &rhs) const {
            return value == rhs.value;
        }
        bool operator!=(const MessageId &rhs) const {
            return !(rhs == *this);
        }

        bool operator<(const MessageId &rhs) const {
            return value < rhs.value;
        }
        bool operator>(const MessageId &rhs) const {
            return rhs < *this;
        }
        bool operator<=(const MessageId &rhs) const {
            return !(rhs < *this);
        }
        bool operator>=(const MessageId &rhs) const {
            return !(*this < rhs);
        }

        friend auto operator++(MessageId &id) -> MessageId &;
        friend auto operator<<(std::ostream &os, MessageId id) -> std::ostream &;
        friend auto to_string(MessageId id) -> std::string;

        [[nodiscard]] explicit operator velocypack::Value() const noexcept;

    private:
        std::uint64_t value;
    };

    auto operator++(MessageId &id) -> MessageId &;
    auto operator<<(std::ostream &os, MessageId id) -> std::ostream &;
    auto to_string(MessageId id) -> std::string;

#if (defined(__GNUC__) && !defined(__clang__))
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
    struct AppendEntriesResult {
        log_term const log_term;
        ErrorCode const errorCode;
        AppendEntriesErrorReason reason;
        MessageId messageId;

        std::optional<term_index_pair> conflict;

        [[nodiscard]] auto isSuccess() const noexcept -> bool;

        AppendEntriesResult(log_term term, MessageId id, term_index_pair conflict,
                            AppendEntriesErrorReason reason) noexcept;
        AppendEntriesResult(log_term, MessageId) noexcept;
        AppendEntriesResult(log_term log_term, ErrorCode errorCode, AppendEntriesErrorReason reason, MessageId) noexcept;
        void to_velocy_pack(velocypack::Builder &builder) const;
        static auto from_velocy_pack(velocypack::Slice slice) -> AppendEntriesResult;

        static auto withConflict(log_term, MessageId, term_index_pair conflict) noexcept -> AppendEntriesResult;
        static auto withRejection(log_term, MessageId, AppendEntriesErrorReason) noexcept -> AppendEntriesResult;
        static auto withPersistenceError(log_term, MessageId, Result const &) noexcept -> AppendEntriesResult;
        static auto withOk(log_term, MessageId) noexcept -> AppendEntriesResult;
    };
#if (defined(__GNUC__) && !defined(__clang__))
#pragma GCC diagnostic pop
#endif

    struct AppendEntriesRequest {
        using EntryContainer = ::immer::flex_vector<InMemoryLogEntry, nil::dbms::immer::dbms_memory_policy>;

        log_term leaderTerm;
        ParticipantId leaderId;
        term_index_pair prevLogEntry;
        log_index leaderCommit;
        log_index lowestIndexToKeep;
        MessageId messageId;
        EntryContainer entries {};
        bool waitForSync = false;

        AppendEntriesRequest() = default;
        AppendEntriesRequest(log_term leaderTerm, ParticipantId leaderId, term_index_pair prevLogEntry,
                             log_index leaderCommit, log_index lowestIndexToKeep, MessageId messageId, bool waitForSync,
                             EntryContainer entries);
        ~AppendEntriesRequest() noexcept = default;

        AppendEntriesRequest(AppendEntriesRequest &&other) noexcept;
        AppendEntriesRequest(AppendEntriesRequest const &other) = default;
        auto operator=(AppendEntriesRequest &&other) noexcept -> AppendEntriesRequest &;
        auto operator=(AppendEntriesRequest const &other) -> AppendEntriesRequest & = default;

        void to_velocy_pack(velocypack::Builder &builder) const;
        static auto from_velocy_pack(velocypack::Slice slice) -> AppendEntriesRequest;
    };

}    // namespace nil::dbms::replication::log

namespace nil::dbms {
    template<>
    struct velocypack::Extractor<replication::log::MessageId> {
        static auto extract(velocypack::Slice slice) -> replication::log::MessageId {
            return replication::log::MessageId {slice.getNumericValue<uint64_t>()};
        }
    };
}    // namespace nil::dbms
