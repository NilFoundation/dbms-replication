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

#include <string_view>
#include <velocypack/Buffer.h>
#include <velocypack/Slice.h>

#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/log/log_common.hpp>

namespace nil::dbms::replication {

    struct log_payload {
        using BufferType = std::basic_string<std::uint8_t>;

        explicit log_payload(BufferType dummy);

        // Named constructors, have to make copies.
        [[nodiscard]] static auto create_from_slice(velocypack::Slice slice) -> log_payload;
        [[nodiscard]] static auto create_from_string(std::string_view string) -> log_payload;

        friend auto operator==(log_payload const &, log_payload const &) -> bool;

        [[nodiscard]] auto byte_size() const noexcept -> std::size_t;
        [[nodiscard]] auto slice() const noexcept -> velocypack::Slice;
        [[nodiscard]] auto copy_buffer() const -> velocypack::UInt8Buffer;

    private:
        BufferType buffer;
    };

    auto operator==(log_payload const &, log_payload const &) -> bool;

    struct LogMetaPayload {
        struct FirstEntryOfTerm {
            ParticipantId leader;
            agency::participants_config participants;

            static auto from_velocy_pack(velocypack::Slice) -> FirstEntryOfTerm;
            void to_velocy_pack(velocypack::Builder &) const;

            bool operator==(const FirstEntryOfTerm &rhs) const {
                return leader == rhs.leader && participants == rhs.participants;
            }
            bool operator!=(const FirstEntryOfTerm &rhs) const {
                return !(rhs == *this);
            }
        };

        static auto withFirstEntryOfTerm(ParticipantId leader, agency::participants_config config) -> LogMetaPayload;

        struct Updateparticipants_config {
            agency::participants_config participants;

            static auto from_velocy_pack(velocypack::Slice) -> Updateparticipants_config;
            void to_velocy_pack(velocypack::Builder &) const;

            bool operator==(const Updateparticipants_config &rhs) const {
                return participants == rhs.participants;
            }
            bool operator!=(const Updateparticipants_config &rhs) const {
                return !(rhs == *this);
            }
        };

        static auto withUpdateparticipants_config(agency::participants_config config) -> LogMetaPayload;

        static auto from_velocy_pack(velocypack::Slice) -> LogMetaPayload;
        void to_velocy_pack(velocypack::Builder &) const;

        std::variant<FirstEntryOfTerm, Updateparticipants_config> info;
        bool operator==(const LogMetaPayload &rhs) const {
            return info == rhs.info;
        }
        bool operator!=(const LogMetaPayload &rhs) const {
            return !(rhs == *this);
        }
    };

    class PersistingLogEntry {
    public:
        PersistingLogEntry(log_term term, log_index index, log_payload payload) :
            PersistingLogEntry(term_index_pair {term, index}, std::move(payload)) {
        }
        PersistingLogEntry(term_index_pair, std::variant<LogMetaPayload, log_payload>);
        PersistingLogEntry(log_index, velocypack::Slice persisted);    // RocksDB from disk constructor

        [[nodiscard]] auto log_term() const noexcept -> log_term;
        [[nodiscard]] auto log_index() const noexcept -> log_index;
        [[nodiscard]] auto log_payload() const noexcept -> log_payload const *;
        [[nodiscard]] auto log_term_index_pair() const noexcept -> term_index_pair;
        [[nodiscard]] auto approxbyte_size() const noexcept -> std::size_t;
        [[nodiscard]] auto hasPayload() const noexcept -> bool;
        [[nodiscard]] auto hasMeta() const noexcept -> bool;
        [[nodiscard]] auto meta() const noexcept -> LogMetaPayload const *;

        class Omitlog_index { };
        constexpr static auto omitlog_index = Omitlog_index();
        void to_velocy_pack(velocypack::Builder &builder) const;
        void to_velocy_pack(velocypack::Builder &builder, Omitlog_index) const;
        static auto from_velocy_pack(velocypack::Slice slice) -> PersistingLogEntry;

        bool operator==(const PersistingLogEntry &rhs) const {
            return _termIndex == rhs._termIndex && _payload == rhs._payload;
        }
        bool operator!=(const PersistingLogEntry &rhs) const {
            return !(rhs == *this);
        }

    private:
        void entriesWithoutIndexto_velocy_pack(velocypack::Builder &builder) const;

        term_index_pair _termIndex;
        // TODO It seems impractical to not copy persisting log entries, so we should
        //      probably make this a shared_ptr (or immer::box).
        std::variant<LogMetaPayload, log_payload> _payload;

        // TODO this is a magic constant "measuring" the size of
        //      of the non-payload data in a PersistingLogEntry
        static inline constexpr auto approxMetaDataSize = std::size_t {42 * 2};
    };

    // A log entry, enriched with non-persisted metadata, to be stored in an
    // InMemoryLog.
    class InMemoryLogEntry {
    public:
        using clock = std::chrono::steady_clock;

        explicit InMemoryLogEntry(PersistingLogEntry entry, bool waitForSync = false);

        [[nodiscard]] auto insertTp() const noexcept -> clock::time_point;
        void setInsertTp(clock::time_point) noexcept;
        [[nodiscard]] auto entry() const noexcept -> PersistingLogEntry const &;
        [[nodiscard]] bool getWaitForSync() const noexcept {
            return _waitForSync;
        }

    private:
        bool _waitForSync;
        // Immutable box that allows sharing, i.e. cheap copying.
        ::immer::box<PersistingLogEntry, ::nil::dbms::immer::dbms_memory_policy> _logEntry;
        // Timepoint at which the insert was started (not the point in time where it
        // was committed)
        clock::time_point _insertTp {};
    };

    // A log entry as visible to the user of a replicated log.
    // Does thus always contain a payload: only internal log entries are without
    // payload, which aren't visible to the user. User-defined log entries always
    // contain a payload.
    // The term is not of interest, and therefore not part of this struct.
    // Note that when these entries are visible, they are already committed.
    // It does not own the payload, so make sure it is still valid when using it.
    class LogEntryView {
    public:
        LogEntryView() = delete;
        LogEntryView(log_index index, log_payload const &payload) noexcept;
        LogEntryView(log_index index, velocypack::Slice payload) noexcept;

        [[nodiscard]] auto log_index() const noexcept -> log_index;
        [[nodiscard]] auto log_payload() const noexcept -> velocypack::Slice;
        [[nodiscard]] auto clonePayload() const -> log_payload;

        void to_velocy_pack(velocypack::Builder &builder) const;
        static auto from_velocy_pack(velocypack::Slice slice) -> LogEntryView;

    private:
        log_index _index;
        velocypack::Slice _payload;
    };

    template<typename T>
    struct TypedLogIterator {
        virtual ~TypedLogIterator() = default;
        // The returned view is guaranteed to stay valid until a successive next()
        // call (only).
        virtual auto next() -> std::optional<T> = 0;
    };

    template<typename T>
    struct Typedlog_rangeIterator : TypedLogIterator<T> {
        // returns the index interval [from, to)
        // Note that this does not imply that all indexes in the range [from, to)
        // are returned. Hence (to - from) is only an upper bound on the number of
        // entries returned.
        [[nodiscard]] virtual auto range() const noexcept -> log_range = 0;
    };

    using LogIterator = TypedLogIterator<LogEntryView>;
    using log_rangeIterator = Typedlog_rangeIterator<LogEntryView>;

    // ReplicatedLog-internal iterator over PersistingLogEntries
    struct persisted_logIterator : TypedLogIterator<PersistingLogEntry> { };
}    // namespace nil::dbms::replication