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

#include "agency_log_specification.hpp"
#include "log_common.hpp"

namespace nil::dbms::replication {

    struct log_payload {
        using BufferType = std::basic_string<std::uint8_t>;

        explicit log_payload(BufferType dummy);

        // Named constructors, have to make copies.
        [[nodiscard]] static auto create_from_slice(velocypack::Slice slice) -> log_payload;
        [[nodiscard]] static auto create_from_string(std::string_view string) -> log_payload;

        friend auto operator==(log_payload const &, log_payload const &) -> bool;

        [[nodiscard]] auto byteSize() const noexcept -> std::size_t;
        [[nodiscard]] auto slice() const noexcept -> velocypack::Slice;
        [[nodiscard]] auto copyBuffer() const -> velocypack::UInt8Buffer;

    private:
        BufferType buffer;
    };

    auto operator==(log_payload const &, log_payload const &) -> bool;

    struct log_meta_payload {
        struct first_entry_of_term {
            ParticipantId leader;
            agency::participants_config participants;

            static auto fromVelocyPack(velocypack::Slice) -> first_entry_of_term;
            void toVelocyPack(velocypack::Builder &) const;

            bool operator==(const first_entry_of_term &rhs) const {
                return leader == rhs.leader && participants == rhs.participants;
            }
            bool operator!=(const first_entry_of_term &rhs) const {
                return !(rhs == *this);
            }
        };

        static auto withFirstEntryOfTerm(ParticipantId leader, agency::participants_config config) -> log_meta_payload;

        struct update_participants_config {
            agency::participants_config participants;

            static auto fromVelocyPack(velocypack::Slice) -> update_participants_config;
            void toVelocyPack(velocypack::Builder &) const;

            bool operator==(const update_participants_config &rhs) const {
                return participants == rhs.participants;
            }
            bool operator!=(const update_participants_config &rhs) const {
                return !(rhs == *this);
            }
        };

        static auto withUpdateParticipantsConfig(agency::participants_config config) -> log_meta_payload;

        static auto fromVelocyPack(velocypack::Slice) -> log_meta_payload;
        void toVelocyPack(velocypack::Builder &) const;

        std::variant<first_entry_of_term, update_participants_config> info;
        bool operator==(const log_meta_payload &rhs) const {
            return info == rhs.info;
        }
        bool operator!=(const log_meta_payload &rhs) const {
            return !(rhs == *this);
        }
    };

    class persisting_log_entry {
    public:
        persisting_log_entry(log_term term, log_index index, log_payload payload) :
            persisting_log_entry(term_index_pair {term, index}, std::move(payload)) {
        }
        persisting_log_entry(term_index_pair, std::variant<log_meta_payload, log_payload>);
        persisting_log_entry(log_index, velocypack::Slice persisted);    // RocksDB from disk constructor

        [[nodiscard]] auto logTerm() const noexcept -> log_term;
        [[nodiscard]] auto logIndex() const noexcept -> log_index;
        [[nodiscard]] auto logPayload() const noexcept -> log_payload const *;
        [[nodiscard]] auto logTermIndexPair() const noexcept -> term_index_pair;
        [[nodiscard]] auto approxByteSize() const noexcept -> std::size_t;
        [[nodiscard]] auto hasPayload() const noexcept -> bool;
        [[nodiscard]] auto hasMeta() const noexcept -> bool;
        [[nodiscard]] auto meta() const noexcept -> log_meta_payload const *;

        class OmitLogIndex { };
        constexpr static auto omitLogIndex = OmitLogIndex();
        void toVelocyPack(velocypack::Builder &builder) const;
        void toVelocyPack(velocypack::Builder &builder, OmitLogIndex) const;
        static auto fromVelocyPack(velocypack::Slice slice) -> persisting_log_entry;

        bool operator==(const persisting_log_entry &rhs) const {
            return _termIndex == rhs._termIndex && _payload == rhs._payload;
        }
        bool operator!=(const persisting_log_entry &rhs) const {
            return !(rhs == *this);
        }

    private:
        void entriesWithoutIndexToVelocyPack(velocypack::Builder &builder) const;

        term_index_pair _termIndex;
        // TODO It seems impractical to not copy persisting log entries, so we should
        //      probably make this a shared_ptr (or immer::box).
        std::variant<log_meta_payload, log_payload> _payload;

        // TODO this is a magic constant "measuring" the size of
        //      of the non-payload data in a PersistingLogEntry
        static inline constexpr auto approxMetaDataSize = std::size_t {42 * 2};
    };

    // A log entry, enriched with non-persisted metadata, to be stored in an
    // InMemoryLog.
    class inmemory_log_entry {
    public:
        using clock = std::chrono::steady_clock;

        explicit inmemory_log_entry(persisting_log_entry entry, bool waitForSync = false);

        [[nodiscard]] auto insert_tp() const noexcept -> clock::time_point;
        void setInsertTp(clock::time_point) noexcept;
        [[nodiscard]] auto entry() const noexcept -> persisting_log_entry const &;
        [[nodiscard]] bool get_wait_for_sync() const noexcept {
            return _waitForSync;
        }

    private:
        bool _waitForSync;
        // Immutable box that allows sharing, i.e. cheap copying.
        ::immer::box<persisting_log_entry, ::nil::dbms::immer::dbms_memory_policy> _logEntry;
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
    class log_entry_view {
    public:
        log_entry_view() = delete;
        log_entry_view(log_index index, log_payload const &payload) noexcept;
        log_entry_view(log_index index, velocypack::Slice payload) noexcept;

        [[nodiscard]] auto logIndex() const noexcept -> log_index;
        [[nodiscard]] auto logPayload() const noexcept -> velocypack::Slice;
        [[nodiscard]] auto clonePayload() const -> log_payload;

        void toVelocyPack(velocypack::Builder &builder) const;
        static auto fromVelocyPack(velocypack::Slice slice) -> log_entry_view;

    private:
        log_index _index;
        velocypack::Slice _payload;
    };

    template<typename T>
    struct typed_log_iterator {
        virtual ~typed_log_iterator() = default;
        // The returned view is guaranteed to stay valid until a successive next()
        // call (only).
        virtual auto next() -> std::optional<T> = 0;
    };

    template<typename T>
    struct typed_log_range_iterator : typed_log_iterator<T> {
        // returns the index interval [from, to)
        // Note that this does not imply that all indexes in the range [from, to)
        // are returned. Hence (to - from) is only an upper bound on the number of
        // entries returned.
        [[nodiscard]] virtual auto range() const noexcept -> log_range = 0;
    };

    using LogIterator = typed_log_iterator<log_entry_view>;
    using LogRangeIterator = typed_log_range_iterator<log_entry_view>;

    // ReplicatedLog-internal iterator over PersistingLogEntries
    struct persisted_log_iterator : typed_log_iterator<persisting_log_entry> { };
}    // namespace nil::dbms::replication
