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

#include "nil/replication_sdk/replicated_state/replicated_state.hpp"
#include <nil/replication_sdk/replicated_state/state_interfaces.hpp>

namespace nil::dbms::replication_sdk::replicated_state {
    /**
     * The black-hole state machine is here only for testing purpose. It accepts
     * all writes. Writes are replicated and then discarded. Followers do nothing,
     * except receiving log data. Snapshot transfer is always successful.
     */
    namespace black_hole {

        struct BlackHoleFactory;
        struct BlackHoleLogEntry;
        struct BlackHoleLeaderState;
        struct BlackHoleFollowerState;
        struct BlackHoleCore;

        struct BlackHoleState {
            using LeaderType = BlackHoleLeaderState;
            using FollowerType = BlackHoleFollowerState;
            using EntryType = BlackHoleLogEntry;
            using FactoryType = BlackHoleFactory;
            using CoreType = BlackHoleCore;
        };

        struct BlackHoleLogEntry {
            std::string value;
        };

        struct BlackHoleLeaderState : replicated_state::ireplicated_leader_state<BlackHoleState> {
            explicit BlackHoleLeaderState(std::unique_ptr<BlackHoleCore> core);
            auto write(std::string_view) -> log_index;

            [[nodiscard]] auto resign() &&noexcept -> std::unique_ptr<BlackHoleCore> override;

        protected:
            auto recoverEntries(std::unique_ptr<EntryIterator> ptr) -> futures::Future<Result> override;

            std::unique_ptr<BlackHoleCore> _core;
        };

        struct BlackHoleFollowerState : replicated_state::ireplicated_follower_state<BlackHoleState> {
            explicit BlackHoleFollowerState(std::unique_ptr<BlackHoleCore> core);

            [[nodiscard]] auto resign() &&noexcept -> std::unique_ptr<BlackHoleCore> override;

        protected:
            auto acquireSnapshot(ParticipantId const &destination, log_index) noexcept
                -> futures::Future<Result> override;
            auto apply_entries(std::unique_ptr<EntryIterator> ptr) noexcept -> futures::Future<Result> override;

            std::unique_ptr<BlackHoleCore> _core;
        };

        struct BlackHoleCore { };

        struct BlackHoleFactory {
            auto constructFollower(std::unique_ptr<BlackHoleCore> core) -> std::shared_ptr<BlackHoleFollowerState>;
            auto constructLeader(std::unique_ptr<BlackHoleCore> core) -> std::shared_ptr<BlackHoleLeaderState>;
            auto constructCore(global_log_identifier const &) -> std::unique_ptr<BlackHoleCore>;
        };

    }    // namespace black_hole

    template<>
    struct entry_deserializer<black_hole::BlackHoleLogEntry> {
        auto operator()(streams::serializer_tag_t<replicated_state::black_hole::BlackHoleLogEntry>,
                        velocypack::Slice s) const -> replicated_state::black_hole::BlackHoleLogEntry;
    };

    template<>
    struct entry_serializer<black_hole::BlackHoleLogEntry> {
        void operator()(streams::serializer_tag_t<replicated_state::black_hole::BlackHoleLogEntry>,
                        replicated_state::black_hole::BlackHoleLogEntry const &e,
                        velocypack::Builder &b) const;
    };

    extern template struct replicated_state::replicated_state_t<black_hole::BlackHoleState>;
}    // namespace nil::dbms::replication_sdk::replicated_state
