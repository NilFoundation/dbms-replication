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

#include "nil/dbms/replication/state/state.hpp"
#include <nil/dbms/replication/state/state_interfaces.hpp>

namespace nil::dbms::replication::state {
    /**
     * The Document State Machine is used as a middle-man between a shard and a
     * replicated log, inside collections from databases configured with
     * replication.
     */
    namespace document {

        struct DocumentFactory;
        struct DocumentLogEntry;
        struct DocumentLeaderState;
        struct DocumentFollowerState;
        struct DocumentCore;

        struct DocumentState {
            using LeaderType = DocumentLeaderState;
            using FollowerType = DocumentFollowerState;
            using EntryType = DocumentLogEntry;
            using FactoryType = DocumentFactory;
            using CoreType = DocumentCore;
        };

        /* Empty for now */
        struct DocumentLogEntry { };
        struct DocumentCore { };

        struct DocumentLeaderState : state::IReplicatedLeaderState<DocumentState> {
            explicit DocumentLeaderState(std::unique_ptr<DocumentCore> core);

            [[nodiscard]] auto resign() &&noexcept -> std::unique_ptr<DocumentCore> override;

            auto recover_entries(std::unique_ptr<EntryIterator> ptr) -> futures::Future<Result> override;

            std::unique_ptr<DocumentCore> _core;
        };

        struct DocumentFollowerState : state::IReplicatedFollowerState<DocumentState> {
            explicit DocumentFollowerState(std::unique_ptr<DocumentCore> core);

        protected:
            [[nodiscard]] auto resign() &&noexcept -> std::unique_ptr<DocumentCore> override;
            auto acquireSnapshot(ParticipantId const &destination, log_index) noexcept
                -> futures::Future<Result> override;
            auto apply_entries(std::unique_ptr<EntryIterator> ptr) noexcept -> futures::Future<Result> override;

            std::unique_ptr<DocumentCore> _core;
        };

        struct DocumentFactory {
            auto construct_follower(std::unique_ptr<DocumentCore> core) -> std::shared_ptr<DocumentFollowerState>;
            auto construct_leader(std::unique_ptr<DocumentCore> core) -> std::shared_ptr<DocumentLeaderState>;
            auto construct_core(global_log_identifier const &) -> std::unique_ptr<DocumentCore>;
        };
    }    // namespace document

    template<>
    struct EntryDeserializer<document::DocumentLogEntry> {
        auto operator()(streams::serializer_tag_t<state::document::DocumentLogEntry>,
                        velocypack::Slice s) const -> state::document::DocumentLogEntry;
    };

    template<>
    struct EntrySerializer<document::DocumentLogEntry> {
        void operator()(streams::serializer_tag_t<state::document::DocumentLogEntry>,
                        state::document::DocumentLogEntry const &e,
                        velocypack::Builder &b) const;
    };

    extern template struct state::ReplicatedState<document::DocumentState>;

}    // namespace nil::dbms::replication::state
