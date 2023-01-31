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

#include "state.hpp"
#include <nil/dbms/replication/state/state_interfaces.hpp>
#include "nil/dbms/replication/streams/streams.hpp"
#include <nil/dbms/replication/streams/log_multiplexer.hpp>

#include <basics/guarded.h>

#include <memory>

namespace nil::dbms::replication::state {

    template<typename S>
    struct LeaderStateManager : ReplicatedState<S>::IStateManager, std::enable_shared_from_this<LeaderStateManager<S>> {
        using Factory = typename ReplicatedStateTraits<S>::FactoryType;
        using EntryType = typename ReplicatedStateTraits<S>::EntryType;
        using FollowerType = typename ReplicatedStateTraits<S>::FollowerType;
        using LeaderType = typename ReplicatedStateTraits<S>::LeaderType;
        using CoreType = typename ReplicatedStateTraits<S>::CoreType;

        using wait_for_appliedQueue = typename ReplicatedState<S>::IStateManager::wait_for_appliedQueue;
        using wait_for_appliedPromise = typename ReplicatedState<S>::IStateManager::wait_for_appliedQueue;

        explicit LeaderStateManager(logger_context loggerContext,
                                    std::shared_ptr<ReplicatedState<S>> const &parent,
                                    std::shared_ptr<log::Ilog_leader>
                                        leader,
                                    std::unique_ptr<CoreType>
                                        core,
                                    std::unique_ptr<ReplicatedStateToken>
                                        token,
                                    std::shared_ptr<Factory>
                                        factory) noexcept;

        using Stream = streams::ProducerStream<EntryType>;
        using Iterator = typename Stream::Iterator;

        [[nodiscard]] auto get_status() const -> StateStatus final;

        void run() noexcept override;

        [[nodiscard]] auto resign() &&noexcept
            -> std::tuple<std::unique_ptr<CoreType>, std::unique_ptr<ReplicatedStateToken>, deferred_action> override;

        using Multiplexer = streams::LogMultiplexer<ReplicatedStateStreamSpec<S>>;

        auto getImplementationState() -> std::shared_ptr<IReplicatedLeaderState<S>>;

        struct guarded_data {
            explicit guarded_data(LeaderStateManager &self,
                                 leader_internal_state internalState,
                                 std::unique_ptr<CoreType>
                                     core,
                                 std::unique_ptr<ReplicatedStateToken>
                                     token);
            LeaderStateManager &self;
            std::shared_ptr<IReplicatedLeaderState<S>> state;
            std::shared_ptr<Stream> stream;

            leader_internal_state internalState {leader_internal_state::kUninitializedState};
            std::chrono::system_clock::time_point lastInternalStateChange;
            std::optional<log_range> recoveryRange;

            std::unique_ptr<CoreType> core;
            std::unique_ptr<ReplicatedStateToken> token;
            bool _didResign = false;

            void updateInternalState(leader_internal_state newState, std::optional<log_range> range = std::nullopt) {
                internalState = newState;
                lastInternalStateChange = std::chrono::system_clock::now();
                recoveryRange = range;
            }
        };

        Guarded<guarded_data> guarded_data;
        std::weak_ptr<ReplicatedState<S>> const parent;
        std::shared_ptr<log::Ilog_leader> const log_leader;
        logger_context const loggerContext;
        std::shared_ptr<Factory> const factory;

    private:
        void beginWaitingForParticipantResigned();
    };

    template<typename S>
    LeaderStateManager<S>::guarded_data::guarded_data(LeaderStateManager &self, leader_internal_state internalState,
                                                    std::unique_ptr<CoreType> core,
                                                    std::unique_ptr<ReplicatedStateToken> token) :
        self(self),
        internalState(internalState), core(std::move(core)), token(std::move(token)) {
        TRI_ASSERT(this->core != nullptr);
        TRI_ASSERT(this->token != nullptr);
    }
}    // namespace nil::dbms::replication::state
