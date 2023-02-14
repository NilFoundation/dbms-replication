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

#include "replicated_state.hpp"
#include <nil/replication_sdk/replicated_state/state_interfaces.hpp>
#include "nil/replication_sdk/streams/streams.hpp"
#include <nil/replication_sdk/streams/log_multiplexer.hpp>

#include <basics/guarded.h>

#include <memory>

namespace nil::dbms::replication_sdk::replicated_state {

    template<typename S>
    struct leader_state_manager : replicated_state_t<S>::is_state_manager, std::enable_shared_from_this<leader_state_manager<S>> {
        using Factory = typename replicated_state_traits<S>::FactoryType;
        using EntryType = typename replicated_state_traits<S>::EntryType;
        using FollowerType = typename replicated_state_traits<S>::FollowerType;
        using LeaderType = typename replicated_state_traits<S>::LeaderType;
        using CoreType = typename replicated_state_traits<S>::CoreType;

        using WaitForAppliedQueue = typename replicated_state_t<S>::is_state_manager::WaitForAppliedQueue;
        using WaitForAppliedPromise = typename replicated_state_t<S>::is_state_manager::WaitForAppliedQueue;

        explicit leader_state_manager(logger_context loggerContext,
                                    std::shared_ptr<replicated_state_t<S>> const &parent,
                                    std::shared_ptr<replicated_log::ilog_leader>
                                        leader,
                                    std::unique_ptr<CoreType>
                                        core,
                                    std::unique_ptr<replicated_state_token>
                                        token,
                                    std::shared_ptr<Factory>
                                        factory) noexcept;

        using Stream = streams::producer_stream<EntryType>;
        using Iterator = typename Stream::Iterator;

        [[nodiscard]] auto getStatus() const -> state_status final;

        void run() noexcept override;

        [[nodiscard]] auto resign() &&noexcept
            -> std::tuple<std::unique_ptr<CoreType>, std::unique_ptr<replicated_state_token>, deferred_action> override;

        using Multiplexer = streams::log_multiplexer<ReplicatedStateStreamSpec<S>>;

        auto getImplementationState() -> std::shared_ptr<ireplicated_leader_state<S>>;

        struct guarded_data {
            explicit guarded_data(leader_state_manager &self,
                                  leader_internal_state internalState,
                                 std::unique_ptr<CoreType>
                                     core,
                                 std::unique_ptr<replicated_state_token>
                                     token);
            leader_state_manager &self;
            std::shared_ptr<ireplicated_leader_state<S>> state;
            std::shared_ptr<Stream> stream;

            leader_internal_state internalState {leader_internal_state::kUninitializedState};
            std::chrono::system_clock::time_point lastInternalStateChange;
            std::optional<log_range> recoveryRange;

            std::unique_ptr<CoreType> core;
            std::unique_ptr<replicated_state_token> token;
            bool _didResign = false;

            void updateInternalState(leader_internal_state newState, std::optional<log_range> range = std::nullopt) {
                internalState = newState;
                lastInternalStateChange = std::chrono::system_clock::now();
                recoveryRange = range;
            }
        };

        Guarded<guarded_data> guardedData;
        std::weak_ptr<replicated_state_t<S>> const parent;
        std::shared_ptr<replicated_log::ilog_leader> const logLeader;
        logger_context const loggerContext;
        std::shared_ptr<Factory> const factory;

    private:
        void beginWaitingForParticipantResigned();
    };

    template<typename S>
    leader_state_manager<S>::guarded_data::guarded_data(leader_state_manager &self, leader_internal_state internalState,
                                                    std::unique_ptr<CoreType> core,
                                                    std::unique_ptr<replicated_state_token> token) :
        self(self),
        internalState(internalState), core(std::move(core)), token(std::move(token)) {
        TRI_ASSERT(this->core != nullptr);
        TRI_ASSERT(this->token != nullptr);
    }
}    // namespace nil::dbms::replication_sdk::replicated_state
