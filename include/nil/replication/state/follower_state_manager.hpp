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

namespace nil::dbms::replication::state {
    template<typename S>
    struct FollowerStateManager : ReplicatedState<S>::IStateManager,
                                  std::enable_shared_from_this<FollowerStateManager<S>> {
        using Factory = typename ReplicatedStateTraits<S>::FactoryType;
        using EntryType = typename ReplicatedStateTraits<S>::EntryType;
        using FollowerType = typename ReplicatedStateTraits<S>::FollowerType;
        using LeaderType = typename ReplicatedStateTraits<S>::LeaderType;
        using CoreType = typename ReplicatedStateTraits<S>::CoreType;
        using wait_for_appliedQueue = typename ReplicatedState<S>::IStateManager::wait_for_appliedQueue;
        using wait_for_appliedPromise = typename ReplicatedState<S>::IStateManager::wait_for_appliedPromise;

        using Stream = streams::Stream<EntryType>;
        using Iterator = typename Stream::Iterator;

        FollowerStateManager(logger_context loggerContext, std::shared_ptr<replicated_state_base> parent,
                             std::shared_ptr<log::ILogFollower> logFollower, std::unique_ptr<CoreType> core,
                             std::unique_ptr<ReplicatedStateToken> token, std::shared_ptr<Factory> factory) noexcept;

        void run() noexcept override;

        [[nodiscard]] auto get_status() const -> StateStatus final;

        [[nodiscard]] auto getFollowerState() const -> std::shared_ptr<IReplicatedFollowerState<S>>;

        [[nodiscard]] auto resign() &&noexcept
            -> std::tuple<std::unique_ptr<CoreType>, std::unique_ptr<ReplicatedStateToken>, deferred_action> override;

        [[nodiscard]] auto getStream() const noexcept -> std::shared_ptr<Stream>;

        auto wait_for_applied(log_index) -> futures::Future<futures::Unit>;

    private:
        void awaitLeaderShip();
        void ingestLogData();

        template<typename F>
        auto poll_new_entries(F &&fn);
        void checkSnapshot(std::shared_ptr<IReplicatedFollowerState<S>>);
        void tryTransferSnapshot(std::shared_ptr<IReplicatedFollowerState<S>>);
        void startService(std::shared_ptr<IReplicatedFollowerState<S>>);
        void retryTransferSnapshot(std::shared_ptr<IReplicatedFollowerState<S>>, std::uint64_t retryCount);

        void apply_entries(std::unique_ptr<Iterator> iter) noexcept;

        using Demultiplexer = streams::LogDemultiplexer<ReplicatedStateStreamSpec<S>>;

        struct guarded_data {
            FollowerStateManager &self;
            log_index _nextWaitForIndex {1};
            std::shared_ptr<Stream> stream;
            std::shared_ptr<IReplicatedFollowerState<S>> state;

            follower_internal_state internalState {follower_internal_state::kUninitializedState};
            std::chrono::system_clock::time_point lastInternalStateChange;
            std::optional<log_range> ingestionRange;
            std::optional<Result> lastError;
            std::uint64_t errorCounter {0};

            // core will be nullptr as soon as the FollowerState was created
            std::unique_ptr<CoreType> core;
            std::unique_ptr<ReplicatedStateToken> token;
            wait_for_appliedQueue wait_for_appliedQueue;

            bool _didResign = false;

            guarded_data(FollowerStateManager &self, std::unique_ptr<CoreType> core,
                        std::unique_ptr<ReplicatedStateToken> token);
            void updateInternalState(follower_internal_state newState, std::optional<log_range> range = std::nullopt);
            void updateInternalState(follower_internal_state newState, Result);
            auto updateNextIndex(log_index nextWaitForIndex) -> deferred_action;
        };

        void handle_poll_result(futures::Future<std::unique_ptr<Iterator>> &&pollFuture);
        void handleAwaitLeadershipResult(futures::Future<log::wait_for_result> &&);

        Guarded<guarded_data> _guarded_data;
        std::weak_ptr<replicated_state_base> const parent;
        std::shared_ptr<log::ILogFollower> const logFollower;
        std::shared_ptr<Factory> const factory;
        logger_context const loggerContext;
    };
}    // namespace nil::dbms::replication::state
