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

namespace nil::dbms::replication_sdk::replicated_state {
    template<typename S>
    struct follower_state_manager : replicated_state_t<S>::is_state_manager,
                                  std::enable_shared_from_this<follower_state_manager<S>> {
        using Factory = typename replicated_state_traits<S>::FactoryType;
        using EntryType = typename replicated_state_traits<S>::EntryType;
        using FollowerType = typename replicated_state_traits<S>::FollowerType;
        using LeaderType = typename replicated_state_traits<S>::LeaderType;
        using CoreType = typename replicated_state_traits<S>::CoreType;
        using WaitForAppliedQueue = typename replicated_state_t<S>::is_state_manager::WaitForAppliedQueue;
        using WaitForAppliedPromise = typename replicated_state_t<S>::is_state_manager::WaitForAppliedPromise;

        using Stream = streams::Stream<EntryType>;
        using Iterator = typename Stream::Iterator;

        follower_state_manager(logger_context loggerContext, std::shared_ptr<replicated_state_base> parent,
                             std::shared_ptr<replicated_log::ilog_follower> logFollower, std::unique_ptr<CoreType> core,
                             std::unique_ptr<replicated_state_token> token, std::shared_ptr<Factory> factory) noexcept;

        void run() noexcept override;

        [[nodiscard]] auto getStatus() const -> state_status final;

        [[nodiscard]] auto getFollowerState() const -> std::shared_ptr<ireplicated_follower_state<S>>;

        [[nodiscard]] auto resign() &&noexcept
            -> std::tuple<std::unique_ptr<CoreType>, std::unique_ptr<replicated_state_token>, deferred_action> override;

        [[nodiscard]] auto getStream() const noexcept -> std::shared_ptr<Stream>;

        auto waitForApplied(log_index) -> futures::Future<futures::Unit>;

    private:
        void awaitLeaderShip();
        void ingestLogData();

        template<typename F>
        auto pollNewEntries(F &&fn);
        void checkSnapshot(std::shared_ptr<ireplicated_follower_state<S>>);
        void tryTransferSnapshot(std::shared_ptr<ireplicated_follower_state<S>>);
        void startService(std::shared_ptr<ireplicated_follower_state<S>>);
        void retryTransferSnapshot(std::shared_ptr<ireplicated_follower_state<S>>, std::uint64_t retryCount);

        void apply_entries(std::unique_ptr<Iterator> iter) noexcept;

        using Demultiplexer = streams::log_demultiplexer<ReplicatedStateStreamSpec<S>>;

        struct guarded_data {
            follower_state_manager &self;
            log_index _nextWaitForIndex {1};
            std::shared_ptr<Stream> stream;
            std::shared_ptr<ireplicated_follower_state<S>> state;

            follower_internal_state internalState {follower_internal_state::kUninitializedState};
            std::chrono::system_clock::time_point lastInternalStateChange;
            std::optional<log_range> ingestionRange;
            std::optional<Result> lastError;
            std::uint64_t errorCounter {0};

            // core will be nullptr as soon as the FollowerState was created
            std::unique_ptr<CoreType> core;
            std::unique_ptr<replicated_state_token> token;
            WaitForAppliedQueue waitForAppliedQueue;

            bool _didResign = false;

            guarded_data(follower_state_manager &self, std::unique_ptr<CoreType> core,
                        std::unique_ptr<replicated_state_token> token);
            void updateInternalState(follower_internal_state newState, std::optional<log_range> range = std::nullopt);
            void updateInternalState(follower_internal_state newState, Result);
            auto updateNextIndex(log_index nextWaitForIndex) -> deferred_action;
        };

        void handlePollResult(futures::Future<std::unique_ptr<Iterator>> &&pollFuture);
        void handleAwaitLeadershipResult(futures::Future<replicated_log::wait_for_result> &&);

        Guarded<guarded_data> _guardedData;
        std::weak_ptr<replicated_state_base> const parent;
        std::shared_ptr<replicated_log::ilog_follower> const logFollower;
        std::shared_ptr<Factory> const factory;
        logger_context const loggerContext;
    };
}    // namespace nil::dbms::replication_sdk::replicated_state
