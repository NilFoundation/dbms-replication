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

#include <map>

#include <nil/dbms/replication/state/state_token.hpp>
#include <nil/dbms/replication/state/state_traits.hpp>
#include <nil/dbms/replication/state/state_status.hpp>
#include "nil/dbms/replication/streams/streams.hpp"

#include "basics/guarded.h"
#include <nil/dbms/replication/deferred_execution.hpp>
#include <nil/dbms/replication/logger_context.hpp>

namespace nil::dbms::futures {
    template<typename T>
    class Future;
    template<typename T>
    class Promise;
    struct Unit;
}    // namespace nil::dbms::futures
namespace nil::dbms {
    class Result;
}
namespace nil::dbms::replication {
    namespace log {
        struct ReplicatedLog;
        struct ILogFollower;
        struct Ilog_leader;
        struct ILogParticipant;
        struct LogUnconfiguredParticipant;
    }    // namespace log

    namespace state {

        struct IReplicatedLeaderStateBase;
        struct IReplicatedFollowerStateBase;

        /**
         * Common base class for all ReplicatedStates, hiding the type information.
         */
        struct replicated_state_base {
            virtual ~replicated_state_base() = default;

            virtual void flush(StateGeneration plannedGeneration) = 0;
            virtual void start(std::unique_ptr<ReplicatedStateToken> token) = 0;
            virtual void forceRebuild() = 0;
            [[nodiscard]] virtual auto get_status() -> std::optional<StateStatus> = 0;
            [[nodiscard]] auto getLeader() -> std::shared_ptr<IReplicatedLeaderStateBase> {
                return get_leader_base();
            }
            [[nodiscard]] auto getFollower() -> std::shared_ptr<IReplicatedFollowerStateBase> {
                return get_follower_base();
            }

        private:
            [[nodiscard]] virtual auto get_leader_base() -> std::shared_ptr<IReplicatedLeaderStateBase> = 0;
            [[nodiscard]] virtual auto get_follower_base() -> std::shared_ptr<IReplicatedFollowerStateBase> = 0;
        };

        template<typename S>
        struct ReplicatedState final : replicated_state_base, std::enable_shared_from_this<ReplicatedState<S>> {
            using Factory = typename ReplicatedStateTraits<S>::FactoryType;
            using EntryType = typename ReplicatedStateTraits<S>::EntryType;
            using FollowerType = typename ReplicatedStateTraits<S>::FollowerType;
            using LeaderType = typename ReplicatedStateTraits<S>::LeaderType;
            using CoreType = typename ReplicatedStateTraits<S>::CoreType;

            explicit ReplicatedState(std::shared_ptr<log::ReplicatedLog> log,
                                     std::shared_ptr<Factory>
                                         factory,
                                     logger_context loggerContext);

            /**
             * Forces to rebuild the state machine depending on the replicated log state.
             */
            void flush(StateGeneration planGeneration) override;
            void start(std::unique_ptr<ReplicatedStateToken> token) override;

            /**
             * Returns the follower state machine. Returns nullptr if no follower state
             * machine is present. (i.e. this server is not a follower)
             */
            [[nodiscard]] auto getFollower() const -> std::shared_ptr<FollowerType>;
            /**
             * Returns the leader state machine. Returns nullptr if no leader state
             * machine is present. (i.e. this server is not a leader)
             */
            [[nodiscard]] auto getLeader() const -> std::shared_ptr<LeaderType>;

            [[nodiscard]] auto get_status() -> std::optional<StateStatus> final;

            /**
             * Rebuilds the managers. Called when the managers participant is gone.
             */
            void forceRebuild() override;

            struct IStateManager {
                virtual ~IStateManager() = default;
                virtual void run() = 0;

                using wait_for_appliedPromise = futures::Promise<futures::Unit>;
                using wait_for_appliedQueue = std::multimap<log_index, wait_for_appliedPromise>;

                [[nodiscard]] virtual auto get_status() const -> StateStatus = 0;
                [[nodiscard]] virtual auto resign() &&noexcept
                    -> std::tuple<std::unique_ptr<CoreType>, std::unique_ptr<ReplicatedStateToken>, deferred_action> = 0;
            };

        private:
            auto get_leader_base() -> std::shared_ptr<IReplicatedLeaderStateBase> final {
                return getLeader();
            }
            auto get_follower_base() -> std::shared_ptr<IReplicatedFollowerStateBase> final {
                return getFollower();
            }

            std::shared_ptr<Factory> const factory;
            std::shared_ptr<log::ReplicatedLog> const log {};

            struct guarded_data {
                auto forceRebuild() -> deferred_action;

                auto runLeader(std::shared_ptr<log::Ilog_leader> log_leader,
                               std::unique_ptr<CoreType>,
                               std::unique_ptr<ReplicatedStateToken>
                                   token) -> deferred_action;
                auto runFollower(std::shared_ptr<log::ILogFollower> logFollower,
                                 std::unique_ptr<CoreType>,
                                 std::unique_ptr<ReplicatedStateToken>
                                     token) -> deferred_action;
                auto
                    runUnconfigured(std::shared_ptr<log::LogUnconfiguredParticipant> unconfiguredParticipant,
                                    std::unique_ptr<CoreType>
                                        core,
                                    std::unique_ptr<ReplicatedStateToken>
                                        token) -> deferred_action;

                auto rebuild(std::unique_ptr<CoreType> core, std::unique_ptr<ReplicatedStateToken> token)
                    -> deferred_action;

                auto flush(StateGeneration planGeneration) -> deferred_action;

                explicit guarded_data(ReplicatedState &self) : _self(self) {
                }

                ReplicatedState &_self;
                std::shared_ptr<IStateManager> currentManager = nullptr;
            };
            Guarded<guarded_data> guarded_data;
            logger_context const loggerContext;
            DatabaseID const database;
        };

        template<typename S>
        using ReplicatedStateStreamSpec = streams::stream_descriptor_set<streams::stream_descriptor<
            streams::StreamId {1},
            typename ReplicatedStateTraits<S>::EntryType,
            streams::tag_descriptor_set<streams::tag_descriptor<1,
                                                                typename ReplicatedStateTraits<S>::Deserializer,
                                                                typename ReplicatedStateTraits<S>::Serializer>>>>;
    }    // namespace state
}    // namespace nil::dbms::replication
