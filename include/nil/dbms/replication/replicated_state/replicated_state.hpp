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

#include <nil/dbms/replication/cluster/cluster_types.hpp>
#include "replicated_state_token.hpp"
#include "replicated_state_traits.hpp"
#include <nil/dbms/replication/replicated_log/log_unconfigured_participant.hpp>
#include "state_status.hpp"
#include <nil/dbms/replication/streams/streams.hpp>

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
    namespace replicated_log {
        struct replicated_log_t;
        struct ilog_follower;
        struct ilog_leader;
        struct ilog_participant;
        struct LogUnconfiguredParticipant;
    }    // namespace replicated_log

    namespace replicated_state {

        struct ireplicated_leader_state_base;
        struct ireplicated_follower_state_base;

        /**
         * Common base class for all ReplicatedStates, hiding the type information.
         */
        struct replicated_state_base {
            virtual ~replicated_state_base() = default;

            virtual void flush(state_generation plannedGeneration) = 0;
            virtual void start(std::unique_ptr<replicated_state_token> token) = 0;
            virtual void forceRebuild() = 0;
            [[nodiscard]] virtual auto getStatus() -> std::optional<state_status> = 0;
            [[nodiscard]] auto getLeader() -> std::shared_ptr<ireplicated_leader_state_base> {
                return getLeaderBase();
            }
            [[nodiscard]] auto getFollower() -> std::shared_ptr<ireplicated_follower_state_base> {
                return getFollowerBase();
            }

        private:
            [[nodiscard]] virtual auto getLeaderBase() -> std::shared_ptr<ireplicated_leader_state_base> = 0;
            [[nodiscard]] virtual auto getFollowerBase() -> std::shared_ptr<ireplicated_follower_state_base> = 0;
        };

        template<typename S>
        struct replicated_state_t final : replicated_state_base, std::enable_shared_from_this<replicated_state_t<S>> {
            using Factory = typename replicated_state_traits<S>::FactoryType;
            using EntryType = typename replicated_state_traits<S>::EntryType;
            using FollowerType = typename replicated_state_traits<S>::FollowerType;
            using LeaderType = typename replicated_state_traits<S>::LeaderType;
            using CoreType = typename replicated_state_traits<S>::CoreType;

            explicit replicated_state_t(std::shared_ptr<replicated_log::replicated_log_t> log,
                                        std::shared_ptr<Factory>
                                            factory,
                                        logger_context loggerContext);

            /**
             * Forces to rebuild the state machine depending on the replicated log state.
             */
            void flush(state_generation planGeneration) override;
            void start(std::unique_ptr<replicated_state_token> token) override;

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

            [[nodiscard]] auto getStatus() -> std::optional<state_status> final;

            /**
             * Rebuilds the managers. Called when the managers participant is gone.
             */
            void forceRebuild() override;

            struct is_state_manager {
                virtual ~is_state_manager() = default;
                virtual void run() = 0;

                using WaitForAppliedPromise = futures::Promise<futures::Unit>;
                using WaitForAppliedQueue = std::multimap<log_index, WaitForAppliedPromise>;

                [[nodiscard]] virtual auto getStatus() const -> state_status = 0;
                [[nodiscard]] virtual auto resign() &&noexcept -> std::
                    tuple<std::unique_ptr<CoreType>, std::unique_ptr<replicated_state_token>, deferred_action> = 0;
            };

        private:
            auto getLeaderBase() -> std::shared_ptr<ireplicated_leader_state_base> final {
                return getLeader();
            }
            auto getFollowerBase() -> std::shared_ptr<ireplicated_follower_state_base> final {
                return getFollower();
            }

            std::shared_ptr<Factory> const factory;
            std::shared_ptr<replicated_log::replicated_log_t> const log {};

            struct guarded_data {
                auto forceRebuild() -> deferred_action;

                auto runLeader(std::shared_ptr<replicated_log::ilog_leader> logLeader,
                               std::unique_ptr<CoreType>,
                               std::unique_ptr<replicated_state_token>
                                   token) -> deferred_action;
                auto runFollower(std::shared_ptr<replicated_log::ilog_follower> logFollower,
                                 std::unique_ptr<CoreType>,
                                 std::unique_ptr<replicated_state_token>
                                     token) -> deferred_action;
                auto
                    runUnconfigured(std::shared_ptr<replicated_log::LogUnconfiguredParticipant> unconfiguredParticipant,
                                    std::unique_ptr<CoreType>
                                        core,
                                    std::unique_ptr<replicated_state_token>
                                        token) -> deferred_action;

                auto rebuild(std::unique_ptr<CoreType> core, std::unique_ptr<replicated_state_token> token)
                    -> deferred_action;

                auto flush(state_generation planGeneration) -> deferred_action;

                explicit guarded_data(replicated_state_t &self) : _self(self) {
                }

                replicated_state_t &_self;
                std::shared_ptr<is_state_manager> currentManager = nullptr;
            };
            Guarded<guarded_data> guardedData;
            logger_context const loggerContext;
            DatabaseID const database;
        };

        template<typename S>
        using ReplicatedStateStreamSpec = streams::stream_descriptor_set<streams::stream_descriptor<
            streams::StreamId {1},
            typename replicated_state_traits<S>::EntryType,
            streams::tag_descriptor_set<streams::tag_descriptor<1,
                                                                typename replicated_state_traits<S>::Deserializer,
                                                                typename replicated_state_traits<S>::Serializer>>>>;
    }    // namespace replicated_state
}    // namespace nil::dbms::replication
