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

#include <basics/guarded.h>
#include <containers/immer_memory_policy.h>
#include <chrono>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "basics/result.h"
#include "futures/Future.h"
#include <nil/dbms/replication/logger_context.hpp>
#include "agency_log_specification.hpp"
#include "ilog_interfaces.hpp"
#include "inmemory_log.hpp"
#include "log_common.hpp"
#include "log_core.hpp"
#include "log_status.hpp"
#include "network_messages.hpp"
#include "wait_for_bag.hpp"
#include "types.hpp"

namespace nil::dbms {
    struct deferred_action;
}    // namespace nil::dbms

#if (_MSC_VER >= 1)
// suppress warnings:
#pragma warning(push)
// conversion from 'size_t' to 'immer::detail::rbts::count_t', possible loss of
// data
#pragma warning(disable : 4267)
// result of 32-bit shift implicitly converted to 64 bits (was 64-bit shift
// intended?)
#pragma warning(disable : 4334)
#endif
#include <immer/flex_vector.hpp>
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

namespace nil::dbms::futures {
    template<typename T>
    class Try;
}
namespace nil::dbms::cluster {
    struct IFailureOracle;
}
namespace nil::dbms::replication::algorithms {
    struct participant_state;
}
namespace nil::dbms::replication::replicated_log {
    struct log_core;
    struct replicated_log_metrics;
}    // namespace nil::dbms::replication::replicated_log

namespace nil::dbms::replication::replicated_log {

    /**
     * @brief Leader instance of a replicated log.
     */
    class log_leader : public std::enable_shared_from_this<log_leader>, public ilog_leader {
    public:
        ~log_leader() override;

        [[nodiscard]] static auto construct(agency::log_plan_config config, std::unique_ptr<log_core> logCore,
                                            std::vector<std::shared_ptr<abstract_follower>> const &followers,
                                            std::shared_ptr<agency::participants_config const> participantsConfig,
                                            ParticipantId id, log_term term, logger_context const &logContext,
                                            std::shared_ptr<replicated_log_metrics> logMetrics,
                                            std::shared_ptr<replicated_log_global_settings const> options,
                                            std::shared_ptr<cluster::IFailureOracle const> failureOracle)
            -> std::shared_ptr<log_leader>;

        auto insert(log_payload payload, bool waitForSync = false) -> log_index override;

        // As opposed to the above insert methods, this one does not trigger the async
        // replication automatically, i.e. does not call triggerAsyncReplication after
        // the insert into the in-memory log. This is necessary for testing. It should
        // not be necessary in production code. It might seem useful for batching, but
        // in that case, it'd be even better to add an insert function taking a batch.
        //
        // This method will however not prevent the resulting log entry from being
        // replicated, if async replication is running in the background already, or
        // if it is triggered by someone else.
        auto insert(log_payload payload, bool waitForSync, DoNotTriggerAsyncReplication) -> log_index override;

        [[nodiscard]] auto waitFor(log_index) -> WaitForFuture override;

        [[nodiscard]] auto waitForIterator(log_index index) -> WaitForIteratorFuture override;

        [[nodiscard]] auto getReplicatedLogSnapshot() const -> in_memory_log::log_type;

        [[nodiscard]] auto readReplicatedEntryByIndex(log_index idx) const -> std::optional<persisting_log_entry>;

        // Triggers sending of appendEntries requests to all followers. This continues
        // until all participants are perfectly in sync, and will then stop.
        // Is usually called automatically after an insert, but can be called manually
        // from test code.
        auto triggerAsyncReplication() -> void override;

        [[nodiscard]] auto getStatus() const -> log_status override;

        [[nodiscard]] auto getQuickStatus() const -> quick_log_status override;

        [[nodiscard]] auto resign() && -> std::tuple<std::unique_ptr<log_core>, deferred_action> override;

        [[nodiscard]] auto getParticipantId() const noexcept -> ParticipantId const &;

        [[nodiscard]] auto release(log_index doneWithIdx) -> Result override;

        [[nodiscard]] auto copyInMemoryLog() const -> in_memory_log override;

        // Returns true if the leader has established its leadership: at least one
        // entry within its term has been committed.
        [[nodiscard]] auto isLeadershipEstablished() const noexcept -> bool override;

        auto waitForLeadership() -> WaitForFuture override;

        [[nodiscard]] auto waitForResign() -> futures::Future<futures::Unit> override;

        // This function returns the current commit index. Do NOT poll this function,
        // use waitFor(idx) instead. This function is used in tests.
        [[nodiscard]] auto getCommitIndex() const noexcept -> log_index override;

        // Updates the flags of the participants.
        auto updateParticipantsConfig(
            std::shared_ptr<agency::participants_config const> const &config,
            std::function<std::shared_ptr<replicated_log::abstract_follower>(ParticipantId const &)> const
                &buildFollower) -> log_index;

        // Returns [acceptedConfig.generation, committedConfig.?generation]
        auto getParticipantConfigGenerations() const noexcept -> std::pair<std::size_t, std::optional<std::size_t>>;

    protected:
        // Use the named constructor construct() to create a leader!
        log_leader(logger_context logContext, std::shared_ptr<replicated_log_metrics> logMetrics,
                   std::shared_ptr<replicated_log_global_settings const> options, agency::log_plan_config config,
                   ParticipantId id, log_term term, log_index firstIndexOfCurrentTerm, in_memory_log inMemoryLog,
                   std::shared_ptr<cluster::IFailureOracle const> failureOracle);

    private:
        struct guarded_leader_data;

        using Guard = MutexGuard<guarded_leader_data, std::unique_lock<std::mutex>>;
        using ConstGuard = MutexGuard<guarded_leader_data const, std::unique_lock<std::mutex>>;

        struct alignas(64) follower_info {
            explicit follower_info(std::shared_ptr<abstract_follower> impl,
                                   term_index_pair lastLogIndex,
                                   logger_context const &logContext);

            std::chrono::steady_clock::duration _lastRequestLatency {};
            std::chrono::steady_clock::time_point _lastRequestStartTP {};
            std::chrono::steady_clock::time_point _errorBackoffEndTP {};
            std::shared_ptr<abstract_follower> _impl;
            term_index_pair lastAckedEntry = term_index_pair {log_term {0}, log_index {0}};
            log_index lastAckedCommitIndex = log_index {0};
            log_index lastAckedLowestIndexToKeep = log_index {0};
            message_id lastSentMessageId {0};
            std::size_t numErrorsSinceLastAnswer = 0;
            append_entries_error_reason lastErrorReason;
            logger_context const logContext;

            enum class State {
                IDLE,
                PREPARE,
                ERROR_BACKOFF,
                REQUEST_IN_FLIGHT,
            } _state = State::IDLE;
        };

        struct local_follower final : abstract_follower {
            // The LocalFollower assumes that the last entry of log core matches
            // lastIndex.
            local_follower(log_leader &self, logger_context logContext, std::unique_ptr<log_core> logCore,
                           term_index_pair lastIndex);
            ~local_follower() override = default;

            local_follower(local_follower const &) = delete;
            local_follower(local_follower &&) noexcept = delete;
            auto operator=(local_follower const &) -> local_follower & = delete;
            auto operator=(local_follower &&) noexcept -> local_follower & = delete;

            [[nodiscard]] auto getParticipantId() const noexcept -> ParticipantId const & override;
            [[nodiscard]] auto appendEntries(append_entries_request request)
                -> nil::dbms::futures::Future<append_entries_result> override;

            [[nodiscard]] auto resign() &&noexcept -> std::unique_ptr<log_core>;
            [[nodiscard]] auto release(log_index stop) const -> Result;

        private:
            log_leader &_leader;
            logger_context const _logContext;
            Guarded<std::unique_ptr<log_core>> _guardedLogCore;
        };

        struct prepared_append_entry_request {
            prepared_append_entry_request() = delete;
            prepared_append_entry_request(std::shared_ptr<log_leader> const &logLeader,
                                          std::shared_ptr<follower_info>
                                              follower,
                                          std::chrono::steady_clock::duration executionDelay);

            std::weak_ptr<log_leader> _parentLog;
            std::weak_ptr<follower_info> _follower;
            std::chrono::steady_clock::duration _executionDelay;
        };

        struct resolved_promise_set {
            WaitForQueue _set;
            wait_for_result result;
            ::immer::flex_vector<in_memory_log_entry, nil::dbms::immer::dbms_memory_policy> _commitedLogEntries;
        };

        struct alignas(128) guarded_leader_data {
            ~guarded_leader_data() = default;
            guarded_leader_data(log_leader &self, in_memory_log inMemoryLog);

            guarded_leader_data() = delete;
            guarded_leader_data(guarded_leader_data const &) = delete;
            guarded_leader_data(guarded_leader_data &&) = delete;
            auto operator=(guarded_leader_data const &) -> guarded_leader_data & = delete;
            auto operator=(guarded_leader_data &&) -> guarded_leader_data & = delete;

            [[nodiscard]] auto prepare_append_entry(std::shared_ptr<follower_info> follower)
                -> std::optional<prepared_append_entry_request>;
            [[nodiscard]] auto prepareAppendEntries() -> std::vector<std::optional<prepared_append_entry_request>>;

            [[nodiscard]] auto
                handleAppendEntriesResponse(follower_info &follower, term_index_pair lastIndex,
                                            log_index currentCommitIndex, log_index currentLITK, log_term currentTerm,
                                            futures::Try<append_entries_result> &&res,
                                            std::chrono::steady_clock::duration latency, message_id messageId)
                    -> std::pair<std::vector<std::optional<prepared_append_entry_request>>, resolved_promise_set>;

            [[nodiscard]] auto checkCommitIndex() -> resolved_promise_set;

            [[nodiscard]] auto collectFollowerStates() const
                -> std::pair<log_index, std::vector<algorithms::participant_state>>;
            [[nodiscard]] auto checkCompaction() -> Result;

            [[nodiscard]] auto updateCommitIndexLeader(log_index newIndex, std::shared_ptr<quorum_data> quorum)
                -> resolved_promise_set;

            [[nodiscard]] auto getInternalLogIterator(log_index firstIdx) const
                -> std::unique_ptr<typed_log_iterator<in_memory_log_entry>>;

            [[nodiscard]] auto getCommittedLogIterator(log_index firstIndex) const -> std::unique_ptr<LogRangeIterator>;

            [[nodiscard]] auto getLocalStatistics() const -> log_statistics;

            [[nodiscard]] auto createAppendEntriesRequest(follower_info &follower,
                                                          term_index_pair const &lastAvailableIndex) const
                -> std::pair<append_entries_request, term_index_pair>;

            [[nodiscard]] auto calculateCommitLag() const noexcept -> std::chrono::duration<double, std::milli>;

            auto insertInternal(std::variant<log_meta_payload, log_payload>, bool waitForSync,
                                std::optional<in_memory_log_entry::clock::time_point> insertTp) -> log_index;

            [[nodiscard]] auto waitForResign() -> std::pair<futures::Future<futures::Unit>, deferred_action>;

            log_leader &_self;
            in_memory_log _inMemoryLog;
            std::unordered_map<ParticipantId, std::shared_ptr<follower_info>> _follower {};
            WaitForQueue _waitForQueue {};
            wait_for_bag _waitForResignQueue;
            std::shared_ptr<quorum_data> _lastQuorum {};
            log_index _commitIndex {0};
            log_index _lowestIndexToKeep {0};
            log_index _releaseIndex {0};
            bool _didResign {false};
            bool _leadershipEstablished {false};
            commit_fail_reason _lastCommitFailReason;

            // active - that is currently used to check for committed entries
            std::shared_ptr<agency::participants_config const> activeParticipantsConfig;
            // committed - latest active config that has committed at least one entry
            // Note that this will be nullptr until leadership is established!
            std::shared_ptr<agency::participants_config const> committedParticipantsConfig;
        };

        logger_context const _logContext;
        std::shared_ptr<replicated_log_metrics> const _logMetrics;
        std::shared_ptr<replicated_log_global_settings const> const _options;
        std::shared_ptr<cluster::IFailureOracle const> const _failureOracle;
        agency::log_plan_config const _config;
        ParticipantId const _id;
        log_term const _currentTerm;
        log_index const _firstIndexOfCurrentTerm;
        // _localFollower is const after construction
        std::shared_ptr<local_follower> _localFollower;
        // make this thread safe in the most simple way possible, wrap everything in
        // a single mutex.
        Guarded<guarded_leader_data> _guardedLeaderData;

        void establishLeadership(std::shared_ptr<agency::participants_config const> config);

        [[nodiscard]] static auto instantiateFollowers(logger_context const &,
                                                       std::vector<std::shared_ptr<abstract_follower>> const &followers,
                                                       std::shared_ptr<local_follower> const &localFollower,
                                                       term_index_pair lastEntry)
            -> std::unordered_map<ParticipantId, std::shared_ptr<follower_info>>;

        auto acquireMutex() -> Guard;
        auto acquireMutex() const -> ConstGuard;

        static void executeAppendEntriesRequests(std::vector<std::optional<prepared_append_entry_request>> requests,
                                                 std::shared_ptr<replicated_log_metrics> const &logMetrics);
        static void handleResolvedPromiseSet(resolved_promise_set set,
                                             std::shared_ptr<replicated_log_metrics> const &logMetrics);
    };

}    // namespace nil::dbms::replication::replicated_log
