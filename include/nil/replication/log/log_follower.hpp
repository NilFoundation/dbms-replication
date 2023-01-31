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

#include <nil/dbms/replication/logger_context.hpp>
#include <nil/dbms/replication/log/ilog_interfaces.hpp>
#include <nil/dbms/replication/log/inmemory_log.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/log_core.hpp>
#include <nil/dbms/replication/log/network_messages.hpp>
#include <nil/dbms/replication/log/log_metrics.hpp>
#include <nil/dbms/replication/log/wait_for_bag.hpp>
#include <nil/dbms/replication/log/types.hpp>

#include <basics/guarded.h>
#include <futures/Future.h>

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>

namespace nil::dbms::replication::log {

    /**
     * @brief Follower instance of a replicated log.
     */
    class LogFollower : public ILogFollower, public std::enable_shared_from_this<LogFollower> {
    public:
        ~LogFollower() override;
        static auto construct(logger_context const &, std::shared_ptr<ReplicatedLogMetrics> logMetrics, ParticipantId id,
                              std::unique_ptr<log_core> log_core, log_term term, std::optional<ParticipantId> leaderId)
            -> std::shared_ptr<LogFollower>;

        // follower only
        [[nodiscard]] auto appendEntries(AppendEntriesRequest) -> futures::Future<AppendEntriesResult> override;

        [[nodiscard]] auto get_status() const -> log_status override;
        [[nodiscard]] auto getQuickStatus() const -> quick_log_status override;
        [[nodiscard]] auto resign() && -> std::tuple<std::unique_ptr<log_core>, deferred_action> override;
        [[nodiscard]] auto getLeader() const noexcept -> std::optional<ParticipantId> const & override;

        [[nodiscard]] auto waitFor(log_index) -> WaitForFuture override;
        [[nodiscard]] auto waitForIterator(log_index index) -> WaitForIteratorFuture override;
        [[nodiscard]] auto waitForResign() -> futures::Future<futures::Unit> override;
        [[nodiscard]] auto getParticipantId() const noexcept -> ParticipantId const & override;
        [[nodiscard]] auto getLogIterator(log_index firstIndex) const -> std::unique_ptr<LogIterator>;
        [[nodiscard]] auto getCommittedLogIterator(log_index firstIndex) const -> std::unique_ptr<LogIterator>;
        [[nodiscard]] auto getCommitIndex() const noexcept -> log_index override;

        [[nodiscard]] auto copyin_memory_log() const -> in_memory_log override;
        [[nodiscard]] auto release(log_index doneWithIdx) -> Result override;

        /// @brief Resolved when the leader has committed at least one entry.
        auto waitForLeaderAcked() -> WaitForFuture override;

    private:
        LogFollower(logger_context const &, std::shared_ptr<ReplicatedLogMetrics> logMetrics, ParticipantId id,
                    std::unique_ptr<log_core> log_core, log_term term, std::optional<ParticipantId> leaderId,
                    in_memory_log inMemoryLog);

        struct GuardedFollowerData {
            GuardedFollowerData() = delete;
            GuardedFollowerData(LogFollower const &self, std::unique_ptr<log_core> log_core, in_memory_log inMemoryLog);

            [[nodiscard]] auto get_local_statistics() const noexcept -> LogStatistics;
            [[nodiscard]] auto getCommittedLogIterator(log_index firstIndex) const -> std::unique_ptr<log_rangeIterator>;
            [[nodiscard]] auto checkCompaction() -> Result;
            auto checkCommitIndex(log_index newCommitIndex, log_index newLITK,
                                  std::unique_ptr<WaitForQueue> outQueue) noexcept -> deferred_action;
            [[nodiscard]] auto didResign() const noexcept -> bool;

            [[nodiscard]] auto waitForResign() -> std::pair<futures::Future<futures::Unit>, deferred_action>;

            LogFollower const &_follower;
            in_memory_log _inMemoryLog;
            std::unique_ptr<log_core> _log_core;
            log_index _commitIndex {0};
            log_index _lowestIndexToKeep;
            log_index _releaseIndex;
            MessageId _lastRecvMessageId {0};
            Guarded<WaitForQueue, nil::dbms::basics::UnshackledMutex> _waitForQueue;
            WaitForBag _waitForResignQueue;
        };
        std::shared_ptr<ReplicatedLogMetrics> const _logMetrics;
        logger_context const _loggerContext;
        ParticipantId const _participantId;
        std::optional<ParticipantId> const _leaderId;
        log_term const _currentTerm;

        // We use the unshackled mutex because guards are captured by futures.
        // When using a std::mutex we would have to release the mutex in the same
        // thread. Using the UnshackledMutex this is no longer required.
        Guarded<GuardedFollowerData, nil::dbms::basics::UnshackledMutex> _guardedFollowerData;
        std::atomic<bool> _appendEntriesInFlight {false};
        std::condition_variable_any _appendEntriesInFlightCondVar {};

        [[nodiscard]] auto appendEntriesPreFlightChecks(GuardedFollowerData const &,
                                                        AppendEntriesRequest const &) const noexcept
            -> std::optional<AppendEntriesResult>;
    };

}    // namespace nil::dbms::replication::log
