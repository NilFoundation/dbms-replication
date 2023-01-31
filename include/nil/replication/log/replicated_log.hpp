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
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/log_leader.hpp>
#include <nil/dbms/replication/log/log_metrics.hpp>
#include <nil/dbms/replication/log/agency_log_specification.hpp>

#include <iosfwd>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace nil::dbms::cluster {
    struct IFailureOracle;
}
namespace nil::dbms::replication::log {
    class LogFollower;
    class log_leader;
    struct AbstractFollower;
    struct log_core;
}    // namespace nil::dbms::replication::log

namespace nil::dbms::replication::log {

    /**
     * @brief Container for a replicated log. These are managed by the responsible
     * vocbase. Exactly one instance exists for each replicated log this server is a
     * participant of.
     *
     * It holds a single ILogParticipant; starting with a
     * LogUnconfiguredParticipant, this will usually be either a log_leader or a
     * LogFollower.
     *
     * The active participant is also responsible for the singular log_core of this
     * log, providing access to the physical log. The fact that only one log_core
     * exists, and only one participant has access to it, asserts that only the
     * active instance can write to (or read from) the physical log.
     *
     * ReplicatedLog is responsible for instantiating Participants, and moving the
     * log_core from the previous active participant to a new one. This happens in
     * becomeLeader and becomeFollower.
     *
     * A mutex must be used to make sure that moving the log_core from the old to
     * the new participant, and switching the participant pointer, happen
     * atomically.
     */
    struct alignas(64) ReplicatedLog {
        explicit ReplicatedLog(std::unique_ptr<log_core> core,
                               std::shared_ptr<ReplicatedLogMetrics> const &metrics,
                               std::shared_ptr<ReplicatedLogGlobalSettings const>
                                   options,
                               logger_context const &logContext);

        ~ReplicatedLog();

        ReplicatedLog() = delete;
        ReplicatedLog(ReplicatedLog const &) = delete;
        ReplicatedLog(ReplicatedLog &&) = delete;
        auto operator=(ReplicatedLog const &) -> ReplicatedLog & = delete;
        auto operator=(ReplicatedLog &&) -> ReplicatedLog & = delete;

        auto getId() const noexcept -> log_id;
        auto getGloballog_id() const noexcept -> global_log_identifier const &;
        auto becomeLeader(agency::log_plan_config config, ParticipantId id, log_term term,
                          std::vector<std::shared_ptr<AbstractFollower>> const &follower,
                          std::shared_ptr<agency::participants_config const> participants_config,
                          std::shared_ptr<cluster::IFailureOracle const> failureOracle) -> std::shared_ptr<log_leader>;
        auto becomeFollower(ParticipantId id, log_term term, std::optional<ParticipantId> leaderId)
            -> std::shared_ptr<LogFollower>;

        auto getParticipant() const -> std::shared_ptr<ILogParticipant>;

        auto getLeader() const -> std::shared_ptr<log_leader>;
        auto getFollower() const -> std::shared_ptr<LogFollower>;

        auto drop() -> std::unique_ptr<log_core>;

        template<typename F, std::enable_if_t<std::is_invocable_v<F, std::shared_ptr<log_leader>>> = 0,
                 typename R = std::invoke_result_t<F, std::shared_ptr<log_leader>>>
        auto executeIfLeader(F &&f) {
            auto leaderPtr = std::dynamic_pointer_cast<log_leader>(getParticipant());
            if constexpr (std::is_void_v<R>) {
                if (leaderPtr != nullptr) {
                    std::invoke(f, leaderPtr);
                }
            } else {
                if (leaderPtr != nullptr) {
                    return std::optional<R> {std::invoke(f, leaderPtr)};
                }
                return std::optional<R> {};
            }
        }

    private:
        global_log_identifier const _log_id;
        logger_context const _logContext = logger_context(Logger::replication);
        mutable std::mutex _mutex;
        std::shared_ptr<ILogParticipant> _participant;
        std::shared_ptr<ReplicatedLogMetrics> const _metrics;
        std::shared_ptr<ReplicatedLogGlobalSettings const> const _options;
    };

}    // namespace nil::dbms::replication::log
