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

#include <nil/replication_sdk/logger_context.hpp>
#include <nil/replication_sdk/replicated_log/ilog_interfaces.hpp>
#include <nil/replication_sdk/replicated_log/log_common.hpp>
#include <nil/replication_sdk/replicated_log/log_leader.hpp>
#include <nil/replication_sdk/replicated_log/replicated_log_metrics.hpp>
#include <nil/replication_sdk/replicated_log/agency_log_specification.hpp>

#include <iosfwd>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace nil::dbms::cluster {
    struct IFailureOracle;
}
namespace nil::dbms::replication_sdk::replicated_log {
    class log_follower;
    class log_leader;
    struct abstract_follower;
    struct log_core;
}    // namespace nil::dbms::replication_sdk::replicated_log

namespace nil::dbms::replication_sdk::replicated_log {

    /**
     * @brief Container for a replicated log. These are managed by the responsible
     * vocbase. Exactly one instance exists for each replicated log this server is a
     * participant of.
     *
     * It holds a single ILogParticipant; starting with a
     * LogUnconfiguredParticipant, this will usually be either a LogLeader or a
     * LogFollower.
     *
     * The active participant is also responsible for the singular LogCore of this
     * log, providing access to the physical log. The fact that only one LogCore
     * exists, and only one participant has access to it, asserts that only the
     * active instance can write to (or read from) the physical log.
     *
     * ReplicatedLog is responsible for instantiating Participants, and moving the
     * LogCore from the previous active participant to a new one. This happens in
     * become_leader and become_follower.
     *
     * A mutex must be used to make sure that moving the LogCore from the old to
     * the new participant, and switching the participant pointer, happen
     * atomically.
     */
    struct alignas(64) replicated_log_t {
        explicit replicated_log_t(std::unique_ptr<log_core> core,
                               std::shared_ptr<replicated_log_metrics> const &metrics,
                               std::shared_ptr<replicated_log_global_settings const>
                                   options,
                               logger_context const &logContext);

        ~replicated_log_t();

        replicated_log_t() = delete;
        replicated_log_t(replicated_log_t const &) = delete;
        replicated_log_t(replicated_log_t &&) = delete;
        auto operator=(replicated_log_t const &) -> replicated_log_t & = delete;
        auto operator=(replicated_log_t &&) -> replicated_log_t & = delete;

        auto get_id() const noexcept -> LogId;
        auto get_global_log_id() const noexcept -> global_log_identifier const &;
        auto become_leader(agency::log_plan_config config, ParticipantId id, log_term term,
                          std::vector<std::shared_ptr<abstract_follower>> const &follower,
                          std::shared_ptr<agency::participants_config const> participantsConfig,
                          std::shared_ptr<cluster::IFailureOracle const> failureOracle) -> std::shared_ptr<log_leader>;
        auto become_follower(ParticipantId id, log_term term, std::optional<ParticipantId> leaderId)
            -> std::shared_ptr<log_follower>;

        auto get_participant() const -> std::shared_ptr<ilog_participant>;

        auto get_leader() const -> std::shared_ptr<log_leader>;
        auto get_follower() const -> std::shared_ptr<log_follower>;

        auto drop() -> std::unique_ptr<log_core>;

        template<typename F, std::enable_if_t<std::is_invocable_v<F, std::shared_ptr<log_leader>>> = 0,
                 typename R = std::invoke_result_t<F, std::shared_ptr<log_leader>>>
        auto execute_if_leader(F &&f) {
            auto leaderPtr = std::dynamic_pointer_cast<log_leader>(get_participant());
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
        global_log_identifier const _logId;
        logger_context const _logContext = logger_context(Logger::REPLICATION2);
        mutable std::mutex _mutex;
        std::shared_ptr<ilog_participant> _participant;
        std::shared_ptr<replicated_log_metrics> const _metrics;
        std::shared_ptr<replicated_log_global_settings const> const _options;
    };

}    // namespace nil::dbms::replication_sdk::replicated_log
