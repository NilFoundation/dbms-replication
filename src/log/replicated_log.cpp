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

#include "logger/LogContextKeys.h"
#include <nil/dbms/replication/exceptions/participant_resigned_exception.hpp>
#include <nil/dbms/replication/log/inmemory_log.hpp>
#include <nil/dbms/replication/log/log_core.hpp>
#include <nil/dbms/replication/log/log_follower.hpp>
#include "nil/dbms/replication/log/log.hpp"
#include <nil/dbms/replication/log/log_leader.hpp>
#include <nil/dbms/replication/log/log_unconfigured_participant.hpp>
#include <nil/dbms/replication/log/persisted_log.hpp>
#include <nil/dbms/metrics/counter.hpp>
#include <nil/dbms/cluster/failure_oracle.hpp>

#include <basics/exceptions.h>
#include <basics/voc_errors.h>

#include <optional>
#include <type_traits>

namespace nil::dbms::replication::log {
    struct AbstractFollower;
}

using namespace nil::dbms;
using namespace nil::dbms::replication;

log::ReplicatedLog::ReplicatedLog(std::unique_ptr<log_core> core,
                                             std::shared_ptr<ReplicatedLogMetrics> const &metrics,
                                             std::shared_ptr<ReplicatedLogGlobalSettings const>
                                                 options,
                                             logger_context const &logContext) :
    _log_id(core->gid()),
    _logContext(logContext.with<logContextKeylog_id>(core->log_id())),
    _participant(std::make_shared<LogUnconfiguredParticipant>(std::move(core), metrics)), _metrics(metrics),
    _options(std::move(options)) {
}

log::ReplicatedLog::~ReplicatedLog() {
    // If we have a participant, it must also hold a replicated log. The only way
    // to remove the log_core from the ReplicatedLog is via drop(), which also sets
    // _participant to nullptr.
    if (_participant != nullptr) {
        // resign returns a log_core and a deferred_action, which can be destroyed
        // immediately
        std::ignore = std::move(*_participant).resign();
    }
}

auto log::ReplicatedLog::becomeLeader(agency::log_plan_config config, ParticipantId id, log_term newTerm,
                                                 std::vector<std::shared_ptr<AbstractFollower>> const &follower,
                                                 std::shared_ptr<agency::participants_config const> participants_config,
                                                 std::shared_ptr<cluster::IFailureOracle const> failureOracle)
    -> std::shared_ptr<log_leader> {
    auto [leader, deferred] = std::invoke([&] {
        std::unique_lock guard(_mutex);
        if (auto currentTerm = _participant->getTerm(); currentTerm && *currentTerm > newTerm) {
            LOG_CTX("b8bf7", INFO, _logContext)
                << "tried to become leader with term " << newTerm << ", but current term is " << *currentTerm;
            THROW_DBMS_EXCEPTION(TRI_ERROR_REPLICATION_REPLICATED_LOG_INVALID_TERM);
        }

        auto [log_core, deferred] = std::move(*_participant).resign();
        LOG_CTX("23d7b", DEBUG, _logContext) << "becoming leader in term " << newTerm;
        auto leader = log_leader::construct(config, std::move(log_core), follower, participants_config, std::move(id),
                                           newTerm, _logContext, _metrics, _options, std::move(failureOracle));
        _participant = std::static_pointer_cast<ILogParticipant>(leader);
        _metrics->replicatedlog_leaderTookOverNumber->count();
        return std::make_pair(std::move(leader), std::move(deferred));
    });

    return leader;
}

auto log::ReplicatedLog::becomeFollower(ParticipantId id, log_term term,
                                                   std::optional<ParticipantId> leaderId)
    -> std::shared_ptr<LogFollower> {
    auto [follower, deferred] = std::invoke([&] {
        std::unique_lock guard(_mutex);
        if (auto currentTerm = _participant->getTerm(); currentTerm && *currentTerm > term) {
            LOG_CTX("c97e9", INFO, _logContext)
                << "tried to become follower with term " << term << ", but current term is " << *currentTerm;
            THROW_DBMS_EXCEPTION(TRI_ERROR_BAD_PARAMETER);
        }
        auto [log_core, deferred] = std::move(*_participant).resign();
        LOG_CTX("1ed24", DEBUG, _logContext)
            << "becoming follower in term " << term << " with leader " << leaderId.value_or("<none>");

        auto follower =
            LogFollower::construct(_logContext, _metrics, std::move(id), std::move(log_core), term, std::move(leaderId));
        _participant = std::static_pointer_cast<ILogParticipant>(follower);
        _metrics->replicatedLogStartedFollowingNumber->operator++();
        return std::make_tuple(follower, std::move(deferred));
    });
    return follower;
}

auto log::ReplicatedLog::getParticipant() const -> std::shared_ptr<ILogParticipant> {
    std::unique_lock guard(_mutex);
    if (_participant == nullptr) {
        throw log::participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_PARTICIPANT_GONE,
                                                           ADB_HERE);
    }

    return _participant;
}

auto log::ReplicatedLog::getLeader() const -> std::shared_ptr<log_leader> {
    auto log = getParticipant();
    if (auto leader = std::dynamic_pointer_cast<nil::dbms::replication::log::log_leader>(log);
        leader != nullptr) {
        return leader;
    } else {
        THROW_DBMS_EXCEPTION(TRI_ERROR_REPLICATION_REPLICATED_LOG_NOT_THE_LEADER);
    }
}

auto log::ReplicatedLog::getFollower() const -> std::shared_ptr<LogFollower> {
    auto log = getParticipant();
    if (auto follower = std::dynamic_pointer_cast<log::LogFollower>(log); follower != nullptr) {
        return follower;
    } else {
        THROW_DBMS_EXCEPTION(TRI_ERROR_REPLICATION_REPLICATED_LOG_NOT_THE_LEADER);
    }
}

auto log::ReplicatedLog::drop() -> std::unique_ptr<log_core> {
    auto [core, deferred] = std::invoke([&] {
        std::unique_lock guard(_mutex);
        auto res = std::move(*_participant).resign();
        _participant = nullptr;
        return res;
    });
    return std::move(core);
}

auto log::ReplicatedLog::getId() const noexcept -> log_id {
    return _log_id.id;
}

auto log::ReplicatedLog::getGloballog_id() const noexcept -> global_log_identifier const & {
    return _log_id;
}
