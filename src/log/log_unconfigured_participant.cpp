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

#include <basics/exceptions.h>
#include <basics/voc_errors.h>

#include <nil/dbms/replication/log/log_unconfigured_participant.hpp>
#include <nil/dbms/replication/log/inmemory_log.hpp>
#include <nil/dbms/replication/log/log_core.hpp>
#include <nil/dbms/replication/log/log_status.hpp>
#include <nil/dbms/replication/log/log_metrics_declarations.hpp>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::log;

auto LogUnconfiguredParticipant::get_status() const -> log_status {
    return log_status {UnconfiguredStatus {}};
}

auto LogUnconfiguredParticipant::getQuickStatus() const -> quick_log_status {
    return quick_log_status {.role = participant_role::kUnconfigured};
}

LogUnconfiguredParticipant::LogUnconfiguredParticipant(std::unique_ptr<log_core> log_core,
                                                       std::shared_ptr<ReplicatedLogMetrics>
                                                           logMetrics) :
    _logMetrics(std::move(logMetrics)),
    _guarded_data(std::move(log_core)) {
    _logMetrics->replicatedLogInactiveNumber->fetch_add(1);
}

auto LogUnconfiguredParticipant::resign() && -> std::tuple<std::unique_ptr<log_core>, deferred_action> {
    return _guarded_data.doUnderLock([](auto &data) { return std::move(data).resign(); });
}

auto LogUnconfiguredParticipant::waitFor(log_index) -> ILogParticipant::WaitForFuture {
    THROW_DBMS_EXCEPTION(TRI_ERROR_REPLICATION_REPLICATED_LOG_UNCONFIGURED);
}

LogUnconfiguredParticipant::~LogUnconfiguredParticipant() {
    _logMetrics->replicatedLogInactiveNumber->fetch_sub(1);
}

auto LogUnconfiguredParticipant::waitForIterator(log_index index) -> ILogParticipant::WaitForIteratorFuture {
    TRI_ASSERT(false);
    THROW_DBMS_EXCEPTION(TRI_ERROR_REPLICATION_REPLICATED_LOG_UNCONFIGURED);
}

auto LogUnconfiguredParticipant::release(log_index doneWithIdx) -> Result {
    THROW_DBMS_EXCEPTION(TRI_ERROR_REPLICATION_REPLICATED_LOG_UNCONFIGURED);
}

auto LogUnconfiguredParticipant::getCommitIndex() const noexcept -> log_index {
    return log_index {0};    // index 0 is always committed.
}

LogUnconfiguredParticipant::guarded_data::guarded_data(std::unique_ptr<nil::dbms::replication::log::log_core>
                                                         log_core) :
    _log_core(std::move(log_core)) {
}

auto LogUnconfiguredParticipant::waitForResign() -> futures::Future<futures::Unit> {
    auto &&[future, action] = _guarded_data.getLockedGuard()->waitForResign();

    action.fire();

    return std::move(future);
}

auto LogUnconfiguredParticipant::copyin_memory_log() const -> in_memory_log {
    THROW_DBMS_EXCEPTION(TRI_ERROR_REPLICATION_REPLICATED_LOG_UNCONFIGURED);
}

auto LogUnconfiguredParticipant::guarded_data::resign()
    && -> std::tuple<std::unique_ptr<nil::dbms::replication::log::log_core>, nil::dbms::deferred_action> {
    auto queue = std::move(_waitForResignQueue);
    auto defer = deferred_action {[queue = std::move(queue)]() mutable noexcept { queue.resolveAll(); }};
    return std::make_tuple(std::move(_log_core), std::move(defer));
}

auto LogUnconfiguredParticipant::guarded_data::didResign() const noexcept -> bool {
    return _log_core == nullptr;
}

auto LogUnconfiguredParticipant::guarded_data::waitForResign()
    -> std::pair<futures::Future<futures::Unit>, deferred_action> {
    if (!didResign()) {
        auto future = _waitForResignQueue.addWaitFor();
        return {std::move(future), deferred_action {}};
    } else {
        TRI_ASSERT(_waitForResignQueue.empty());
        auto promise = futures::Promise<futures::Unit> {};
        auto future = promise.getFuture();

        auto action = deferred_action([promise = std::move(promise)]() mutable noexcept {
            TRI_ASSERT(promise.valid());
            promise.setValue();
        });

        return {std::move(future), std::move(action)};
    }
}
