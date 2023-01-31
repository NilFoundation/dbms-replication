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
#include <nil/dbms/metrics/gauge.hpp>
#include <nil/dbms/replication/log/log_follower.hpp>
#include <nil/dbms/replication/log/algorithms.hpp>
#include <nil/dbms/replication/log/log_status.hpp>
#include <nil/dbms/replication/log/network_messages.hpp>
#include <nil/dbms/replication/log/log_iterator.hpp>
#include <nil/dbms/replication/log/log_metrics.hpp>
#include <nil/dbms/replication/exceptions/participant_resigned_exception.hpp>

#include <basics/exceptions.h>
#include <basics/result.h>
#include <basics/string_utils.h>
#include <basics/debugging.h>
#include <basics/voc_errors.h>
#include <futures/Promise.h>

#include <basics/scope_guard.h>
#include <basics/application_exit.h>
#include <algorithm>

#include <utility>
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
#include <immer/flex_vector_transient.hpp>
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::log;

auto LogFollower::appendEntriesPreFlightChecks(GuardedFollowerData const &data,
                                               AppendEntriesRequest const &req) const noexcept
    -> std::optional<AppendEntriesResult> {
    if (data._log_core == nullptr) {
        // Note that a `ReplicatedLog` instance, when destroyed, will resign its
        // participant. This is intentional and has been thoroughly discussed to be
        // the preferable behavior in production, so no log_core can ever be "lost"
        // but still working in the background. It is expected to be unproblematic,
        // as the ReplicatedLogs are the entries in the central log registry in the
        // vocbase.
        // It is an easy pitfall in the tests, however, as it's easy to drop the
        // shared_ptr to the ReplicatedLog, and keep only the one to the
        // participant. In that case, the participant looses its log_core, which is
        // hard to find out. Thus we increase the log level for this message to make
        // this more visible.
#ifdef DBMS_USE_GOOGLE_TESTS
#define LOST_LOG_CORE_LOGLEVEL INFO
#else
#define LOST_LOG_CORE_LOGLEVEL DEBUG
#endif
        LOG_CTX("d290d", LOST_LOG_CORE_LOGLEVEL, _loggerContext) << "reject append entries - log core gone";
        return AppendEntriesResult::withRejection(_currentTerm, req.messageId,
                                                  {AppendEntriesErrorReason::ErrorType::kLostlog_core});
    }

    if (data._lastRecvMessageId >= req.messageId) {
        LOG_CTX("d291d", DEBUG, _loggerContext) << "reject append entries - message id out dated: " << req.messageId;
        return AppendEntriesResult::withRejection(_currentTerm, req.messageId,
                                                  {AppendEntriesErrorReason::ErrorType::kMessageOutdated});
    }

    if (_appendEntriesInFlight) {
        LOG_CTX("92282", DEBUG, _loggerContext) << "reject append entries - previous append entry still in flight";
        return AppendEntriesResult::withRejection(_currentTerm, req.messageId,
                                                  {AppendEntriesErrorReason::ErrorType::kPrevAppendEntriesInFlight});
    }

    if (req.leaderId != _leaderId) {
        LOG_CTX("a2009", DEBUG, _loggerContext) << "reject append entries - wrong leader, given = " << req.leaderId
                                                << " current = " << _leaderId.value_or("<none>");
        return AppendEntriesResult::withRejection(_currentTerm, req.messageId,
                                                  {AppendEntriesErrorReason::ErrorType::kInvalidLeaderId});
    }

    if (req.leaderTerm != _currentTerm) {
        LOG_CTX("dd7a3", DEBUG, _loggerContext)
            << "reject append entries - wrong term, given = " << req.leaderTerm << ", current = " << _currentTerm;
        return AppendEntriesResult::withRejection(_currentTerm, req.messageId,
                                                  {AppendEntriesErrorReason::ErrorType::kWrongTerm});
    }

    // It is always allowed to replace the log entirely
    if (req.prevLogEntry.index > log_index {0}) {
        if (auto conflict = algorithms::detectConflict(data._inMemoryLog, req.prevLogEntry); conflict.has_value()) {
            auto [reason, next] = *conflict;

            LOG_CTX("5971a", DEBUG, _loggerContext)
                << "reject append entries - prev log did not match: " << to_string(reason);
            return AppendEntriesResult::withConflict(_currentTerm, req.messageId, next);
        }
    }

    return std::nullopt;
}

auto log::LogFollower::appendEntries(AppendEntriesRequest req)
    -> nil::dbms::futures::Future<AppendEntriesResult> {
    MeasureTimeGuard measureTimeGuard {_logMetrics->replicatedLogFollowerAppendEntriesRtUs};

    auto dataGuard = _guardedFollowerData.getLockedGuard();

    {
        // Preflight checks - does the leader, log and other stuff match?
        // This code block should not modify the local state, only check values.
        if (auto result = appendEntriesPreFlightChecks(dataGuard.get(), req); result.has_value()) {
            return *result;
        }

        dataGuard->_lastRecvMessageId = req.messageId;
    }

    // In case of an exception, this scope guard sets the in flight flag to false.
    // _appendEntriesInFlight is an atomic variable, hence we are allowed to set
    // it without acquiring the mutex.
    //
    // _appendEntriesInFlight is set true, only if the _guardedFollowerData mutex
    // is locked. It is set to false precisely once by the scope guard below.
    // Setting it to false does not require the mutex.
    _appendEntriesInFlight = true;
    auto inFlightScopeGuard =
        ScopeGuard([&flag = _appendEntriesInFlight, &cv = _appendEntriesInFlightCondVar]() noexcept {
            flag = false;
            cv.notify_one();
        });

    {
        // Transactional Code Block
        // This code removes parts of the log and makes sure that
        // disk and in memory always agree. We first create the new state in memory
        // as a copy, then modify the log on disk. This is an atomic operation. If
        // it fails, we forget the new state. Otherwise we replace the old in memory
        // state with the new value.

        if (dataGuard->_inMemoryLog.getLastIndex() != req.prevLogEntry.index) {
            auto newInMemoryLog = dataGuard->_inMemoryLog.takeSnapshotUpToAndIncluding(req.prevLogEntry.index);
            auto res = dataGuard->_log_core->remove_back(req.prevLogEntry.index + 1);
            if (!res.ok()) {
                LOG_CTX("f17b8", ERR, _loggerContext)
                    << "failed to remove log entries after " << req.prevLogEntry.index;
                return AppendEntriesResult::withPersistenceError(_currentTerm, req.messageId, res);
            }

            // commit the deletion in memory
            static_assert(std::is_nothrow_move_assignable_v<decltype(newInMemoryLog)>);
            dataGuard->_inMemoryLog = std::move(newInMemoryLog);
        }
    }

    // If there are no new entries to be appended, we can simply update the commit
    // index and lci and return early.
    auto toBeResolved = std::make_unique<WaitForQueue>();
    if (req.entries.empty()) {
        auto action = dataGuard->checkCommitIndex(req.leaderCommit, req.lowestIndexToKeep, std::move(toBeResolved));
        auto result = AppendEntriesResult::withOk(dataGuard->_follower._currentTerm, req.messageId);
        dataGuard.unlock();    // unlock here, action must be executed after
        inFlightScopeGuard.fire();
        action.fire();
        static_assert(std::is_nothrow_move_constructible_v<AppendEntriesResult>);
        return {std::move(result)};
    }

    // Allocations
    auto newInMemoryLog = std::invoke([&] {
        // if prevlog_index is 0, we want to replace the entire log
        // Note that req.entries might not start at 1, because the log could be
        // compacted already.
        if (req.prevLogEntry.index == log_index {0}) {
            TRI_ASSERT(!req.entries.empty());
            LOG_CTX("14696", DEBUG, _loggerContext)
                << "replacing my log. New logs starts at " << req.entries.front().entry().log_term_index_pair() << ".";
            return InMemoryLog {req.entries};
        }
        return dataGuard->_inMemoryLog.append(_loggerContext, req.entries);
    });
    auto iter = std::make_unique<InMemorypersisted_logIterator>(req.entries);

    auto *core = dataGuard->_log_core.get();
    static_assert(std::is_nothrow_move_constructible_v<decltype(newInMemoryLog)>);

    auto checkResultAndCommitIndex =
        [self = shared_from_this(), inFlightGuard = std::move(inFlightScopeGuard), req = std::move(req),
         newInMemoryLog = std::move(newInMemoryLog), toBeResolved = std::move(toBeResolved)](
            futures::Try<Result> &&tryRes) mutable -> std::pair<AppendEntriesResult, deferred_action> {
        // We have to release the guard after this lambda is finished.
        // Otherwise it would be released when the lambda is destroyed, which
        // happens *after* the following thenValue calls have been executed. In
        // particular the lock is held until the end of the future chain is reached.
        // This will cause deadlocks.
        decltype(inFlightGuard) inFlightGuardLocal = std::move(inFlightGuard);
        auto data = self->_guardedFollowerData.getLockedGuard();

        auto const &res = tryRes.get();
        {
            // This code block does not throw any exceptions. This is executed after
            // we wrote to the on-disk-log.
            static_assert(noexcept(res.fail()));
            if (res.fail()) {
                LOG_CTX("216d8", ERR, data->_follower._loggerContext)
                    << "failed to insert log entries: " << res.errorMessage();
                return std::make_pair(
                    AppendEntriesResult::withPersistenceError(data->_follower._currentTerm, req.messageId, res),
                    deferred_action {});
            }

            // commit the write in memory
            static_assert(std::is_nothrow_move_assignable_v<decltype(newInMemoryLog)>);
            data->_inMemoryLog = std::move(newInMemoryLog);

            LOG_CTX("dd72d", TRACE, data->_follower._loggerContext)
                << "appended " << req.entries.size() << " log entries after " << req.prevLogEntry.index
                << ", leader commit index = " << req.leaderCommit;
        }

        auto action = data->checkCommitIndex(req.leaderCommit, req.lowestIndexToKeep, std::move(toBeResolved));

        static_assert(noexcept(AppendEntriesResult::withOk(data->_follower._currentTerm, req.messageId)));
        static_assert(std::is_nothrow_move_constructible_v<deferred_action>);
        return std::make_pair(AppendEntriesResult::withOk(data->_follower._currentTerm, req.messageId),
                              std::move(action));
    };
    static_assert(std::is_nothrow_move_constructible_v<decltype(checkResultAndCommitIndex)>);

    // Action
    auto f = core->insert_async(std::move(iter), req.waitForSync);
    // Release mutex here, otherwise we might deadlock in
    // checkResultAndCommitIndex if another request arrives before the previous
    // one was processed.
    dataGuard.unlock();
    return std::move(f)
        .then(std::move(checkResultAndCommitIndex))
        .then([measureTime = std::move(measureTimeGuard)](auto &&res) mutable {
            measureTime.fire();
            auto &&[result, action] = res.get();
            // It is okay to fire here, because commitToMemoryAndResolve has
            // released the guard already.
            action.fire();
            return std::move(result);
        });
}

auto log::LogFollower::GuardedFollowerData::checkCommitIndex(log_index newCommitIndex, log_index newLITK,
                                                                        std::unique_ptr<WaitForQueue> outQueue) noexcept
    -> deferred_action {
    TRI_ASSERT(outQueue != nullptr) << "expect outQueue to be preallocated";

    auto const generateToBeResolved = [&, this] {
        try {
            auto waitForQueue = _waitForQueue.getLockedGuard();

            auto const end = waitForQueue->upper_bound(_commitIndex);
            for (auto it = waitForQueue->begin(); it != end;) {
                LOG_CTX("69022", TRACE, _follower._loggerContext) << "resolving promise for index " << it->first;
                outQueue->insert(waitForQueue->extract(it++));
            }
            return deferred_action([commitIndex = _commitIndex, toBeResolved = std::move(outQueue)]() noexcept {
                for (auto &it : *toBeResolved) {
                    if (!it.second.isFulfilled()) {
                        // This only throws if promise was fulfilled earlier.
                        it.second.setValue(wait_for_result {commitIndex, std::shared_ptr<QuorumData> {}});
                    }
                }
            });
        } catch (std::exception const &e) {
            // If those promises are not fulfilled we can not continue.
            // Note that the move constructor of std::multi_map is not noexcept.
            LOG_CTX("e7a3d", FATAL, this->_follower._loggerContext)
                << "failed to fulfill replication promises due to exception; "
                   "system "
                   "can not continue. message: "
                << e.what();
            FATAL_ERROR_EXIT();
        } catch (...) {
            // If those promises are not fulfilled we can not continue.
            // Note that the move constructor of std::multi_map is not noexcept.
            LOG_CTX("c0bba", FATAL, _follower._loggerContext)
                << "failed to fulfill replication promises due to exception; "
                   "system can not continue";
            FATAL_ERROR_EXIT();
        }
    };

    if (_lowestIndexToKeep < newLITK) {
        LOG_CTX("fc467", TRACE, _follower._loggerContext)
            << "largest common index went from " << _lowestIndexToKeep << " to " << newLITK << ".";
        _lowestIndexToKeep = newLITK;
        // TODO do we want to call checkCompaction here?
        // std::ignore = checkCompaction();
    }

    if (_commitIndex < newCommitIndex && !_inMemoryLog.empty()) {
        _commitIndex = std::min(newCommitIndex, _inMemoryLog.back().entry().log_index());
        LOG_CTX("1641d", TRACE, _follower._loggerContext) << "increment commit index: " << _commitIndex;
        return generateToBeResolved();
    }

    return {};
}

auto log::LogFollower::GuardedFollowerData::didResign() const noexcept -> bool {
    return _log_core == nullptr;
}

log::LogFollower::GuardedFollowerData::GuardedFollowerData(LogFollower const &self,
                                                                      std::unique_ptr<log_core>
                                                                          log_core,
                                                                      InMemoryLog inMemoryLog) :
    _follower(self),
    _inMemoryLog(std::move(inMemoryLog)), _log_core(std::move(log_core)) {
}

auto log::LogFollower::get_status() const -> log_status {
    return _guardedFollowerData.doUnderLock([this](auto const &followerData) {
        if (followerData._log_core == nullptr) {
            throw participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED, ADB_HERE);
        }
        FollowerStatus status;
        status.local = followerData.get_local_statistics();
        status.leader = _leaderId;
        status.term = _currentTerm;
        status.lowestIndexToKeep = followerData._lowestIndexToKeep;
        return log_status {std::move(status)};
    });
}

auto log::LogFollower::getQuickStatus() const -> quick_log_status {
    return _guardedFollowerData.doUnderLock([this](auto const &followerData) {
        if (followerData._log_core == nullptr) {
            throw participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED, ADB_HERE);
        }
        constexpr auto kBaseIndex = log_index {0};
        return quick_log_status {.role = participant_role::kFollower,
                               .term = _currentTerm,
                               .local = followerData.get_local_statistics(),
                               .leadershipEstablished = followerData._commitIndex > kBaseIndex};
    });
}

auto log::LogFollower::getParticipantId() const noexcept -> ParticipantId const & {
    return _participantId;
}

auto log::LogFollower::resign() && -> std::tuple<std::unique_ptr<log_core>, deferred_action> {
    return _guardedFollowerData.doUnderLock([this](GuardedFollowerData &followerData) {
        LOG_CTX("838fe", DEBUG, _loggerContext) << "follower resign";
        if (followerData.didResign()) {
            LOG_CTX("55a1d", WARN, _loggerContext) << "follower log core is already gone. Resign was called twice!";
            basics::abortOrThrowException(
                participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED, ADB_HERE));
        }

        // use a unique ptr because move constructor for multimaps is not
        // noexcept
        struct Queues {
            WaitForQueue waitForQueue;
            WaitForBag waitForResignQueue;
        };
        auto queues = std::make_unique<Queues>();
        std::swap(queues->waitForQueue, followerData._waitForQueue.getLockedGuard().get());
        queues->waitForResignQueue = std::move(followerData._waitForResignQueue);

        auto action = [queues = std::move(queues)]() noexcept {
            std::for_each(queues->waitForQueue.begin(), queues->waitForQueue.end(), [](auto &pair) {
                pair.second.setException(
                    participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED, ADB_HERE));
            });
            queues->waitForResignQueue.resolveAll();
        };
        using action_type = decltype(action);

        static_assert(std::is_nothrow_move_constructible_v<action_type>);
        static_assert(std::is_nothrow_constructible_v<deferred_action, std::add_rvalue_reference_t<action_type>>);

        // make_tuple is noexcept, _log_core is a unique_ptr which is nothrow
        // move constructable
        return std::make_tuple(std::move(followerData._log_core), deferred_action {std::move(action)});
    });
}

log::LogFollower::LogFollower(logger_context const &logContext,
                                         std::shared_ptr<ReplicatedLogMetrics> logMetrics, ParticipantId id,
                                         std::unique_ptr<log_core> log_core, log_term term,
                                         std::optional<ParticipantId> leaderId,
                                         log::InMemoryLog inMemoryLog) :
    _logMetrics(std::move(logMetrics)),
    _loggerContext(logContext.with<logContextKeyLogComponent>("follower")
                       .with<logContextKeyLeaderId>(leaderId.value_or("<none>"))
                       .with<logContextKeyTerm>(term)),
    _participantId(std::move(id)), _leaderId(std::move(leaderId)), _currentTerm(term),
    _guardedFollowerData(*this, std::move(log_core), std::move(inMemoryLog)) {
    _logMetrics->replicatedLogFollowerNumber->fetch_add(1);
}

auto log::LogFollower::waitFor(log_index idx) -> log::ILogParticipant::WaitForFuture {
    auto self = _guardedFollowerData.getLockedGuard();
    if (self->didResign()) {
        auto promise = WaitForPromise {};
        promise.setException(
            participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED, ADB_HERE));
        return promise.getFuture();
    }
    if (self->_commitIndex >= idx) {
        return futures::Future<wait_for_result> {std::in_place, self->_commitIndex,
                                               std::make_shared<QuorumData>(idx, _currentTerm)};
    }
    // emplace might throw a std::bad_alloc but the remainder is noexcept
    // so either you inserted it and or nothing happens
    // TODO locking ok? Iterator stored but lock guard is temporary
    auto it = self->_waitForQueue.getLockedGuard()->emplace(idx, WaitForPromise {});
    auto &promise = it->second;
    auto future = promise.getFuture();
    TRI_ASSERT(future.valid());
    return future;
}

auto log::LogFollower::waitForIterator(log_index index)
    -> log::ILogParticipant::WaitForIteratorFuture {
    if (index == log_index {0}) {
        THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_BAD_PARAMETER, "invalid parameter; log index 0 is invalid");
    }
    return waitFor(index).thenValue([this, self = shared_from_this(), index](auto &&quorum) -> WaitForIteratorFuture {
        auto [fromIndex, iter] = _guardedFollowerData.doUnderLock(
            [&](GuardedFollowerData &followerData) -> std::pair<log_index, std::unique_ptr<log_rangeIterator>> {
                TRI_ASSERT(index <= followerData._commitIndex);

                /*
                 * This code here ensures that if only private log entries are present
                 * we do not reply with an empty iterator but instead wait for the
                 * next entry containing payload.
                 */

                auto actualIndex = std::max(index, followerData._inMemoryLog.getFirstIndex());
                while (actualIndex <= followerData._commitIndex) {
                    auto memtry = followerData._inMemoryLog.getEntryByIndex(actualIndex);
                    TRI_ASSERT(memtry.has_value())
                        << "first index is "
                        << followerData._inMemoryLog.getFirstIndex();    // should always have a value
                    if (!memtry.has_value()) {
                        break;
                    }
                    if (memtry->entry().hasPayload()) {
                        break;
                    }
                    actualIndex = actualIndex + 1;
                }

                if (actualIndex > followerData._commitIndex) {
                    return std::make_pair(actualIndex, nullptr);
                }

                return std::make_pair(actualIndex, followerData.getCommittedLogIterator(actualIndex));
            });

        // call here, otherwise we deadlock with waitFor
        if (iter == nullptr) {
            return waitForIterator(fromIndex);
        }

        return std::move(iter);
    });
}

auto log::LogFollower::getLogIterator(log_index firstIndex) const -> std::unique_ptr<LogIterator> {
    return _guardedFollowerData.doUnderLock([&](GuardedFollowerData const &data) -> std::unique_ptr<LogIterator> {
        auto const endIdx = data._inMemoryLog.getLastterm_index_pair().index + 1;
        TRI_ASSERT(firstIndex <= endIdx);
        return data._inMemoryLog.getIteratorFrom(firstIndex);
    });
}

auto log::LogFollower::getCommittedLogIterator(log_index firstIndex) const -> std::unique_ptr<LogIterator> {
    return _guardedFollowerData.doUnderLock([&](GuardedFollowerData const &data) -> std::unique_ptr<LogIterator> {
        return data.getCommittedLogIterator(firstIndex);
    });
}

auto log::LogFollower::GuardedFollowerData::getCommittedLogIterator(log_index firstIndex) const
    -> std::unique_ptr<log_rangeIterator> {
    auto const endIdx = _inMemoryLog.getNextIndex();
    TRI_ASSERT(firstIndex < endIdx);
    // return an iterator for the range [firstIndex, _commitIndex + 1)
    return _inMemoryLog.getIteratorRange(firstIndex, _commitIndex + 1);
}

log::LogFollower::~LogFollower() {
    _logMetrics->replicatedLogFollowerNumber->fetch_sub(1);
    if (auto queueEmpty = _guardedFollowerData.getLockedGuard()->_waitForQueue.getLockedGuard()->empty(); !queueEmpty) {
        TRI_ASSERT(false) << "expected wait-for-queue to be empty";
        LOG_CTX("ce7f8", ERR, _loggerContext) << "expected wait-for-queue to be empty";
    }
}

auto LogFollower::release(log_index doneWithIdx) -> Result {
    auto guard = _guardedFollowerData.getLockedGuard();
    if (guard->didResign()) {
        return {TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED};
    }
    guard.wait(_appendEntriesInFlightCondVar, [&] { return !_appendEntriesInFlight; });

    TRI_ASSERT(doneWithIdx <= guard->_inMemoryLog.getLastIndex());
    if (doneWithIdx <= guard->_releaseIndex) {
        return {};
    }
    guard->_releaseIndex = doneWithIdx;
    LOG_CTX("a0c95", TRACE, _loggerContext) << "new release index set to " << guard->_releaseIndex;
    return guard->checkCompaction();
}

auto LogFollower::waitForLeaderAcked() -> WaitForFuture {
    return waitFor(log_index {1});
}

auto LogFollower::getLeader() const noexcept -> std::optional<ParticipantId> const & {
    return _leaderId;
}

auto LogFollower::getCommitIndex() const noexcept -> log_index {
    return _guardedFollowerData.getLockedGuard()->_commitIndex;
}

auto LogFollower::waitForResign() -> futures::Future<futures::Unit> {
    auto &&[future, action] = _guardedFollowerData.getLockedGuard()->waitForResign();

    action.fire();

    return std::move(future);
}

auto LogFollower::construct(logger_context const &loggerContext, std::shared_ptr<ReplicatedLogMetrics> logMetrics,
                            ParticipantId id, std::unique_ptr<log_core> log_core, log_term term,
                            std::optional<ParticipantId> leaderId) -> std::shared_ptr<LogFollower> {
    auto log = InMemoryLog::loadFromlog_core(*log_core);

    auto const lastIndex = log.getLastterm_index_pair();

    if (lastIndex.term >= term) {
        LOG_CTX("2d80c", WARN, loggerContext)
            << "Becoming follower in term " << term << " but spearhead is already at term " << lastIndex.term;
    }

    struct MakeSharedWrapper : LogFollower {
        MakeSharedWrapper(logger_context const &loggerContext, std::shared_ptr<ReplicatedLogMetrics> logMetrics,
                          ParticipantId id, std::unique_ptr<log_core> log_core, log_term term,
                          std::optional<ParticipantId> leaderId, InMemoryLog inMemoryLog) :
            LogFollower(loggerContext, std::move(logMetrics), std::move(id), std::move(log_core), term,
                        std::move(leaderId), std::move(inMemoryLog)) {
        }
    };

    return std::make_shared<MakeSharedWrapper>(loggerContext, std::move(logMetrics), std::move(id), std::move(log_core),
                                               term, std::move(leaderId), std::move(log));
}
auto LogFollower::copyInMemoryLog() const -> InMemoryLog {
    return _guardedFollowerData.getLockedGuard()->_inMemoryLog;
}

auto log::LogFollower::GuardedFollowerData::get_local_statistics() const noexcept -> LogStatistics {
    auto result = LogStatistics {};
    result.commitIndex = _commitIndex;
    result.firstIndex = _inMemoryLog.getFirstIndex();
    result.spearHead = _inMemoryLog.getLastterm_index_pair();
    result.releaseIndex = _releaseIndex;
    return result;
}

auto LogFollower::GuardedFollowerData::checkCompaction() -> Result {
    auto const compactionStop = std::min(_lowestIndexToKeep, _releaseIndex + 1);
    LOG_CTX("080d5", TRACE, _follower._loggerContext) << "compaction index calculated as " << compactionStop;
    if (compactionStop <= _inMemoryLog.getFirstIndex() + 1000) {
        // only do a compaction every 1000 entries
        LOG_CTX("ebb9f", TRACE, _follower._loggerContext)
            << "won't trigger a compaction, not enough entries. First index = " << _inMemoryLog.getFirstIndex();
        return {};
    }

    auto newLog = _inMemoryLog.release(compactionStop);
    auto res = _log_core->remove_front(compactionStop).get();
    if (res.ok()) {
        _inMemoryLog = std::move(newLog);
    }
    LOG_CTX("f1028", TRACE, _follower._loggerContext) << "compaction result = " << res.errorMessage();
    return res;
}
auto LogFollower::GuardedFollowerData::waitForResign() -> std::pair<futures::Future<futures::Unit>, deferred_action> {
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