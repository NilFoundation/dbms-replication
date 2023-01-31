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

#include <nil/dbms/replication/log/log_leader.hpp>

#include <basics/exceptions.h>
#include <basics/guarded.h>
#include <basics/string_utils.h>
#include <basics/application_exit.h>
#include <basics/debugging.h>
#include <basics/system_compiler.h>
#include <basics/voc_errors.h>
#include <containers/immer_memory_policy.h>
#include <futures/Future.h>
#include <futures/Try.h>
#include <logger/LogMacros.h>
#include <logger/Logger.h>
#include <logger/LoggerStream.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <iterator>
#include <memory>
#include <ratio>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "basics/error_code.h"
#include <nil/dbms/cluster/failure_oracle.hpp>
#include "futures/Promise-inl.h"
#include "futures/Promise.h"
#include "futures/Unit.h"
#include "logger/LogContextKeys.h"
#include <nil/dbms/replication/deferred_execution.hpp>
#include <nil/dbms/replication/exceptions/participant_resigned_exception.hpp>
#include <nil/dbms/replication/log/algorithms.hpp>
#include <nil/dbms/replication/log/inmemory_log.hpp>
#include <nil/dbms/replication/log/log_core.hpp>
#include <nil/dbms/replication/log/log_status.hpp>
#include <nil/dbms/replication/log/persisted_log.hpp>
#include <nil/dbms/replication/log/log_iterator.hpp>
#include <nil/dbms/replication/log/log_metrics.hpp>
#include <nil/dbms/metrics/gauge.hpp>
#include <nil/dbms/metrics/histogram.hpp>
#include <nil/dbms/metrics/log_scale.hpp>
#include <nil/dbms/scheduler/scheduler_feature.hpp>
#include <nil/dbms/scheduler/supervised_scheduler.hpp>
#include "immer/detail/iterator_facade.hpp"
#include "immer/detail/rbts/rrbtree_iterator.hpp"

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

log::log_leader::log_leader(logger_context logContext, std::shared_ptr<ReplicatedLogMetrics> logMetrics,
                                     std::shared_ptr<ReplicatedLogGlobalSettings const> options,
                                     agency::log_plan_config config, ParticipantId id, log_term term, log_index firstIndex,
                                     InMemoryLog inMemoryLog,
                                     std::shared_ptr<cluster::IFailureOracle const> failureOracle) :
    _logContext(std::move(logContext)),
    _logMetrics(std::move(logMetrics)), _options(std::move(options)), _failureOracle(std::move(failureOracle)),
    _config(config), _id(std::move(id)), _currentTerm(term), _firstIndexOfCurrentTerm(firstIndex),
    _guardedLeaderData(*this, std::move(inMemoryLog)) {
    _logMetrics->replicatedlog_leaderNumber->fetch_add(1);
}

log::log_leader::~log_leader() {
    _logMetrics->replicatedlog_leaderNumber->fetch_sub(1);
    if (auto queueEmpty = _guardedLeaderData.getLockedGuard()->_waitForQueue.empty(); !queueEmpty) {
        TRI_ASSERT(false) << "expected wait-for-queue to be empty";
        LOG_CTX("ce7f7", ERR, _logContext) << "expected wait-for-queue to be empty";
    }
}

auto log::log_leader::instantiateFollowers(logger_context const &logContext,
                                                     std::vector<std::shared_ptr<AbstractFollower>> const &followers,
                                                     std::shared_ptr<LocalFollower> const &localFollower,
                                                     term_index_pair lastEntry)
    -> std::unordered_map<ParticipantId, std::shared_ptr<FollowerInfo>> {
    auto initLastIndex = lastEntry.index.saturatedDecrement();

    std::unordered_map<ParticipantId, std::shared_ptr<FollowerInfo>> followers_map;
    followers_map.reserve(followers.size() + 1);
    followers_map.emplace(localFollower->getParticipantId(),
                          std::make_shared<FollowerInfo>(localFollower, lastEntry, logContext));
    for (auto const &impl : followers) {
        auto const &[it, inserted] = followers_map.emplace(
            impl->getParticipantId(),
            std::make_shared<FollowerInfo>(impl, term_index_pair {log_term {0}, initLastIndex}, logContext));
        TRI_ASSERT(inserted) << "duplicate participant id: " << impl->getParticipantId();
    }
    return followers_map;
}

namespace {
    auto delayedFuture(std::chrono::steady_clock::duration duration) -> futures::Future<futures::Unit> {
        if (SchedulerFeature::SCHEDULER) {
            return SchedulerFeature::SCHEDULER->delay(duration);
        }

        // std::this_thread::sleep_for(duration);
        return futures::Future<futures::Unit> {std::in_place};
    }
}    // namespace

void log::log_leader::handleResolvedPromiseSet(ResolvedPromiseSet resolvedPromises,
                                                         std::shared_ptr<ReplicatedLogMetrics> const &logMetrics) {
    auto const commitTp = InMemoryLogEntry::clock::now();
    for (auto const &it : resolvedPromises._commitedLogEntries) {
        using namespace std::chrono_literals;
        auto const entryDuration = commitTp - it.insertTp();
        logMetrics->replicatedLogInsertsRtt->count(entryDuration / 1us);
    }

    for (auto &promise : resolvedPromises._set) {
        TRI_ASSERT(promise.second.valid());
        promise.second.setValue(resolvedPromises.result);
    }
}

void log::log_leader::execute_append_entries_requests(std::vector<std::optional<PreparedAppendEntryRequest>>
                                                                 requests,
                                                             std::shared_ptr<ReplicatedLogMetrics> const &logMetrics) {
    for (auto &it : requests) {
        if (it.has_value()) {
            delayedFuture(it->_executionDelay).thenFinal([it = std::move(it), logMetrics](auto &&) mutable {
                auto follower = it->_follower.lock();
                auto log_leader = it->_parentLog.lock();
                if (log_leader == nullptr || follower == nullptr) {
                    LOG_TOPIC("de312", TRACE, Logger::replication) << "parent log already gone, not sending any more "
                                                                       "AppendEntryRequests";
                    return;
                }

                auto [request, lastIndex] = log_leader->_guardedLeaderData.doUnderLock([&](auto const &self) {
                    auto lastAvailableIndex = self._inMemoryLog.getLastterm_index_pair();
                    LOG_CTX("71801", TRACE, follower->logContext)
                        << "last acked index = " << follower->lastAckedEntry
                        << ", current index = " << lastAvailableIndex
                        << ", last acked commit index = " << follower->lastAckedCommitIndex
                        << ", current commit index = " << self._commitIndex
                        << ", last acked litk = " << follower->lastAckedLowestIndexToKeep
                        << ", current litk = " << self._lowestIndexToKeep;
                    // We can only get here if there is some new information
                    // for this follower
                    TRI_ASSERT(follower->lastAckedEntry.index != lastAvailableIndex.index ||
                               self._commitIndex != follower->lastAckedCommitIndex ||
                               self._lowestIndexToKeep != follower->lastAckedLowestIndexToKeep);

                    return self.createAppendEntriesRequest(*follower, lastAvailableIndex);
                });

                auto messageId = request.messageId;
                LOG_CTX("1b0ec", TRACE, follower->logContext) << "sending append entries, messageId = " << messageId;

                // We take the start time here again to have a more precise
                // measurement. (And do not use follower._lastRequestStartTP)
                // TODO really needed?
                auto startTime = std::chrono::steady_clock::now();
                // Capture a weak pointer `parentLog` that will be locked
                // when the request returns. If the locking is successful
                // we are still in the same term.
                follower->_impl->appendEntries(std::move(request))
                    .thenFinal([weakParentLog = it->_parentLog, followerWeak = it->_follower, lastIndex = lastIndex,
                                currentCommitIndex = request.leaderCommit, currentLITK = request.lowestIndexToKeep,
                                currentTerm = log_leader->_currentTerm, messageId = messageId, startTime,
                                logMetrics = logMetrics](futures::Try<AppendEntriesResult> &&res) noexcept {
                        // This has to remain noexcept, because the code below is not
                        // exception safe
                        auto const endTime = std::chrono::steady_clock::now();

                        auto self = weakParentLog.lock();
                        auto follower = followerWeak.lock();
                        if (self != nullptr && follower != nullptr) {
                            using namespace std::chrono_literals;
                            auto const duration = endTime - startTime;
                            self->_logMetrics->replicatedLogAppendEntriesRttUs->count(duration / 1us);
                            LOG_CTX("8ff44", TRACE, follower->logContext)
                                << "received append entries response, messageId = " << messageId;
                            auto [preparedRequests, resolvedPromises] =
                                std::invoke([&]() -> std::pair<std::vector<std::optional<PreparedAppendEntryRequest>>,
                                                               ResolvedPromiseSet> {
                                    auto guarded = self->acquireMutex();
                                    if (!guarded->_didResign) {
                                        return guarded->handleAppendEntriesResponse(
                                            *follower, lastIndex, currentCommitIndex, currentLITK, currentTerm,
                                            std::move(res), endTime - startTime, messageId);
                                    } else {
                                        LOG_CTX("da116", DEBUG, follower->logContext)
                                            << "received response from follower but leader "
                                               "already resigned, messageId = "
                                            << messageId;
                                    }
                                    return {};
                                });

                            handleResolvedPromiseSet(std::move(resolvedPromises), logMetrics);
                            execute_append_entries_requests(std::move(preparedRequests), logMetrics);
                        } else {
                            if (follower == nullptr) {
                                LOG_TOPIC("6f490", DEBUG, Logger::replication) << "follower already gone.";
                            } else {
                                LOG_CTX("de300", DEBUG, follower->logContext)
                                    << "parent log already gone, messageId = " << messageId;
                            }
                        }
                    });
            });
        }
    }
}

auto log::log_leader::construct(agency::log_plan_config config, std::unique_ptr<log_core> log_core,
                                          std::vector<std::shared_ptr<AbstractFollower>> const &followers,
                                          std::shared_ptr<agency::participants_config const> participants_config,
                                          ParticipantId id, log_term term, logger_context const &logContext,
                                          std::shared_ptr<ReplicatedLogMetrics> logMetrics,
                                          std::shared_ptr<ReplicatedLogGlobalSettings const> options,
                                          std::shared_ptr<cluster::IFailureOracle const> failureOracle)
    -> std::shared_ptr<log_leader> {
    if (ADB_UNLIKELY(log_core == nullptr)) {
        auto followerIds = std::vector<std::string> {};
        std::transform(followers.begin(), followers.end(), std::back_inserter(followerIds),
                       [](auto const &follower) -> std::string { return follower->getParticipantId(); });
        auto message =
            basics::StringUtils::concatT("log_core missing when constructing log_leader, leader id: ", id, "term: ", term,
                                         "effectiveWriteConcern: ", config.effectiveWriteConcern,
                                         "followers: ", basics::StringUtils::join(followerIds, ", "));
        THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL, std::move(message));
    }

    // Workaround to be able to use make_shared, while log_leader's constructor
    // is actually protected.
    struct MakeSharedlog_leader : log_leader {
    public:
        MakeSharedlog_leader(logger_context logContext, std::shared_ptr<ReplicatedLogMetrics> logMetrics,
                            std::shared_ptr<ReplicatedLogGlobalSettings const> options, agency::log_plan_config config,
                            ParticipantId id, log_term term, log_index firstIndexOfCurrentTerm, InMemoryLog inMemoryLog,
                            std::shared_ptr<cluster::IFailureOracle const> failureOracle) :
            log_leader(std::move(logContext), std::move(logMetrics), std::move(options), config, std::move(id), term,
                      firstIndexOfCurrentTerm, std::move(inMemoryLog), std::move(failureOracle)) {
        }
    };

    auto log = InMemoryLog::loadFromlog_core(*log_core);
    auto const lastIndex = log.getLastterm_index_pair();
    // if this assertion triggers there is an entry present in the log
    // that has the current term. Did create a different leader with the same term
    // in your test?
    if (lastIndex.term >= term) {
        LOG_CTX("8ed2f", FATAL, logContext) << "Failed to construct log leader. Current term is " << term
                                            << " but spearhead is already at " << lastIndex.term;
        FATAL_ERROR_EXIT();    // This must never happen in production
    }

    // Note that although we add an entry to establish our leadership
    // we do still want to use the unchanged lastIndex to initialize
    // our followers with, as none of them can possibly have this entry.
    // This is particularly important for the LocalFollower, which blindly
    // accepts appendEntriesRequests, and we would thus forget persisting this
    // entry on the leader!

    auto commonLogContext = logContext.with<logContextKeyTerm>(term).with<logContextKeyLeaderId>(id);

    auto leader = std::make_shared<MakeSharedlog_leader>(
        commonLogContext.with<logContextKeyLogComponent>("leader"), std::move(logMetrics), std::move(options), config,
        std::move(id), term, lastIndex.index + 1u, log, std::move(failureOracle));
    auto localFollower = std::make_shared<LocalFollower>(
        *leader, commonLogContext.with<logContextKeyLogComponent>("local-follower"), std::move(log_core), lastIndex);

    TRI_ASSERT(participants_config != nullptr);
    {
        auto leaderDataGuard = leader->acquireMutex();

        leaderDataGuard->_follower = instantiateFollowers(commonLogContext, followers, localFollower, lastIndex);
        leaderDataGuard->activeparticipants_config = participants_config;
        leader->_localFollower = std::move(localFollower);
        TRI_ASSERT(leaderDataGuard->_follower.size() >= config.effectiveWriteConcern)
            << "actual followers: " << leaderDataGuard->_follower.size()
            << " effectiveWriteConcern: " << config.effectiveWriteConcern;
        TRI_ASSERT(leaderDataGuard->_follower.size() == leaderDataGuard->activeparticipants_config->participants.size());
        TRI_ASSERT(
            std::all_of(leaderDataGuard->_follower.begin(), leaderDataGuard->_follower.end(), [&](auto const &it) {
                return leaderDataGuard->activeparticipants_config->participants.find(it.first) !=
                       leaderDataGuard->activeparticipants_config->participants.end();
            }));
    }

    leader->establishLeadership(std::move(participants_config));

    return leader;
}

auto log::log_leader::acquireMutex() -> log_leader::Guard {
    return _guardedLeaderData.getLockedGuard();
}

auto log::log_leader::acquireMutex() const -> log_leader::ConstGuard {
    return _guardedLeaderData.getLockedGuard();
}

auto log::log_leader::resign() && -> std::tuple<std::unique_ptr<log_core>, deferred_action> {
    return _guardedLeaderData.doUnderLock([this, &localFollower = *_localFollower,
                                           &participantId = _id](GuardedLeaderData &leaderData) {
        if (leaderData._didResign) {
            LOG_CTX("5d3b8", ERR, _logContext) << "Leader " << participantId << " already resigned!";
            throw participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED, ADB_HERE);
        }

        // WARNING! This stunt is here to make things exception safe.
        // The move constructor of std::multimap is **not** noexcept.
        // Thus we have to make a new map unique and use std::swap to
        // transfer the content. And then move the unique_ptr into
        // the lambda.
        struct Queues {
            WaitForQueue waitForQueue;
            WaitForBag waitForResignQueue;
        };
        auto queues = std::make_unique<Queues>();
        std::swap(queues->waitForQueue, leaderData._waitForQueue);
        queues->waitForResignQueue = std::move(leaderData._waitForResignQueue);
        auto action = [queues = std::move(queues)]() mutable noexcept {
            for (auto &[idx, promise] : queues->waitForQueue) {
                // Check this to make sure that setException does not throw
                if (!promise.isFulfilled()) {
                    promise.setException(
                        participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED, ADB_HERE));
                }
            }
            queues->waitForResignQueue.resolveAll();
        };

        LOG_CTX("8696f", DEBUG, _logContext) << "resign";
        leaderData._didResign = true;
        static_assert(std::is_nothrow_constructible_v<deferred_action, std::add_rvalue_reference_t<decltype(action)>>);
        static_assert(noexcept(std::declval<LocalFollower &&>().resign()));
        return std::make_tuple(std::move(localFollower).resign(), deferred_action(std::move(action)));
    });
}

auto log::log_leader::readReplicatedEntryByIndex(log_index idx) const -> std::optional<PersistingLogEntry> {
    return _guardedLeaderData.doUnderLock([&idx](auto &leaderData) -> std::optional<PersistingLogEntry> {
        if (leaderData._didResign) {
            throw participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED, ADB_HERE);
        }
        if (auto entry = leaderData._inMemoryLog.getEntryByIndex(idx);
            entry.has_value() && entry->entry().log_index() <= leaderData._commitIndex) {
            return entry->entry();
        } else {
            return std::nullopt;
        }
    });
}

auto log::log_leader::get_status() const -> log_status {
    return _guardedLeaderData.doUnderLock([term = _currentTerm](GuardedLeaderData const &leaderData) {
        if (leaderData._didResign) {
            throw participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED, ADB_HERE);
        }
        LeaderStatus status;
        status.local = leaderData.get_local_statistics();
        status.term = term;
        status.lowestIndexToKeep = leaderData._lowestIndexToKeep;
        status.lastCommitStatus = leaderData._lastCommitFailReason;
        status.leadershipEstablished = leaderData._leadershipEstablished;
        status.activeparticipants_config = *leaderData.activeparticipants_config;
        if (auto const config = leaderData.committedparticipants_config; config != nullptr) {
            status.committedparticipants_config = *config;
        }
        for (auto const &[pid, f] : leaderData._follower) {
            auto lastRequestLatencyMS =
                std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(f->_lastRequestLatency);
            auto state = std::invoke([&, &f = f] {
                switch (f->_state) {
                    case FollowerInfo::State::ERROR_BACKOFF:
                        return FollowerState::withErrorBackoff(
                            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
                                f->_errorBackoffEndTP - std::chrono::steady_clock::now()),
                            f->numErrorsSinceLastAnswer);
                    case FollowerInfo::State::REQUEST_IN_FLIGHT:
                        return FollowerState::withRequestInFlight(
                            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
                                std::chrono::steady_clock::now() - f->_lastRequestStartTP));
                    default:
                        return FollowerState::withUpToDate();
                }
            });
            auto const &participantId = f->_impl->getParticipantId();
            TRI_ASSERT(pid == participantId);
            TRI_ASSERT(!pid.empty());
            status.follower.emplace(participantId,
                                    FollowerStatistics {LogStatistics {f->lastAckedEntry, f->lastAckedCommitIndex},
                                                        f->lastErrorReason, lastRequestLatencyMS, state});
        }

        status.commitLagMS = leaderData.calculateCommitLag();
        return log_status {std::move(status)};
    });
}

auto log::log_leader::getQuickStatus() const -> quick_log_status {
    return _guardedLeaderData.doUnderLock([term = _currentTerm](GuardedLeaderData const &leaderData) {
        if (leaderData._didResign) {
            throw participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED, ADB_HERE);
        }
        auto commitFailReason = std::optional<CommitFailReason> {};
        if (leaderData.calculateCommitLag() > std::chrono::seconds {20}) {
            commitFailReason = leaderData._lastCommitFailReason;
        }
        return quick_log_status {.role = participant_role::kLeader,
                               .term = term,
                               .local = leaderData.get_local_statistics(),
                               .leadershipEstablished = leaderData._leadershipEstablished,
                               .commitFailReason = commitFailReason,
                               .activeparticipants_config = leaderData.activeparticipants_config,
                               .committedparticipants_config = leaderData.committedparticipants_config};
    });
}

auto log::log_leader::insert(log_payload payload, bool waitForSync) -> log_index {
    auto index = insert(std::move(payload), waitForSync, doNotTriggerAsyncReplication);
    triggerAsyncReplication();
    return index;
}

auto log::log_leader::insert(log_payload payload, bool waitForSync, DoNotTriggerAsyncReplication) -> log_index {
    auto const insertTp = InMemoryLogEntry::clock::now();
    // Currently we use a mutex. Is this the only valid semantic?
    return _guardedLeaderData.doUnderLock([&](GuardedLeaderData &leaderData) {
        return leaderData.insertInternal(std::move(payload), waitForSync, insertTp);
    });
}

auto log::log_leader::GuardedLeaderData::insertInternal(
    std::variant<LogMetaPayload, log_payload> payload, bool waitForSync,
    std::optional<InMemoryLogEntry::clock::time_point> insertTp) -> log_index {
    if (this->_didResign) {
        throw participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED, ADB_HERE);
    }
    auto const index = this->_inMemoryLog.getNextIndex();
    auto const payloadSize = std::holds_alternative<log_payload>(payload) ? std::get<log_payload>(payload).byte_size() : 0;
    auto logEntry = InMemoryLogEntry(PersistingLogEntry(term_index_pair {_self._currentTerm, index}, std::move(payload)),
                                     waitForSync);
    logEntry.setInsertTp(insertTp.has_value() ? *insertTp : InMemoryLogEntry::clock::now());
    this->_inMemoryLog.appendInPlace(_self._logContext, std::move(logEntry));
    _self._logMetrics->replicatedLogInsertsBytes->count(payloadSize);
    return index;
}

auto log::log_leader::waitFor(log_index index) -> WaitForFuture {
    return _guardedLeaderData.doUnderLock([index](auto &leaderData) {
        if (leaderData._didResign) {
            auto promise = WaitForPromise {};
            promise.setException(
                participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED, ADB_HERE));
            return promise.getFuture();
        }
        if (leaderData._commitIndex >= index) {
            return futures::Future<wait_for_result> {std::in_place, leaderData._commitIndex, leaderData._lastQuorum};
        }
        auto it = leaderData._waitForQueue.emplace(index, WaitForPromise {});
        auto &promise = it->second;
        auto &&future = promise.getFuture();
        TRI_ASSERT(future.valid());
        return std::move(future);
    });
}

auto log::log_leader::getParticipantId() const noexcept -> ParticipantId const & {
    return _id;
}

auto log::log_leader::triggerAsyncReplication() -> void {
    auto preparedRequests = _guardedLeaderData.doUnderLock([](auto &leaderData) {
        if (leaderData._didResign) {
            throw participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED, ADB_HERE);
        }
        return leaderData.prepareAppendEntries();
    });
    execute_append_entries_requests(std::move(preparedRequests), _logMetrics);
}

auto log::log_leader::GuardedLeaderData::updateCommitIndexLeader(log_index newIndex,
                                                                           std::shared_ptr<QuorumData>
                                                                               quorum) -> ResolvedPromiseSet {
    LOG_CTX("a9a7e", TRACE, _self._logContext)
        << "updating commit index to " << newIndex << " with quorum " << quorum->quorum;
    auto oldIndex = _commitIndex;

    TRI_ASSERT(_commitIndex < newIndex) << "_commitIndex == " << _commitIndex << ", newIndex == " << newIndex;
    _commitIndex = newIndex;
    _lastQuorum = quorum;

    try {
        WaitForQueue toBeResolved;
        auto const end = _waitForQueue.upper_bound(_commitIndex);
        for (auto it = _waitForQueue.begin(); it != end;) {
            LOG_CTX("37d9d", TRACE, _self._logContext) << "resolving promise for index " << it->first;
            toBeResolved.insert(_waitForQueue.extract(it++));
        }
        return ResolvedPromiseSet {std::move(toBeResolved), wait_for_result(newIndex, std::move(quorum)),
                                   _inMemoryLog.slice(oldIndex, newIndex + 1)};
    } catch (std::exception const &e) {
        // If those promises are not fulfilled we can not continue.
        // Note that the move constructor of std::multi_map is not noexcept.
        LOG_CTX("e7a4e", FATAL, _self._logContext) << "failed to fulfill replication promises due to exception; system "
                                                      "can not continue. message: "
                                                   << e.what();
        FATAL_ERROR_EXIT();
    } catch (...) {
        // If those promises are not fulfilled we can not continue.
        // Note that the move constructor of std::multi_map is not noexcept.
        LOG_CTX("c0bbb", FATAL, _self._logContext) << "failed to fulfill replication promises due to exception; system "
                                                      "can not continue";
        FATAL_ERROR_EXIT();
    }
}

auto log::log_leader::GuardedLeaderData::prepareAppendEntries()
    -> std::vector<std::optional<PreparedAppendEntryRequest>> {
    auto appendEntryRequests = std::vector<std::optional<PreparedAppendEntryRequest>> {};
    appendEntryRequests.reserve(_follower.size());
    std::transform(_follower.begin(), _follower.end(), std::back_inserter(appendEntryRequests),
                   [this](auto &follower) { return prepareAppendEntry(follower.second); });
    return appendEntryRequests;
}

auto log::log_leader::GuardedLeaderData::prepareAppendEntry(std::shared_ptr<FollowerInfo> follower)
    -> std::optional<PreparedAppendEntryRequest> {
    if (follower->_state != FollowerInfo::State::IDLE) {
        LOG_CTX("1d7b6", TRACE, follower->logContext) << "request in flight - skipping";
        return std::nullopt;    // wait for the request to return
    }

    auto const lastAvailableIndex = _inMemoryLog.getLastterm_index_pair();
    LOG_CTX("8844a", TRACE, follower->logContext)
        << "last acked index = " << follower->lastAckedEntry << ", current index = " << lastAvailableIndex
        << ", last acked commit index = " << follower->lastAckedCommitIndex
        << ", current commit index = " << _commitIndex << ", last acked lci = " << follower->lastAckedLowestIndexToKeep
        << ", current lci = " << _lowestIndexToKeep;
    if (follower->lastAckedEntry.index == lastAvailableIndex.index && _commitIndex == follower->lastAckedCommitIndex &&
        _lowestIndexToKeep == follower->lastAckedLowestIndexToKeep) {
        LOG_CTX("74b71", TRACE, follower->logContext) << "up to date";
        return std::nullopt;    // nothing to replicate
    }

    auto const executionDelay = std::invoke([&] {
        using namespace std::chrono_literals;
        if (follower->numErrorsSinceLastAnswer > 0) {
            // Capped exponential backoff. Wait for 100us, 200us, 400us, ...
            // until at most 100us * 2 ** 17 == 13.11s.
            auto executionDelay = 100us * (1u << std::min(follower->numErrorsSinceLastAnswer, std::size_t {17}));
            LOG_CTX("2a6f7", DEBUG, follower->logContext)
                << follower->numErrorsSinceLastAnswer << " requests failed, last one was "
                << follower->lastSentMessageId << " - waiting " << executionDelay / 1ms
                << "ms before sending next message.";
            follower->_state = FollowerInfo::State::ERROR_BACKOFF;
            follower->_errorBackoffEndTP = std::chrono::steady_clock::now() + executionDelay;
            return executionDelay;
        } else {
            follower->_state = FollowerInfo::State::PREPARE;
            return 0us;
        }
    });

    return PreparedAppendEntryRequest {_self.shared_from_this(), std::move(follower), executionDelay};
}

auto log::log_leader::GuardedLeaderData::createAppendEntriesRequest(
    log::log_leader::FollowerInfo &follower,
    term_index_pair const &lastAvailableIndex) const -> std::pair<AppendEntriesRequest, term_index_pair> {
    auto const lastAcked = _inMemoryLog.getEntryByIndex(follower.lastAckedEntry.index);

    AppendEntriesRequest req;
    req.leaderCommit = _commitIndex;
    req.lowestIndexToKeep = _lowestIndexToKeep;
    req.leaderTerm = _self._currentTerm;
    req.leaderId = _self._id;
    req.waitForSync = _self._config.waitForSync;
    req.messageId = ++follower.lastSentMessageId;

    follower._state = FollowerInfo::State::REQUEST_IN_FLIGHT;
    follower._lastRequestStartTP = std::chrono::steady_clock::now();

    if (lastAcked) {
        req.prevLogEntry.index = lastAcked->entry().log_index();
        req.prevLogEntry.term = lastAcked->entry().log_term();
        TRI_ASSERT(req.prevLogEntry.index == follower.lastAckedEntry.index);
    } else {
        req.prevLogEntry.index = log_index {0};
        req.prevLogEntry.term = log_term {0};
    }

    {
        auto it = getInternalLogIterator(follower.lastAckedEntry.index + 1);
        auto transientEntries = decltype(req.entries)::transient_type {};
        auto sizeCounter = std::size_t {0};
        while (auto entry = it->next()) {
            req.waitForSync |= entry->getWaitForSync();

            transientEntries.push_back(InMemoryLogEntry(*entry));
            sizeCounter += entry->entry().approxbyte_size();

            if (sizeCounter >= _self._options->_thresholdNetworkBatchSize) {
                break;
            }
        }
        req.entries = std::move(transientEntries).persistent();
    }

    auto isEmptyAppendEntries = req.entries.empty();
    auto lastIndex = isEmptyAppendEntries ? lastAvailableIndex : req.entries.back().entry().log_term_index_pair();

    LOG_CTX("af3c6", TRACE, follower.logContext)
        << "creating append entries request with " << req.entries.size()
        << " entries , prevLogEntry.term = " << req.prevLogEntry.term
        << ", prevLogEntry.index = " << req.prevLogEntry.index << ", leaderCommit = " << req.leaderCommit
        << ", lci = " << req.lowestIndexToKeep << ", msg-id = " << req.messageId;

    return std::make_pair(std::move(req), lastIndex);
}

auto log::log_leader::GuardedLeaderData::handleAppendEntriesResponse(
    FollowerInfo &follower, term_index_pair lastIndex, log_index currentCommitIndex, log_index currentLITK,
    log_term currentTerm, futures::Try<AppendEntriesResult> &&res, std::chrono::steady_clock::duration latency,
    MessageId messageId) -> std::pair<std::vector<std::optional<PreparedAppendEntryRequest>>, ResolvedPromiseSet> {
    if (currentTerm != _self._currentTerm) {
        LOG_CTX("7ab2e", WARN, follower.logContext)
            << "received append entries response with wrong term: " << currentTerm;
        return {};
    }

    ResolvedPromiseSet toBeResolved;

    follower._lastRequestLatency = latency;

    if (follower.lastSentMessageId == messageId) {
        LOG_CTX("35a32", TRACE, follower.logContext)
            << "received message " << messageId << " - no other requests in flight";
        // there is no request in flight currently
        follower._state = FollowerInfo::State::IDLE;
    }
    if (res.hasValue()) {
        auto &response = res.get();
        TRI_ASSERT(messageId == response.messageId) << messageId << " vs. " << response.messageId;
        if (follower.lastSentMessageId == response.messageId) {
            LOG_CTX("35134", TRACE, follower.logContext)
                << "received append entries response, messageId = " << response.messageId
                << ", errorCode = " << to_string(response.errorCode)
                << ", reason  = " << to_string(response.reason.error);

            follower.lastErrorReason = response.reason;
            if (response.isSuccess()) {
                follower.numErrorsSinceLastAnswer = 0;
                follower.lastAckedEntry = lastIndex;
                follower.lastAckedCommitIndex = currentCommitIndex;
                follower.lastAckedLowestIndexToKeep = currentLITK;
            } else {
                TRI_ASSERT(response.reason.error != AppendEntriesErrorReason::ErrorType::kNone);
                switch (response.reason.error) {
                    case AppendEntriesErrorReason::ErrorType::kNoPrevLogMatch:
                        follower.numErrorsSinceLastAnswer = 0;
                        TRI_ASSERT(response.conflict.has_value());
                        follower.lastAckedEntry.index = response.conflict.value().index.saturatedDecrement();
                        LOG_CTX("33c6d", DEBUG, follower.logContext)
                            << "reset last acked index to " << follower.lastAckedEntry;
                        break;
                    default:
                        LOG_CTX("1bd0b", DEBUG, follower.logContext)
                            << "received error from follower, reason = " << to_string(response.reason.error)
                            << " message id = " << messageId;
                        ++follower.numErrorsSinceLastAnswer;
                }
            }
        } else {
            LOG_CTX("056a8", DEBUG, follower.logContext)
                << "received outdated response from follower " << follower._impl->getParticipantId() << ": "
                << response.messageId << ", expected " << messageId << ", latest " << follower.lastSentMessageId;
        }
    } else if (res.hasException()) {
        ++follower.numErrorsSinceLastAnswer;
        follower.lastErrorReason = {AppendEntriesErrorReason::ErrorType::kCommunicationError};
        try {
            res.throwIfFailed();
        } catch (std::exception const &e) {
            follower.lastErrorReason.details = e.what();
            LOG_CTX("e094b", INFO, follower.logContext)
                << "exception in appendEntries to follower " << follower._impl->getParticipantId() << ": " << e.what();
        } catch (...) {
            LOG_CTX("05608", INFO, follower.logContext)
                << "exception in appendEntries to follower " << follower._impl->getParticipantId() << ".";
        }
    } else {
        LOG_CTX("dc441", FATAL, follower.logContext)
            << "in appendEntries to follower " << follower._impl->getParticipantId()
            << ", result future has neither value nor exception.";
        TRI_ASSERT(false);
        FATAL_ERROR_EXIT();
    }

    // checkCommitIndex is called regardless of follower response.
    // The follower might be failed, but the agency can't tell that immediately.
    // Thus, we might have to commit an entry without this follower.
    toBeResolved = checkCommitIndex();
    // try sending the next batch
    return std::make_pair(prepareAppendEntries(), std::move(toBeResolved));
}

auto log::log_leader::GuardedLeaderData::getInternalLogIterator(log_index firstIdx) const
    -> std::unique_ptr<TypedLogIterator<InMemoryLogEntry>> {
    auto const endIdx = _inMemoryLog.getLastterm_index_pair().index + 1;
    TRI_ASSERT(firstIdx <= endIdx);
    return _inMemoryLog.getMemtryIteratorFrom(firstIdx);
}

auto log::log_leader::GuardedLeaderData::getCommittedLogIterator(log_index firstIndex) const
    -> std::unique_ptr<log_rangeIterator> {
    auto const endIdx = _inMemoryLog.getNextIndex();
    TRI_ASSERT(firstIndex < endIdx);
    // return an iterator for the range [firstIndex, _commitIndex + 1)
    return _inMemoryLog.getIteratorRange(firstIndex, _commitIndex + 1);
}

/*
 * Collects last acknowledged term/index pairs from all followers.
 * While doing so, it calculates the largest common index, which is
 * the lowest acknowledged index of all followers.
 * No followers are filtered out at this step.
 */
auto log::log_leader::GuardedLeaderData::collectFollowerStates() const
    -> std::pair<log_index, std::vector<algorithms::participant_state>> {
    auto largestCommonIndex = _commitIndex;
    std::vector<algorithms::participant_state> participant_states;
    participant_states.reserve(_follower.size());
    for (auto const &[pid, follower] : _follower) {
        // The lastAckedEntry is the last index/term pair that we sent that this
        // follower acknowledged - means we sent it. And we must not have entries
        // in our log with a term newer than currentTerm, which could have been
        // sent to a follower.
        TRI_ASSERT(follower->lastAckedEntry.term <= this->_self._currentTerm);

        auto flags = activeparticipants_config->participants.find(pid);
        TRI_ASSERT(flags != std::end(activeparticipants_config->participants));

        participant_states.emplace_back(
            algorithms::participant_state {.lastAckedEntry = follower->lastAckedEntry,
                                          .id = pid,
                                          .failed = _self._failureOracle->isServerFailed(pid),
                                          .flags = flags->second});

        largestCommonIndex = std::min(largestCommonIndex, follower->lastAckedCommitIndex);
    }

    return {largestCommonIndex, std::move(participant_states)};
}

auto log::log_leader::GuardedLeaderData::checkCommitIndex() -> ResolvedPromiseSet {
    auto [largestCommonIndex, indexes] = collectFollowerStates();

    if (largestCommonIndex > _lowestIndexToKeep) {
        LOG_CTX("851bb", TRACE, _self._logContext)
            << "largest common index went from " << _lowestIndexToKeep << " to " << largestCommonIndex;
        _lowestIndexToKeep = largestCommonIndex;
    }

    auto [newCommitIndex, commitFailReason, quorum] = algorithms::calculateCommitIndex(
        indexes, _self._config.effectiveWriteConcern, _commitIndex, _inMemoryLog.getLastterm_index_pair());
    _lastCommitFailReason = commitFailReason;

    LOG_CTX("6a6c0", TRACE, _self._logContext)
        << "calculated commit index as " << newCommitIndex << ", current commit index = " << _commitIndex;
    if (newCommitIndex > _commitIndex) {
        auto const quorum_data = std::make_shared<QuorumData>(newCommitIndex, _self._currentTerm, std::move(quorum));
        return updateCommitIndexLeader(newCommitIndex, quorum_data);
    }
    return {};
}

auto log::log_leader::GuardedLeaderData::get_local_statistics() const -> LogStatistics {
    auto result = LogStatistics {};
    result.commitIndex = _commitIndex;
    result.firstIndex = _inMemoryLog.getFirstIndex();
    result.spearHead = _inMemoryLog.getLastterm_index_pair();
    result.releaseIndex = _releaseIndex;
    return result;
}

log::log_leader::GuardedLeaderData::GuardedLeaderData(log::log_leader &self,
                                                                InMemoryLog inMemoryLog) :
    _self(self),
    _inMemoryLog(std::move(inMemoryLog)) {
}

auto log::log_leader::release(log_index doneWithIdx) -> Result {
    return _guardedLeaderData.doUnderLock([&](GuardedLeaderData &self) -> Result {
        if (self._didResign) {
            return {TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED};
        }
        TRI_ASSERT(doneWithIdx <= self._inMemoryLog.getLastIndex());
        if (doneWithIdx <= self._releaseIndex) {
            return {};
        }
        self._releaseIndex = doneWithIdx;
        LOG_CTX("a0c96", TRACE, _logContext) << "new release index set to " << self._releaseIndex;
        return self.checkCompaction();
    });
}

auto log::log_leader::GuardedLeaderData::checkCompaction() -> Result {
    auto const compactionStop = std::min(_lowestIndexToKeep, _releaseIndex + 1);
    LOG_CTX("080d6", TRACE, _self._logContext) << "compaction index calculated as " << compactionStop;
    if (compactionStop <= _inMemoryLog.getFirstIndex() + 1000) {
        // only do a compaction every 1000 entries
        LOG_CTX("ebba0", TRACE, _self._logContext)
            << "won't trigger a compaction, not enough entries. First index = " << _inMemoryLog.getFirstIndex();
        return {};
    }

    auto newLog = _inMemoryLog.release(compactionStop);
    auto res = _self._localFollower->release(compactionStop);
    if (res.ok()) {
        _inMemoryLog = std::move(newLog);
    }
    LOG_CTX("f1029", TRACE, _self._logContext) << "compaction result = " << res.errorMessage();
    return res;
}

auto log::log_leader::GuardedLeaderData::calculateCommitLag() const noexcept
    -> std::chrono::duration<double, std::milli> {
    auto memtry = _inMemoryLog.getEntryByIndex(_commitIndex + 1);
    if (memtry.has_value()) {
        return std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(std::chrono::steady_clock::now() -
                                                                                     memtry->insertTp());
    } else {
        TRI_ASSERT(_commitIndex == log_index {0} || _commitIndex == _inMemoryLog.getLastIndex())
            << "If there is no entry following the commitIndex the last index "
               "should be the commitIndex. _commitIndex = "
            << _commitIndex << ", lastIndex = " << _inMemoryLog.getLastIndex();
        return {};
    }
}

auto log::log_leader::GuardedLeaderData::waitForResign()
    -> std::pair<futures::Future<futures::Unit>, deferred_action> {
    if (!_didResign) {
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

auto log::log_leader::getReplicatedLogSnapshot() const -> InMemoryLog::log_type {
    auto [log, commitIndex] = _guardedLeaderData.doUnderLock([](auto const &leaderData) {
        if (leaderData._didResign) {
            throw participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED, ADB_HERE);
        }

        return std::make_pair(leaderData._inMemoryLog, leaderData._commitIndex);
    });

    return log.takeSnapshotUpToAndIncluding(commitIndex).copyFlexVector();
}

auto log::log_leader::waitForIterator(log_index index)
    -> log::ILogParticipant::WaitForIteratorFuture {
    if (index == log_index {0}) {
        THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_BAD_PARAMETER, "invalid parameter; log index 0 is invalid");
    }

    return waitFor(index).thenValue([this, self = shared_from_this(), index](auto &&quorum) -> WaitForIteratorFuture {
        auto [actualIndex, iter] = _guardedLeaderData.doUnderLock(
            [index](GuardedLeaderData &leaderData) -> std::pair<log_index, std::unique_ptr<log_rangeIterator>> {
                TRI_ASSERT(index <= leaderData._commitIndex);

                /*
                 * This code here ensures that if only private log entries are present
                 * we do not reply with an empty iterator but instead wait for the
                 * next entry containing payload.
                 */

                auto testIndex = index;
                while (testIndex <= leaderData._commitIndex) {
                    auto memtry = leaderData._inMemoryLog.getEntryByIndex(testIndex);
                    if (!memtry.has_value()) {
                        break;
                    }
                    if (memtry->entry().hasPayload()) {
                        break;
                    }
                    testIndex = testIndex + 1;
                }

                if (testIndex > leaderData._commitIndex) {
                    return std::make_pair(testIndex, nullptr);
                }

                return std::make_pair(testIndex, leaderData.getCommittedLogIterator(testIndex));
            });

        // call here, otherwise we deadlock with waitFor
        if (iter == nullptr) {
            return waitForIterator(actualIndex);
        }

        return std::move(iter);
    });
}

auto log::log_leader::copyInMemoryLog() const -> log::InMemoryLog {
    return _guardedLeaderData.getLockedGuard()->_inMemoryLog;
}

log::log_leader::LocalFollower::LocalFollower(log::log_leader &self, logger_context logContext,
                                                        std::unique_ptr<log_core> log_core,
                                                        [[maybe_unused]] term_index_pair lastIndex) :
    _leader(self),
    _logContext(std::move(logContext)), _guardedlog_core(std::move(log_core)) {
    // TODO save lastIndex. note that it must be protected under the same mutex as
    //      insertions in the persisted log in log_core.
    // TODO use lastIndex in appendEntries to assert that the request matches the
    //      existing log.
    // TODO in maintainer mode only, read here the last entry from log_core, and
    //      assert that lastIndex matches that entry.
}

auto log::log_leader::LocalFollower::getParticipantId() const noexcept -> ParticipantId const & {
    return _leader.getParticipantId();
}

auto log::log_leader::LocalFollower::appendEntries(AppendEntriesRequest const request)
    -> futures::Future<AppendEntriesResult> {
    MeasureTimeGuard measureTimeGuard(_leader._logMetrics->replicatedLogFollowerAppendEntriesRtUs);

    auto messageLogContext = _logContext.with<logContextKeyMessageId>(request.messageId)
                                 .with<logContextKeyPrevlog_idx>(request.prevLogEntry.index)
                                 .with<logContextKeyPrevlog_term>(request.prevLogEntry.term)
                                 .with<logContextKeyLeaderCommit>(request.leaderCommit);

    auto returnAppendEntriesResult = [term = request.leaderTerm, messageId = request.messageId,
                                      logContext = messageLogContext,
                                      measureTime = std::move(measureTimeGuard)](Result const &res) mutable {
        // fire here because the lambda is destroyed much later in a future
        measureTime.fire();
        if (!res.ok()) {
            LOG_CTX("fdc87", FATAL, logContext) << "local follower failed to write entries: " << res;
            FATAL_ERROR_EXIT();
        }
        LOG_CTX("e0800", TRACE, logContext) << "local follower completed append entries";
        return AppendEntriesResult {term, messageId};
    };

    LOG_CTX("6fa8b", TRACE, messageLogContext) << "local follower received append entries";

    if (request.entries.empty()) {
        // Nothing to do here, save some work.
        return returnAppendEntriesResult(Result(TRI_ERROR_NO_ERROR));
    }

    auto iter = std::make_unique<InMemorypersisted_logIterator>(request.entries);
    return _guardedlog_core.doUnderLock([&](auto &log_core) -> futures::Future<AppendEntriesResult> {
        if (log_core == nullptr) {
            LOG_CTX("e9b70", DEBUG, messageLogContext)
                << "local follower received append entries although the log core is "
                   "moved away.";
            return AppendEntriesResult::withRejection(request.leaderTerm, request.messageId,
                                                      {AppendEntriesErrorReason::ErrorType::kLostlog_core});
        }

        // Note that the beginning of iter here is always (and must be) exactly the
        // next index after the last one in the log_core.
        return log_core->insert_async(std::move(iter), request.waitForSync)
            .thenValue(std::move(returnAppendEntriesResult));
    });
}

auto log::log_leader::LocalFollower::resign() &&noexcept -> std::unique_ptr<log_core> {
    LOG_CTX("2062b", TRACE, _logContext) << "local follower received resign, term = " << _leader._currentTerm;
    // Although this method is marked noexcept, the doUnderLock acquires a
    // std::mutex which can throw an exception. In that case we just crash here.
    return _guardedlog_core.doUnderLock([&](auto &guardedlog_core) {
        auto log_core = std::move(guardedlog_core);
        LOG_CTX_IF("0f9b8", DEBUG, _logContext, log_core == nullptr)
            << "local follower asked to resign but log core already gone, term = " << _leader._currentTerm;
        return log_core;
    });
}

auto log::log_leader::is_leadership_established() const noexcept -> bool {
    return _guardedLeaderData.getLockedGuard()->_leadershipEstablished;
}

void log::log_leader::establishLeadership(std::shared_ptr<agency::participants_config const> config) {
    LOG_CTX("f3aa8", TRACE, _logContext) << "trying to establish leadership";
    auto waitForIndex = _guardedLeaderData.doUnderLock([&](GuardedLeaderData &data) {
        auto const lastIndex = data._inMemoryLog.getLastterm_index_pair();
        TRI_ASSERT(lastIndex.term != data._self._currentTerm);
        // Immediately append an empty log entry in the new term. This is
        // necessary because we must not commit entries of older terms, but do
        // not want to wait with committing until the next insert.

        // Also make sure that this entry is written with waitForSync = true to
        // ensure that entries of the previous term are synced as well.
        auto meta = LogMetaPayload::FirstEntryOfTerm {.leader = data._self._id, .participants = *config};
        auto firstIndex = data.insertInternal(LogMetaPayload {std::move(meta)}, true, std::nullopt);
        TRI_ASSERT(firstIndex == lastIndex.index + 1);
        return firstIndex;
    });

    TRI_ASSERT(waitForIndex == _firstIndexOfCurrentTerm);
    waitFor(waitForIndex)
        .thenFinal([weak = weak_from_this(),
                    config = std::move(config)](futures::Try<wait_for_result> &&result) mutable noexcept {
            if (auto self = weak.lock(); self) {
                try {
                    result.throwIfFailed();
                    self->_guardedLeaderData.doUnderLock([&](auto &data) {
                        data._leadershipEstablished = true;
                        if (data.activeparticipants_config->generation == config->generation) {
                            data.committedparticipants_config = std::move(config);
                        }
                    });
                    LOG_CTX("536f4", TRACE, self->_logContext) << "leadership established";
                } catch (participant_resigned_exception const &err) {
                    LOG_CTX("22264", TRACE, self->_logContext)
                        << "failed to establish leadership due to resign: " << err.what();
                } catch (std::exception const &err) {
                    LOG_CTX("5ceda", FATAL, self->_logContext) << "failed to establish leadership: " << err.what();
                }
            } else {
                LOG_TOPIC("94696", TRACE, Logger::replication)
                    << "leader is already gone, no leadership was established";
            }
        });
}

auto log::log_leader::wait_for_leadership() -> log::ILogParticipant::WaitForFuture {
    return waitFor(_firstIndexOfCurrentTerm);
}

namespace {
    // For (unordered) maps `left` and `right`, return `keys(left) \ keys(right)`
    auto const keySetDifference = [](auto const &left, auto const &right) {
        using left_t = std::decay_t<decltype(left)>;
        using right_t = std::decay_t<decltype(right)>;
        static_assert(std::is_same_v<typename left_t::key_type, typename right_t::key_type>);
        using key_t = typename left_t::key_type;

        auto result = std::vector<key_t> {};
        for (auto const &[key, val] : left) {
            if (right.find(key) == right.end()) {
                result.emplace_back(key);
            }
        }

        return result;
    };
}    // namespace

auto log::log_leader::updateparticipants_config(
    std::shared_ptr<agency::participants_config const> const &config,
    std::function<std::shared_ptr<log::AbstractFollower>(ParticipantId const &)> const &buildFollower)
    -> log_index {
    LOG_CTX("ac277", TRACE, _logContext) << "trying to update configuration to generation " << config->generation;
    auto waitForIndex = _guardedLeaderData.doUnderLock([&](GuardedLeaderData &data) {
        auto const [followersToRemove, additionalFollowers] = std::invoke([&] {
            auto const &oldFollowers = data._follower;
            // Note that newParticipants contains the leader, while oldFollowers does
            // not.
            auto const &newParticipants = config->participants;
            auto const additionalParticipantIds = keySetDifference(newParticipants, oldFollowers);
            auto followersToRemove_ = keySetDifference(oldFollowers, newParticipants);

            auto additionalFollowers_ = std::unordered_map<ParticipantId, std::shared_ptr<AbstractFollower>> {};
            for (auto const &participantId : additionalParticipantIds) {
                // exclude the leader
                if (participantId != _id) {
                    additionalFollowers_.try_emplace(participantId, buildFollower(participantId));
                }
            }
            return std::pair(std::move(followersToRemove_), std::move(additionalFollowers_));
        });

        if (data.activeparticipants_config->generation >= config->generation) {
            auto const message = basics::StringUtils::concatT(
                "updated participant config generation is smaller or equal to "
                "current generation - refusing to update; ",
                "new = ", config->generation, ", current = ", data.activeparticipants_config->generation);
            LOG_CTX("bab5b", TRACE, _logContext) << message;
            THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_BAD_PARAMETER, message);
        }

#ifdef DBMS_ENABLE_MAINTAINER_MODE
        // all participants in the new configuration must either exist already, or
        // be added via additionalFollowers.
        {
            auto const &newConfigParticipants = config->participants;
            TRI_ASSERT(std::all_of(newConfigParticipants.begin(), newConfigParticipants.end(),
                                   [&, &additionalFollowers = additionalFollowers](auto const &it) {
                                       return data._follower.find(it.first) != data._follower.end() ||
                                              additionalFollowers.find(it.first) != additionalFollowers.end() ||
                                              it.first == data._self.getParticipantId();
                                   }));
        }
#endif

        // Create a copy. This is important to keep the following code
        // exception-safe, in particular never leave data._follower behind in a
        // half-updated state.
        auto followers = data._follower;

        {    // remove obsolete followers
            for (auto const &it : followersToRemove) {
                followers.erase(it);
            }
        }
        {    // add new followers
            for (auto &&[participantId, abstractFollowerPtr] : additionalFollowers) {
                auto const lastIndex = data._inMemoryLog.getLastterm_index_pair().index.saturatedDecrement();
                followers.try_emplace(participantId,
                                      std::make_shared<FollowerInfo>(std::move(abstractFollowerPtr),
                                                                     term_index_pair {log_term {0}, lastIndex},
                                                                     data._self._logContext));
            }
        }

#ifdef DBMS_ENABLE_MAINTAINER_MODE
        // all participants (but the leader) in the new configuration must now be
        // part of followers
        {
            auto const &newConfigParticipants = config->participants;
            TRI_ASSERT(std::all_of(newConfigParticipants.begin(), newConfigParticipants.end(), [&](auto const &it) {
                return followers.find(it.first) != followers.end() || it.first == data._self.getParticipantId();
            }));
        }
#endif

        auto meta = LogMetaPayload::Updateparticipants_config {.participants = *config};
        auto const idx = data.insertInternal(LogMetaPayload {std::move(meta)}, true, std::nullopt);
        data.activeparticipants_config = config;
        data._follower.swap(followers);

        return idx;
    });

    triggerAsyncReplication();
    waitFor(waitForIndex).thenFinal([weak = weak_from_this(), config](futures::Try<wait_for_result> &&result) noexcept {
        if (auto self = weak.lock(); self) {
            try {
                result.throwIfFailed();
                if (auto guard = self->_guardedLeaderData.getLockedGuard();
                    guard->activeparticipants_config->generation == config->generation) {
                    // Make sure config is the currently active configuration. It
                    // could happen that activeparticipants_config was changed before
                    // config got any chance to see anything committed, thus never
                    // being considered an actual committedparticipants_config. In this
                    // case we skip it.
                    guard->committedparticipants_config = config;
                    LOG_CTX("536f5", DEBUG, self->_logContext)
                        << "configuration committed, generation " << config->generation;
                } else {
                    LOG_CTX("fd245", TRACE, self->_logContext)
                        << "configuration already newer than generation " << config->generation;
                }
            } catch (participant_resigned_exception const &err) {
                LOG_CTX("3959f", DEBUG, self->_logContext)
                    << "leader resigned before new participant configuration was "
                       "committed: "
                    << err.message();
            } catch (std::exception const &err) {
                LOG_CTX("1af0f", FATAL, self->_logContext) << "failed to commit new participant config; " << err.what();
                FATAL_ERROR_EXIT();    // TODO is there nothing we can do here?
            }
        }

        LOG_TOPIC("a4fc1", TRACE, Logger::replication)
            << "leader is already gone, configuration change was not committed";
    });

    return waitForIndex;
}

auto log::log_leader::getCommitIndex() const noexcept -> log_index {
    return _guardedLeaderData.getLockedGuard()->_commitIndex;
}

auto log::log_leader::getParticipantConfigGenerations() const noexcept
    -> std::pair<std::size_t, std::optional<std::size_t>> {
    return _guardedLeaderData.doUnderLock([&](GuardedLeaderData const &data) {
        auto activeGeneration = data.activeparticipants_config->generation;
        auto committedGeneration = std::optional<std::size_t> {};

        if (auto committedConfig = data.committedparticipants_config; committedConfig != nullptr) {
            committedGeneration = committedConfig->generation;
        }

        return std::make_pair(activeGeneration, committedGeneration);
    });
}

auto log::log_leader::waitForResign() -> futures::Future<futures::Unit> {
    using namespace nil::dbms::futures;
    auto &&[future, action] = _guardedLeaderData.getLockedGuard()->waitForResign();

    action.fire();

    return std::move(future);
}

auto log::log_leader::LocalFollower::release(log_index stop) const -> Result {
    auto res = _guardedlog_core.doUnderLock([&](auto &core) {
        LOG_CTX("23745", DEBUG, _logContext) << "local follower releasing with stop at " << stop;
        return core->remove_front(stop).get();
    });
    LOG_CTX_IF("2aba1", WARN, _logContext, res.fail())
        << "local follower failed to release log entries: " << res.errorMessage();
    return res;
}

log::log_leader::PreparedAppendEntryRequest::PreparedAppendEntryRequest(
    std::shared_ptr<log_leader> const &log_leader,
    std::shared_ptr<FollowerInfo>
        follower,
    std::chrono::steady_clock::duration executionDelay) :
    _parentLog(log_leader),
    _follower(std::move(follower)), _executionDelay(executionDelay) {
}

log::log_leader::FollowerInfo::FollowerInfo(std::shared_ptr<AbstractFollower> impl,
                                                      term_index_pair lastlog_index, logger_context const &logContext) :
    _impl(std::move(impl)),
    lastAckedEntry(lastlog_index), logContext(logContext.with<logContextKeyLogComponent>("follower-info")
                                                 .with<logContextKeyFollowerId>(_impl->getParticipantId())) {
}