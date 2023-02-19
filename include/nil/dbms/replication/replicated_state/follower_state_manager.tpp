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

#include "follower_state_manager.hpp"
#include <nil/dbms/replication/exceptions/participant_resigned_exception.hpp>

#include "basics/application_exit.h"
#include "basics/debugging.h"
#include "basics/voc_errors.h"
#include "basics/exceptions.h"
// #include "nil/dbms/scheduler/scheduler_feature.hpp"

namespace nil::dbms::replication::replicated_state {

    template<typename S>
    void follower_state_manager<S>::apply_entries(std::unique_ptr<Iterator> iter) noexcept {
        TRI_ASSERT(iter != nullptr);
        auto range = iter->range();
        LOG_CTX("3678e", TRACE, loggerContext) << "apply entries in range " << range;

        auto state = _guardedData.doUnderLock([&](guarded_data &data) {
            data.updateInternalState(follower_internal_state::kApplyRecentEntries, range);
            return data.state;
        });

        TRI_ASSERT(state != nullptr);

        state->apply_entries(std::move(iter))
            .thenFinal([weak = this->weak_from_this(), range](futures::Try<Result> tryResult) noexcept {
                auto self = weak.lock();
                if (self == nullptr) {
                    LOG_CTX("a87aa", TRACE, self->loggerContext) << "replicated state already gone";
                    return;
                }
                try {
                    auto &result = tryResult.get();
                    if (result.ok()) {
                        LOG_CTX("6e9bb", TRACE, self->loggerContext) << "follower state applied range " << range;

                        auto [outerAction, pollFuture] =
                            self->pollNewEntries([&](guarded_data &data) { return data.updateNextIndex(range.to); });
                        // this action will resolve promises that wait for a given index to
                        // be applied
                        outerAction.fire();
                        self->handlePollResult(std::move(pollFuture));
                        return;
                    } else {
                        LOG_CTX("335f0", ERR, self->loggerContext)
                            << "follower failed to apply range " << range << " and returned error " << result;
                    }
                } catch (std::exception const &e) {
                    LOG_CTX("2fbae", ERR, self->loggerContext)
                        << "follower failed to apply range " << range << " with exception: " << e.what();
                } catch (...) {
                    LOG_CTX("1a737", ERR, self->loggerContext)
                        << "follower failed to apply range " << range << " with unknown exception";
                }

                LOG_CTX("c89c8", DEBUG, self->loggerContext) << "trigger retry for polling";
                // TODO retry
                std::abort();
            });
    }

    template<typename S>
    template<typename F>
    auto follower_state_manager<S>::pollNewEntries(F &&fn) {
        return _guardedData.doUnderLock([&](guarded_data &data) {
            auto result = std::invoke(std::forward<F>(fn), data);
            TRI_ASSERT(data.stream != nullptr);
            LOG_CTX("a1462", TRACE, loggerContext)
                << "polling for new entries _nextWaitForIndex = " << data._nextWaitForIndex;
            data.updateInternalState(follower_internal_state::kNothingToApply);
            return std::make_tuple(std::move(result), data.stream->wait_for_iterator(data._nextWaitForIndex));
        });
    }

    template<typename S>
    void follower_state_manager<S>::handlePollResult(futures::Future<std::unique_ptr<Iterator>> &&pollFuture) {
        std::move(pollFuture)
            .thenFinal([weak = this->weak_from_this()](futures::Try<std::unique_ptr<Iterator>> result) noexcept {
                auto self = weak.lock();
                if (self == nullptr) {
                    return;
                }
                try {
                    self->apply_entries(std::move(result).get());
                } catch (replicated_log::participant_resigned_exception const &) {
                    if (auto ptr = self->parent.lock(); ptr) {
                        LOG_CTX("654fb", TRACE, self->loggerContext) << "forcing rebuild because participant resigned";
                        ptr->forceRebuild();
                    } else {
                        LOG_CTX("15cb4", TRACE, self->loggerContext)
                            << "LogFollower resigned, but Replicated State already gone";
                    }
                } catch (basics::Exception const &e) {
                    LOG_CTX("f2188", FATAL, self->loggerContext)
                        << "waiting for leader ack failed with unexpected exception: " << e.message();
                }
            });
    }

    template<typename S>
    auto follower_state_manager<S>::waitForApplied(log_index idx) -> futures::Future<futures::Unit> {
        auto guard = _guardedData.getLockedGuard();
        if (guard->_nextWaitForIndex > idx) {
            return futures::Future<futures::Unit> {std::in_place};
        }

        auto it = guard->waitForAppliedQueue.emplace(idx, WaitForAppliedPromise {});
        auto f = it->second.getFuture();
        TRI_ASSERT(f.valid());
        return f;
    }

    template<typename S>
    void follower_state_manager<S>::tryTransferSnapshot(std::shared_ptr<ireplicated_follower_state<S>> hiddenState) {
        auto &leader = logFollower->getLeader();
        TRI_ASSERT(leader.has_value()) << "leader established it's leadership. There "
                                          "has to be a leader in the current term";

        LOG_CTX("52a11", DEBUG, loggerContext)
            << "try to acquire a new snapshot, starting at " << logFollower->getCommitIndex();
        auto f = hiddenState->acquireSnapshot(*leader, logFollower->getCommitIndex());
        std::move(f).thenFinal([weak = this->weak_from_this(), hiddenState](futures::Try<Result> &&tryResult) noexcept {
            auto self = weak.lock();
            if (self == nullptr) {
                return;
            }

            auto result = basics::catchToResult([&] { return tryResult.get(); });
            if (result.ok()) {
                LOG_CTX("44d58", DEBUG, self->loggerContext) << "snapshot transfer successfully completed";

                bool startService = self->_guardedData.doUnderLock([&](guarded_data &data) {
                    if (data.token == nullptr) {
                        return false;
                    }
                    data.token->snapshot.updateStatus(snapshot_status::kCompleted);
                    return true;
                });
                if (startService) {
                    self->startService(hiddenState);
                }
                return;
            } else {
                LOG_CTX("9a68a", ERR, self->loggerContext)
                    << "failed to transfer snapshot: " << result.errorMessage() << " - retry scheduled";

                auto retryCount = self->_guardedData.doUnderLock([&](guarded_data &data) {
                    data.updateInternalState(follower_internal_state::kSnapshotTransferFailed, result);
                    return data.errorCounter;
                });

                self->retryTransferSnapshot(std::move(hiddenState), retryCount);
            }
        });
    }

    namespace {
        inline auto delayedFuture(std::chrono::steady_clock::duration duration) -> futures::Future<futures::Unit> {
            //          TODO: return SchedulerFeature::SCHEDULER->delay(duration); when SchedulerFeature is available
            //          from include
            //            if (SchedulerFeature::SCHEDULER) {
            //                return SchedulerFeature::SCHEDULER->delay(duration);
            //            }

            std::this_thread::sleep_for(duration);
            return futures::Future<futures::Unit> {std::in_place};
        }

        inline auto calcRetryDuration(std::uint64_t retryCount) -> std::chrono::steady_clock::duration {
            // Capped exponential backoff. Wait for 100us, 200us, 400us, ...
            // until at most 100us * 2 ** 17 == 13.11s.
            auto executionDelay = std::chrono::microseconds {100} * (1u << std::min(retryCount, std::uint64_t {17}));
            return std::chrono::duration_cast<std::chrono::steady_clock::duration>(executionDelay);
        }
    }    // namespace

    template<typename S>
    void follower_state_manager<S>::retryTransferSnapshot(std::shared_ptr<ireplicated_follower_state<S>> hiddenState,
                                                          std::uint64_t retryCount) {
        auto duration = calcRetryDuration(retryCount);
        LOG_CTX("2ea59", TRACE, loggerContext)
            << "retry snapshot transfer after "
            << std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() << "ms";
        delayedFuture(duration).thenFinal([weak = this->weak_from_this(), hiddenState](auto &&) {
            auto self = weak.lock();
            if (self == nullptr) {
                return;
            }

            self->tryTransferSnapshot(hiddenState);
        });
    }

    template<typename S>
    void follower_state_manager<S>::checkSnapshot(std::shared_ptr<ireplicated_follower_state<S>> hiddenState) {
        bool needsSnapshot = _guardedData.doUnderLock([&](guarded_data &data) {
            LOG_CTX("aee5b", DEBUG, loggerContext)
                << "snapshot status is " << data.token->snapshot.status << ", generation is " << data.token->generation;
            return data.token->snapshot.status != snapshot_status::kCompleted;
        });
        if (needsSnapshot) {
            LOG_CTX("3d0fc", DEBUG, loggerContext) << "new snapshot is required";
            tryTransferSnapshot(hiddenState);
        } else {
            LOG_CTX("9cd75", DEBUG, loggerContext) << "no snapshot transfer required";
            startService(hiddenState);
        }
    }

    template<typename S>
    void follower_state_manager<S>::startService(std::shared_ptr<ireplicated_follower_state<S>> hiddenState) {
        hiddenState->setStateManager(this->shared_from_this());

        auto [nothing, pollFuture] = pollNewEntries([&](guarded_data &data) {
            LOG_CTX("26c55", TRACE, loggerContext) << "starting service as follower";
            data.state = hiddenState;
            return std::monostate {};
        });

        handlePollResult(std::move(pollFuture));
    }

    template<typename S>
    void follower_state_manager<S>::ingestLogData() {
        auto core = _guardedData.doUnderLock([&](guarded_data &data) {
            data.updateInternalState(follower_internal_state::kTransferSnapshot);
            auto demux = Demultiplexer::construct(logFollower);
            demux->listen();
            data.stream = demux->template getStreamById<1>();
            return std::move(data.core);
        });

        LOG_CTX("1d843", TRACE, loggerContext) << "creating follower state instance";
        auto hiddenState = factory->constructFollower(std::move(core));

        LOG_CTX("ea777", TRACE, loggerContext) << "check if new snapshot is required";
        checkSnapshot(hiddenState);
    }

    template<typename S>
    void follower_state_manager<S>::awaitLeaderShip() {
        _guardedData.getLockedGuard()->updateInternalState(follower_internal_state::kWaitForLeaderConfirmation);
        try {
            handleAwaitLeadershipResult(logFollower->waitForLeaderAcked());
        } catch (replicated_log::participant_resigned_exception const &) {
            if (auto p = parent.lock(); p) {
                LOG_CTX("1cb5c", TRACE, loggerContext) << "forcing rebuild because participant resigned";
                return p->forceRebuild();
            } else {
                LOG_CTX("a62cb", TRACE, loggerContext) << "replicated state already gone";
            }
        }
    }

    template<typename S>
    void follower_state_manager<S>::handleAwaitLeadershipResult(futures::Future<replicated_log::wait_for_result> &&f) {
        std::move(f).thenFinal(
            [weak = this->weak_from_this()](futures::Try<replicated_log::wait_for_result> &&result) noexcept {
                auto self = weak.lock();
                if (self == nullptr) {
                    return;
                }
                try {
                    try {
                        result.throwIfFailed();
                        LOG_CTX("53ba1", TRACE, self->loggerContext) << "leadership acknowledged - ingesting log data";
                        self->ingestLogData();
                    } catch (replicated_log::participant_resigned_exception const &) {
                        if (auto ptr = self->parent.lock(); ptr) {
                            LOG_CTX("79e37", DEBUG, self->loggerContext)
                                << "participant resigned before leadership - force rebuild";
                            ptr->forceRebuild();
                        } else {
                            LOG_CTX("15cb4", DEBUG, self->loggerContext)
                                << "LogFollower resigned, but Replicated State already "
                                   "gone";
                        }
                    } catch (basics::Exception const &e) {
                        LOG_CTX("f2188", FATAL, self->loggerContext)
                            << "waiting for leader ack failed with unexpected exception: " << e.message();
                        FATAL_ERROR_EXIT();
                    }
                } catch (std::exception const &ex) {
                    LOG_CTX("c7787", FATAL, self->loggerContext)
                        << "waiting for leader ack failed with unexpected exception: " << ex.what();
                    FATAL_ERROR_EXIT();
                } catch (...) {
                    LOG_CTX("43456", FATAL, self->loggerContext)
                        << "waiting for leader ack failed with unexpected exception";
                    FATAL_ERROR_EXIT();
                }
            });
    }

    template<typename S>
    void follower_state_manager<S>::run() noexcept {
        // 1. wait for log follower to have committed at least one entry
        // 2. receive a new snapshot (if required)
        //    if (old_generation != new_generation || snapshot_status != Completed)
        // 3. start polling for new entries
        awaitLeaderShip();
    }

    template<typename S>
    follower_state_manager<S>::follower_state_manager(logger_context loggerContext,
                                                      std::shared_ptr<replicated_state_base>
                                                          parent,
                                                      std::shared_ptr<replicated_log::ilog_follower>
                                                          logFollower,
                                                      std::unique_ptr<CoreType>
                                                          core,
                                                      std::unique_ptr<replicated_state_token>
                                                          token,
                                                      std::shared_ptr<Factory>
                                                          factory) noexcept :
        _guardedData(*this, std::move(core), std::move(token)),
        parent(parent), logFollower(std::move(logFollower)), factory(std::move(factory)),
        loggerContext(std::move(loggerContext)) {
    }

    template<typename S>
    auto follower_state_manager<S>::getStatus() const -> state_status {
        return _guardedData.doUnderLock([&](guarded_data const &data) {
            if (data._didResign) {
                TRI_ASSERT(data.core == nullptr && data.token == nullptr);
                throw replicated_log::participant_resigned_exception(
                    TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED, ADB_HERE);
            } else {
                // Note that `this->core` is passed into the state when the follower is
                // started (more particularly, when the follower state is created), but
                // `this->state` is only set after replaying the log has finished.
                // Thus both can be null here.
                TRI_ASSERT(data.token != nullptr);
            }
            follower_status status;
            status.managerState.state = data.internalState;
            status.managerState.lastChange = data.lastInternalStateChange;
            status.managerState.detail = std::nullopt;
            status.generation = data.token->generation;
            status.snapshot = data.token->snapshot;

            if (data.lastError.has_value()) {
                status.managerState.detail =
                    basics::StringUtils::concatT("Last error was: ", data.lastError->errorMessage());
            }

            return state_status {.variant = std::move(status)};
        });
    }

    template<typename S>
    auto follower_state_manager<S>::getFollowerState() const -> std::shared_ptr<ireplicated_follower_state<S>> {
        return _guardedData.getLockedGuard()->state;
    }

    template<typename S>
    auto follower_state_manager<S>::resign() &&noexcept
        -> std::tuple<std::unique_ptr<CoreType>, std::unique_ptr<replicated_state_token>, deferred_action> {
        auto resolveQueue = std::make_unique<WaitForAppliedQueue>();
        LOG_CTX("63622", TRACE, loggerContext) << "Follower manager resigning";
        auto guard = _guardedData.getLockedGuard();
        auto core = std::invoke([&] {
            if (guard->state != nullptr) {
                TRI_ASSERT(guard->core == nullptr);
                return std::move(*guard->state).resign();
            } else {
                return std::move(guard->core);
            }
        });
        TRI_ASSERT(core != nullptr);
        TRI_ASSERT(guard->token != nullptr);
        TRI_ASSERT(!guard->_didResign);
        guard->_didResign = true;
        std::swap(*resolveQueue, guard->waitForAppliedQueue);
        return std::make_tuple(std::move(core), std::move(guard->token),
                               deferred_action([resolveQueue = std::move(resolveQueue)]() noexcept {
                                   for (auto &p : *resolveQueue) {
                                       p.second.setException(replicated_log::participant_resigned_exception(
                                           TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED, ADB_HERE));
                                   }
                               }));
    }

    template<typename S>
    auto follower_state_manager<S>::getStream() const noexcept -> std::shared_ptr<Stream> {
        return _guardedData.getLockedGuard()->stream;
    }

    template<typename S>
    void follower_state_manager<S>::guarded_data::updateInternalState(follower_internal_state newState,
                                                                      std::optional<log_range>
                                                                          range) {
        internalState = newState;
        lastInternalStateChange = std::chrono::system_clock::now();
        ingestionRange = range;
        lastError.reset();
        errorCounter = 0;
    }

    template<typename S>
    void follower_state_manager<S>::guarded_data::updateInternalState(follower_internal_state newState, Result error) {
        internalState = newState;
        lastInternalStateChange = std::chrono::system_clock::now();
        ingestionRange.reset();
        lastError.emplace(std::move(error));
        errorCounter += 1;
    }

    template<typename S>
    auto follower_state_manager<S>::guarded_data::updateNextIndex(log_index nextWaitForIndex) -> deferred_action {
        _nextWaitForIndex = nextWaitForIndex;
        auto resolveQueue = std::make_unique<WaitForAppliedQueue>();
        LOG_CTX("9929a", TRACE, self.loggerContext) << "Resolving WaitForApplied promises upto " << nextWaitForIndex;
        auto const end = waitForAppliedQueue.lower_bound(nextWaitForIndex);
        for (auto it = waitForAppliedQueue.begin(); it != end;) {
            resolveQueue->insert(waitForAppliedQueue.extract(it++));
        }
        return deferred_action([resolveQueue = std::move(resolveQueue)]() noexcept {
            for (auto &p : *resolveQueue) {
                p.second.setValue();
            }
        });
    }

    template<typename S>
    follower_state_manager<S>::guarded_data::guarded_data(follower_state_manager &self, std::unique_ptr<CoreType> core,
                                                          std::unique_ptr<replicated_state_token> token) :
        self(self),
        core(std::move(core)), token(std::move(token)) {
        TRI_ASSERT(this->core != nullptr);
        TRI_ASSERT(this->token != nullptr);
    }

}    // namespace nil::dbms::replication::replicated_state
