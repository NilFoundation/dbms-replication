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

#include "leader_state_manager.hpp"
#include "basics/application_exit.h"
#include "basics/debugging.h"
#include "basics/voc_errors.h"

namespace nil::dbms::replication::state {
    template<typename S>
    void LeaderStateManager<S>::run() noexcept {
        // 1. wait for leadership established
        // 1.2. digest available entries into multiplexer
        // 2. construct leader state
        // 2.2 apply all log entries of the previous term
        // 3. make leader state available

        LOG_CTX("53ba0", TRACE, loggerContext) << "LeaderStateManager waiting for leadership to be established";
        guarded_data.getLockedGuard()->updateInternalState(LeaderInternalState::kWaitingForLeadershipEstablished);
        log_leader->wait_for_leadership()
            .thenValue([weak = this->weak_from_this()](auto &&result) {
                auto self = weak.lock();
                if (self == nullptr) {
                    return futures::Future<Result> {TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED};
                }
                LOG_CTX("53ba1", TRACE, self->loggerContext) << "LeaderStateManager established";
                auto f = self->guarded_data.doUnderLock([&](guarded_data &data) {
                    TRI_ASSERT(data.internalState == LeaderInternalState::kWaitingForLeadershipEstablished);
                    data.updateInternalState(LeaderInternalState::kIngestingExistingLog);
                    auto mux = Multiplexer::construct(self->log_leader);
                    mux->digestAvailableEntries();
                    data.stream = mux->template getStreamById<1>();    // TODO fix stream id
                    return data.stream->waitForIterator(log_index {0});
                });

                LOG_CTX("53ba2", TRACE, self->loggerContext) << "receiving committed entries for recovery";
                // TODO we don't have to `waitFor` we can just access the log.
                //    new entries are not yet written, because the stream is
                //    not published.
                return std::move(f).thenValue([weak](std::unique_ptr<Iterator> &&result) {
                    auto self = weak.lock();
                    if (self == nullptr) {
                        return futures::Future<Result> {TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED};
                    }
                    LOG_CTX("53ba0", TRACE, self->loggerContext) << "creating leader instance";
                    auto core = self->guarded_data.doUnderLock([&](guarded_data &data) {
                        data.updateInternalState(LeaderInternalState::kRecoveryInProgress, result->range());
                        return std::move(data.core);
                    });
                    if (core == nullptr) {
                        LOG_CTX("6d9ee", DEBUG, self->loggerContext) << "core already gone";
                        return futures::Future<Result> {TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED};
                    }
                    LOG_CTX("5af0d", DEBUG, self->loggerContext) << "starting recovery on range " << result->range();
                    std::shared_ptr<IReplicatedLeaderState<S>> machine =
                        self->factory->construct_leader(std::move(core));
                    return machine->recover_entries(std::move(result))
                        .then([weak, machine](futures::Try<Result> &&tryResult) mutable -> Result {
                            auto self = weak.lock();
                            if (self == nullptr) {
                                return Result {TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED};
                            }
                            try {
                                if (auto result = tryResult.get(); result.ok()) {
                                    LOG_CTX("1a375", DEBUG, self->loggerContext) << "recovery on leader completed";
                                    auto state = self->guarded_data.doUnderLock(
                                        [&](guarded_data &data) -> std::shared_ptr<IReplicatedLeaderState<S>> {
                                            if (data.token == nullptr) {
                                                LOG_CTX("59a31", DEBUG, self->loggerContext) << "token already gone";
                                                return nullptr;
                                            }
                                            data.state = machine;
                                            data.token->snapshot.updateStatus(SnapshotStatus::kCompleted);
                                            data.updateInternalState(LeaderInternalState::kServiceAvailable);
                                            data.state->_stream = data.stream;
                                            return data.state;
                                        });

                                    if (state != nullptr) {
                                        state->on_snapshot_completed();
                                        self->beginWaitingForParticipantResigned();
                                        return result;
                                    } else {
                                        return Result {TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED};
                                    }
                                } else {
                                    LOG_CTX("3fd49", FATAL, self->loggerContext)
                                        << "recovery failed with error: " << result.errorMessage();
                                    FATAL_ERROR_EXIT();
                                }
                            } catch (std::exception const &e) {
                                LOG_CTX("3aaf8", FATAL, self->loggerContext)
                                    << "recovery failed with exception: " << e.what();
                                FATAL_ERROR_EXIT();
                            } catch (...) {
                                LOG_CTX("a207d", FATAL, self->loggerContext)
                                    << "recovery failed with unknown exception";
                                FATAL_ERROR_EXIT();
                            }
                        });
                });
            })
            .thenFinal([weak = this->weak_from_this()](futures::Try<Result> &&result) {
                auto self = weak.lock();
                if (self == nullptr) {
                    return;
                }
                try {
                    auto res = result.get();    // throws exceptions
                    if (res.is(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED)) {
                        return;
                    }
                    TRI_ASSERT(res.ok());
                    if (!res.ok()) {
                        THROW_DBMS_EXCEPTION(res);
                    }
                } catch (nil::dbms::basics::Exception const &e) {
                    if (e.code() == TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED) {
                        return;
                    }
                    LOG_CTX("e73bd", FATAL, self->loggerContext)
                        << "Unexpected exception in leader startup procedure: " << e.what();
                    FATAL_ERROR_EXIT();
                } catch (std::exception const &e) {
                    LOG_CTX("e73bc", FATAL, self->loggerContext)
                        << "Unexpected exception in leader startup procedure: " << e.what();
                    FATAL_ERROR_EXIT();
                } catch (...) {
                    LOG_CTX("4d2b7", FATAL, self->loggerContext) << "Unexpected exception in leader startup procedure";
                    FATAL_ERROR_EXIT();
                }
            });
    }

    template<typename S>
    LeaderStateManager<S>::LeaderStateManager(logger_context loggerContext,
                                              std::shared_ptr<ReplicatedState<S>> const &parent,
                                              std::shared_ptr<log::Ilog_leader> leader,
                                              std::unique_ptr<CoreType> core,
                                              std::unique_ptr<ReplicatedStateToken> token,
                                              std::shared_ptr<Factory> factory) noexcept :
        guarded_data(*this, LeaderInternalState::kWaitingForLeadershipEstablished, std::move(core), std::move(token)),
        parent(parent), log_leader(std::move(leader)), loggerContext(std::move(loggerContext)),
        factory(std::move(factory)) {
    }

    template<typename S>
    auto LeaderStateManager<S>::get_status() const -> StateStatus {
        auto guard = guarded_data.getLockedGuard();
        if (guard->_didResign) {
            TRI_ASSERT(guard->core == nullptr && guard->token == nullptr);
            throw log::participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED,
                                                               ADB_HERE);
        } else {
            // Note that `this->core` is passed into the state when the leader is
            // started (more particularly, when the leader state is created), but
            // `this->state` is only set after replaying the log has finished.
            // Thus both can be null here.
            TRI_ASSERT(guard->token != nullptr);
        }
        LeaderStatus status;
        status.managerState.state = guard->internalState;
        status.managerState.lastChange = guard->lastInternalStateChange;
        if (guard->internalState == LeaderInternalState::kRecoveryInProgress && guard->recoveryRange) {
            status.managerState.detail = "recovery range is " + to_string(*guard->recoveryRange);
        } else {
            status.managerState.detail = std::nullopt;
        }
        status.snapshot = guard->token->snapshot;
        status.generation = guard->token->generation;
        return StateStatus {.variant = std::move(status)};
    }

    template<typename S>
    auto LeaderStateManager<S>::resign() &&noexcept
        -> std::tuple<std::unique_ptr<CoreType>, std::unique_ptr<ReplicatedStateToken>, deferred_action> {
        LOG_CTX("edcf3", TRACE, loggerContext) << "Leader manager resign";
        auto guard = guarded_data.getLockedGuard();
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
        return std::make_tuple(std::move(core), std::move(guard->token), deferred_action {});
    }

    template<typename S>
    void LeaderStateManager<S>::beginWaitingForParticipantResigned() {
        log_leader->waitForResign().thenFinal([weak = this->weak_from_this()](auto &&) {
            if (auto self = weak.lock(); self != nullptr) {
                if (auto parentPtr = self->parent.lock(); parentPtr != nullptr) {
                    parentPtr->forceRebuild();
                }
            }
        });
    }

    template<typename S>
    auto LeaderStateManager<S>::getImplementationState() -> std::shared_ptr<IReplicatedLeaderState<S>> {
        return guarded_data.getLockedGuard()->state;
    }
}    // namespace nil::dbms::replication::state