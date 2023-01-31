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

#include "state.hpp"

#include <string>
#include <unordered_map>
#include <utility>

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>

#include <nil/dbms/replication/log/log_unconfigured_participant.hpp>
#include "nil/dbms/replication/log/log.hpp"
#include <nil/dbms/replication/state/leader_state_manager.hpp>
#include <nil/dbms/replication/state/follower_state_manager.hpp>
#include <nil/dbms/replication/state/unconfigured_state_manager.hpp>
#include <nil/dbms/replication/streams/log_multiplexer.hpp>
#include <nil/dbms/replication/streams/log_multiplexer.tpp>
#include <nil/dbms/replication/streams/stream_specification.hpp>
#include "nil/dbms/replication/streams/streams.hpp"

#include <nil/dbms/replication/exceptions/participant_resigned_exception.hpp>
#include <nil/dbms/replication/state/leader_state_manager.tpp>
#include <nil/dbms/replication/state/follower_state_manager.tpp>
#include <nil/dbms/replication/state/unconfigured_state_manager.tpp>
#include <nil/dbms/replication/state/state_interfaces.hpp>

#include "logger/LogContextKeys.h"

namespace nil::dbms::replication::state {

    template<typename S>
    auto IReplicatedLeaderState<S>::getStream() const -> std::shared_ptr<Stream> const & {
        if (_stream) {
            return _stream;
        }

        THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_BACKEND_UNAVAILABLE);
    }

    template<typename S>
    auto IReplicatedFollowerState<S>::getStream() const -> std::shared_ptr<Stream> const & {
        if (_stream) {
            return _stream;
        }

        THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_BACKEND_UNAVAILABLE);
    }

    template<typename S>
    void IReplicatedFollowerState<S>::setStateManager(std::shared_ptr<FollowerStateManager<S>> manager) noexcept {
        _manager = manager;
        _stream = manager->getStream();
        TRI_ASSERT(_stream != nullptr);
    }

    template<typename S>
    auto IReplicatedFollowerState<S>::wait_for_applied(log_index index) -> futures::Future<futures::Unit> {
        if (auto manager = _manager.lock(); manager != nullptr) {
            return manager->wait_for_applied(index);
        } else {
            wait_for_appliedFuture future(std::make_exception_ptr(log::participant_resigned_exception(
                TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED, ADB_HERE)));
            return future;
        }
    }

    template<typename S>
    ReplicatedState<S>::ReplicatedState(std::shared_ptr<log::ReplicatedLog> log,
                                        std::shared_ptr<Factory> factory, logger_context loggerContext) :
        factory(std::move(factory)),
        log(std::move(log)), guarded_data(*this), loggerContext(std::move(loggerContext)) {
    }

    template<typename S>
    void ReplicatedState<S>::flush(StateGeneration planGeneration) {
        auto deferred = guarded_data.getLockedGuard()->flush(planGeneration);
        // execute *after* the lock has been released
        deferred.fire();
    }

    template<typename S>
    auto ReplicatedState<S>::getFollower() const -> std::shared_ptr<FollowerType> {
        auto guard = guarded_data.getLockedGuard();
        if (auto machine = std::dynamic_pointer_cast<FollowerStateManager<S>>(guard->currentManager); machine) {
            return std::static_pointer_cast<FollowerType>(machine->getFollowerState());
        }
        return nullptr;
    }

    template<typename S>
    auto ReplicatedState<S>::getLeader() const -> std::shared_ptr<LeaderType> {
        auto guard = guarded_data.getLockedGuard();
        if (auto internalState = std::dynamic_pointer_cast<LeaderStateManager<S>>(guard->currentManager);
            internalState) {
            if (auto state = internalState->getImplementationState(); state != nullptr) {
                return std::static_pointer_cast<LeaderType>(state);
            }
        }
        return nullptr;
    }

    template<typename S>
    auto ReplicatedState<S>::get_status() -> std::optional<StateStatus> {
        return guarded_data.doUnderLock([&](guarded_data &data) -> std::optional<StateStatus> {
            if (data.currentManager == nullptr) {
                return std::nullopt;
            }
            // This is guaranteed to not throw in case the currentManager is not
            // resigned.
            return data.currentManager->get_status();
        });
    }

    template<typename S>
    void ReplicatedState<S>::forceRebuild() {
        LOG_CTX("8041a", TRACE, loggerContext) << "Force rebuild of replicated state";
        auto deferred = guarded_data.getLockedGuard()->forceRebuild();
        // execute *after* the lock has been released
        deferred.fire();
    }

    template<typename S>
    void ReplicatedState<S>::start(std::unique_ptr<ReplicatedStateToken> token) {
        auto core = factory->construct_core(log->getGloballog_id());
        auto deferred = guarded_data.getLockedGuard()->rebuild(std::move(core), std::move(token));
        // execute *after* the lock has been released
        deferred.fire();
    }

    template<typename S>
    auto ReplicatedState<S>::guarded_data::rebuild(std::unique_ptr<CoreType> core,
                                                  std::unique_ptr<ReplicatedStateToken>
                                                      token) -> deferred_action try {
        LOG_CTX("edaef", TRACE, _self.loggerContext) << "replicated state rebuilding - query participant";
        auto participant = _self.log->getParticipant();
        if (auto leader = std::dynamic_pointer_cast<log::Ilog_leader>(participant); leader) {
            LOG_CTX("99890", TRACE, _self.loggerContext) << "obtained leader participant";
            return runLeader(std::move(leader), std::move(core), std::move(token));
        } else if (auto follower = std::dynamic_pointer_cast<log::ILogFollower>(participant); follower) {
            LOG_CTX("f5328", TRACE, _self.loggerContext) << "obtained follower participant";
            return runFollower(std::move(follower), std::move(core), std::move(token));
        } else if (auto unconfiguredLogParticipant =
                       std::dynamic_pointer_cast<log::LogUnconfiguredParticipant>(participant);
                   unconfiguredLogParticipant) {
            LOG_CTX("ad84b", TRACE, _self.loggerContext) << "obtained unconfigured participant";
            return runUnconfigured(std::move(unconfiguredLogParticipant), std::move(core), std::move(token));
        } else {
            LOG_CTX("33d5f", FATAL, _self.loggerContext) << "Replicated log has an unhandled participant type.";
            std::abort();
        }
    } catch (replication::log::participant_resigned_exception const &ex) {
        LOG_CTX("eacb9", TRACE, _self.loggerContext)
            << "Replicated log participant is gone. Replicated state will go soon "
               "as well. Error code: "
            << ex.code();
        currentManager = nullptr;
        return {};
    } catch (...) {
        throw;
    }

    template<typename S>
    auto ReplicatedState<S>::guarded_data::runFollower(std::shared_ptr<log::ILogFollower> logFollower,
                                                      std::unique_ptr<CoreType>
                                                          core,
                                                      std::unique_ptr<ReplicatedStateToken>
                                                          token) -> deferred_action try {
        LOG_CTX("95b9d", DEBUG, _self.loggerContext) << "create follower state";

        auto loggerCtx = _self.loggerContext.template with<logContextKeyStateComponent>("follower-manager");
        auto manager = std::make_shared<FollowerStateManager<S>>(std::move(loggerCtx), _self.shared_from_this(),
                                                                 std::move(logFollower), std::move(core),
                                                                 std::move(token), _self.factory);
        currentManager = manager;

        static_assert(noexcept(manager->run()));
        return deferred_action([manager]() noexcept { manager->run(); });
    } catch (std::exception const &e) {
        LOG_CTX("ab9de", DEBUG, _self.loggerContext) << "runFollower caught exception: " << e.what();
        throw;
    }

    template<typename S>
    auto ReplicatedState<S>::guarded_data::runLeader(std::shared_ptr<log::Ilog_leader> log_leader,
                                                    std::unique_ptr<CoreType>
                                                        core,
                                                    std::unique_ptr<ReplicatedStateToken>
                                                        token) -> deferred_action try {
        LOG_CTX("95b9d", DEBUG, _self.loggerContext) << "create leader state";

        auto loggerCtx = _self.loggerContext.template with<logContextKeyStateComponent>("leader-manager");
        auto manager = std::make_shared<LeaderStateManager<S>>(std::move(loggerCtx), _self.shared_from_this(),
                                                               std::move(log_leader), std::move(core), std::move(token),
                                                               _self.factory);
        currentManager = manager;

        static_assert(noexcept(manager->run()));
        return deferred_action([manager]() noexcept { manager->run(); });
    } catch (std::exception const &e) {
        LOG_CTX("016f3", DEBUG, _self.loggerContext) << "run leader caught exception: " << e.what();
        throw;
    }

    template<typename S>
    auto ReplicatedState<S>::guarded_data::runUnconfigured(
        std::shared_ptr<log::LogUnconfiguredParticipant> unconfiguredParticipant,
        std::unique_ptr<CoreType> core, std::unique_ptr<ReplicatedStateToken> token) -> deferred_action try {
        LOG_CTX("5d7c6", DEBUG, _self.loggerContext) << "create unconfigured state";
        auto manager = std::make_shared<UnconfiguredStateManager<S>>(
            _self.shared_from_this(), std::move(unconfiguredParticipant), std::move(core), std::move(token));
        currentManager = manager;

        static_assert(noexcept(manager->run()));
        return deferred_action([manager]() noexcept { manager->run(); });
    } catch (std::exception const &e) {
        LOG_CTX("6f1eb", DEBUG, _self.loggerContext) << "run unconfigured caught exception: " << e.what();
        throw;
    }

    template<typename S>
    auto ReplicatedState<S>::guarded_data::forceRebuild() -> deferred_action {
        try {
            auto [core, token, queueAction] = std::move(*currentManager).resign();
            auto runAction = rebuild(std::move(core), std::move(token));
            return deferred_action::combine(std::move(queueAction), std::move(runAction));
        } catch (std::exception const &e) {
            LOG_CTX("af348", DEBUG, _self.loggerContext) << "forced rebuild caught exception: " << e.what();
            throw;
        }
    }

    template<typename S>
    auto ReplicatedState<S>::guarded_data::flush(StateGeneration planGeneration) -> deferred_action {
        auto [core, token, queueAction] = std::move(*currentManager).resign();
        if (token->generation != planGeneration) {
            token = std::make_unique<ReplicatedStateToken>(planGeneration);
        }

        auto runAction = rebuild(std::move(core), std::move(token));
        return deferred_action::combine(std::move(queueAction), std::move(runAction));
    }

}    // namespace nil::dbms::replication::state
