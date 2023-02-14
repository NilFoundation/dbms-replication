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

#include "replicated_state.hpp"

#include <string>
#include <unordered_map>
#include <utility>

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>

#include <nil/replication_sdk/replicated_log/log_unconfigured_participant.hpp>
#include "nil/replication_sdk/replicated_log/replicated_log.hpp"
#include <nil/replication_sdk/replicated_state/leader_state_manager.hpp>
#include <nil/replication_sdk/replicated_state/follower_state_manager.hpp>
#include <nil/replication_sdk/replicated_state/unconfigured_state_manager.hpp>
#include <nil/replication_sdk/streams/log_multiplexer.hpp>
#include <nil/replication_sdk/streams/log_multiplexer.tpp>
#include <nil/replication_sdk/streams/stream_specification.hpp>
#include "nil/replication_sdk/streams/streams.hpp"

#include <nil/replication_sdk/exceptions/participant_resigned_exception.hpp>
#include <nil/replication_sdk/replicated_state/leader_state_manager.tpp>
#include <nil/replication_sdk/replicated_state/follower_state_manager.tpp>
#include <nil/replication_sdk/replicated_state/unconfigured_state_manager.tpp>
#include <nil/replication_sdk/replicated_state/state_interfaces.hpp>

#include "logger/LogContextKeys.h"

namespace nil::dbms::replication_sdk::replicated_state {

    template<typename S>
    auto ireplicated_leader_state<S>::getStream() const -> std::shared_ptr<Stream> const & {
        if (_stream) {
            return _stream;
        }

        THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_BACKEND_UNAVAILABLE);
    }

    template<typename S>
    auto ireplicated_follower_state<S>::getStream() const -> std::shared_ptr<Stream> const & {
        if (_stream) {
            return _stream;
        }

        THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_BACKEND_UNAVAILABLE);
    }

    template<typename S>
    void ireplicated_follower_state<S>::setStateManager(std::shared_ptr<follower_state_manager<S>> manager) noexcept {
        _manager = manager;
        _stream = manager->getStream();
        TRI_ASSERT(_stream != nullptr);
    }

    template<typename S>
    auto ireplicated_follower_state<S>::waitForApplied(log_index index) -> futures::Future<futures::Unit> {
        if (auto manager = _manager.lock(); manager != nullptr) {
            return manager->waitForApplied(index);
        } else {
            WaitForAppliedFuture future(std::make_exception_ptr(replicated_log::participant_resigned_exception(
                TRI_ERROR_REPLICATION_REPLICATED_LOG_FOLLOWER_RESIGNED, ADB_HERE)));
            return future;
        }
    }

    template<typename S>
    replicated_state_t<S>::replicated_state_t(std::shared_ptr<replicated_log::replicated_log_t> log,
                                        std::shared_ptr<Factory> factory, logger_context loggerContext) :
        factory(std::move(factory)),
        log(std::move(log)), guardedData(*this), loggerContext(std::move(loggerContext)) {
    }

    template<typename S>
    void replicated_state_t<S>::flush(state_generation planGeneration) {
        auto deferred = guardedData.getLockedGuard()->flush(planGeneration);
        // execute *after* the lock has been released
        deferred.fire();
    }

    template<typename S>
    auto replicated_state_t<S>::getFollower() const -> std::shared_ptr<FollowerType> {
        auto guard = guardedData.getLockedGuard();
        if (auto machine = std::dynamic_pointer_cast<follower_state_manager<S>>(guard->currentManager); machine) {
            return std::static_pointer_cast<FollowerType>(machine->getFollowerState());
        }
        return nullptr;
    }

    template<typename S>
    auto replicated_state_t<S>::getLeader() const -> std::shared_ptr<LeaderType> {
        auto guard = guardedData.getLockedGuard();
        if (auto internalState = std::dynamic_pointer_cast<leader_state_manager<S>>(guard->currentManager);
            internalState) {
            if (auto state = internalState->getImplementationState(); state != nullptr) {
                return std::static_pointer_cast<LeaderType>(state);
            }
        }
        return nullptr;
    }

    template<typename S>
    auto replicated_state_t<S>::getStatus() -> std::optional<state_status> {
        return guardedData.doUnderLock([&](guarded_data &data) -> std::optional<state_status> {
            if (data.currentManager == nullptr) {
                return std::nullopt;
            }
            // This is guaranteed to not throw in case the currentManager is not
            // resigned.
            return data.currentManager->getStatus();
        });
    }

    template<typename S>
    void replicated_state_t<S>::forceRebuild() {
        LOG_CTX("8041a", TRACE, loggerContext) << "Force rebuild of replicated state";
        auto deferred = guardedData.getLockedGuard()->forceRebuild();
        // execute *after* the lock has been released
        deferred.fire();
    }

    template<typename S>
    void replicated_state_t<S>::start(std::unique_ptr<replicated_state_token> token) {
        auto core = factory->constructCore(log->get_global_log_id());
        auto deferred = guardedData.getLockedGuard()->rebuild(std::move(core), std::move(token));
        // execute *after* the lock has been released
        deferred.fire();
    }

    template<typename S>
    auto replicated_state_t<S>::guarded_data::rebuild(std::unique_ptr<CoreType> core,
                                                  std::unique_ptr<replicated_state_token>
                                                      token) -> deferred_action try {
        LOG_CTX("edaef", TRACE, _self.loggerContext) << "replicated state rebuilding - query participant";
        auto participant = _self.log->get_participant();
        if (auto leader = std::dynamic_pointer_cast<replicated_log::ilog_leader>(participant); leader) {
            LOG_CTX("99890", TRACE, _self.loggerContext) << "obtained leader participant";
            return runLeader(std::move(leader), std::move(core), std::move(token));
        } else if (auto follower = std::dynamic_pointer_cast<replicated_log::ilog_follower>(participant); follower) {
            LOG_CTX("f5328", TRACE, _self.loggerContext) << "obtained follower participant";
            return runFollower(std::move(follower), std::move(core), std::move(token));
        } else if (auto unconfiguredLogParticipant =
                       std::dynamic_pointer_cast<replicated_log::LogUnconfiguredParticipant>(participant);
                   unconfiguredLogParticipant) {
            LOG_CTX("ad84b", TRACE, _self.loggerContext) << "obtained unconfigured participant";
            return runUnconfigured(std::move(unconfiguredLogParticipant), std::move(core), std::move(token));
        } else {
            LOG_CTX("33d5f", FATAL, _self.loggerContext) << "Replicated log has an unhandled participant type.";
            std::abort();
        }
    } catch (replication_sdk::replicated_log::participant_resigned_exception const &ex) {
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
    auto replicated_state_t<S>::guarded_data::runFollower(std::shared_ptr<replicated_log::ilog_follower> logFollower,
                                                      std::unique_ptr<CoreType>
                                                          core,
                                                      std::unique_ptr<replicated_state_token>
                                                          token) -> deferred_action try {
        LOG_CTX("95b9d", DEBUG, _self.loggerContext) << "create follower state";

        auto loggerCtx = _self.loggerContext.template with<logContextKeyStateComponent>("follower-manager");
        auto manager = std::make_shared<follower_state_manager<S>>(std::move(loggerCtx), _self.shared_from_this(),
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
    auto replicated_state_t<S>::guarded_data::runLeader(std::shared_ptr<replicated_log::ilog_leader> logLeader,
                                                    std::unique_ptr<CoreType>
                                                        core,
                                                    std::unique_ptr<replicated_state_token>
                                                        token) -> deferred_action try {
        LOG_CTX("95b9d", DEBUG, _self.loggerContext) << "create leader state";

        auto loggerCtx = _self.loggerContext.template with<logContextKeyStateComponent>("leader-manager");
        auto manager = std::make_shared<leader_state_manager<S>>(std::move(loggerCtx), _self.shared_from_this(),
                                                               std::move(logLeader), std::move(core), std::move(token),
                                                               _self.factory);
        currentManager = manager;

        static_assert(noexcept(manager->run()));
        return deferred_action([manager]() noexcept { manager->run(); });
    } catch (std::exception const &e) {
        LOG_CTX("016f3", DEBUG, _self.loggerContext) << "run leader caught exception: " << e.what();
        throw;
    }

    template<typename S>
    auto replicated_state_t<S>::guarded_data::runUnconfigured(
        std::shared_ptr<replicated_log::LogUnconfiguredParticipant> unconfiguredParticipant,
        std::unique_ptr<CoreType> core, std::unique_ptr<replicated_state_token> token) -> deferred_action try {
        LOG_CTX("5d7c6", DEBUG, _self.loggerContext) << "create unconfigured state";
        auto manager = std::make_shared<unconfigured_state_manager<S>>(
            _self.shared_from_this(), std::move(unconfiguredParticipant), std::move(core), std::move(token));
        currentManager = manager;

        static_assert(noexcept(manager->run()));
        return deferred_action([manager]() noexcept { manager->run(); });
    } catch (std::exception const &e) {
        LOG_CTX("6f1eb", DEBUG, _self.loggerContext) << "run unconfigured caught exception: " << e.what();
        throw;
    }

    template<typename S>
    auto replicated_state_t<S>::guarded_data::forceRebuild() -> deferred_action {
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
    auto replicated_state_t<S>::guarded_data::flush(state_generation planGeneration) -> deferred_action {
        auto [core, token, queueAction] = std::move(*currentManager).resign();
        if (token->generation != planGeneration) {
            token = std::make_unique<replicated_state_token>(planGeneration);
        }

        auto runAction = rebuild(std::move(core), std::move(token));
        return deferred_action::combine(std::move(queueAction), std::move(runAction));
    }

}    // namespace nil::dbms::replication_sdk::replicated_state
