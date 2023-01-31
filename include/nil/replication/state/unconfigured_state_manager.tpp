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

#include <nil/dbms/replication/state/unconfigured_state_manager.hpp>

#include <nil/dbms/replication/exceptions/participant_resigned_exception.hpp>

namespace nil::dbms::replication::state {

    template<typename S>
    UnconfiguredStateManager<S>::UnconfiguredStateManager(
        std::shared_ptr<ReplicatedState<S>> const &parent,
        std::shared_ptr<log::LogUnconfiguredParticipant> unconfiguredParticipant,
        std::unique_ptr<CoreType> core, std::unique_ptr<ReplicatedStateToken> token) :
        _parent(parent),
        _unconfiguredParticipant(std::move(unconfiguredParticipant)), _core(std::move(core)), _token(std::move(token)) {
    }

    template<typename S>
    void UnconfiguredStateManager<S>::run() noexcept {
        _unconfiguredParticipant->waitForResign().thenFinal([weak = _parent](futures::Try<futures::Unit> &&result) {
            TRI_ASSERT(result.valid());
            if (result.hasValue()) {
                if (auto self = weak.lock(); self != nullptr) {
                    self->forceRebuild();
                }
            } else if (result.hasException()) {
                // This can be a FutureException(ErrorCode::BrokenPromise), or
                // TRI_ERROR_REPLICATION_REPLICATED_LOG_PARTICIPANT_GONE.
                // In either case, the ReplicatedLog itself is dropped or destroyed
                // (not just the LogParticipant instance of the current term).
                LOG_TOPIC("4ffab", TRACE, Logger::REPLICATED_STATE)
                    << "Replicated log participant is gone. Replicated state will go "
                       "soon as well.";
            } else {
                TRI_ASSERT(false);
            }
        });
    }

    template<typename S>
    auto UnconfiguredStateManager<S>::get_status() const -> StateStatus {
        if (_core == nullptr || _token == nullptr) {
            TRI_ASSERT(_core == nullptr && _token == nullptr);
            throw log::participant_resigned_exception(TRI_ERROR_REPLICATION_REPLICATED_LOG_PARTICIPANT_GONE,
                                                               ADB_HERE);
        }
        UnconfiguredStatus status;
        status.snapshot = _token->snapshot;
        status.generation = _token->generation;
        return StateStatus {.variant = std::move(status)};
    }

    template<typename S>
    auto UnconfiguredStateManager<S>::resign() &&noexcept
        -> std::tuple<std::unique_ptr<CoreType>, std::unique_ptr<ReplicatedStateToken>, deferred_action> {
        return std::make_tuple(std::move(_core), std::move(_token), deferred_action {});
    }

}    // namespace nil::dbms::replication::state
