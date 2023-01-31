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

#include "basics/exceptions.h"
#include "basics/voc_errors.h"

#include <memory>

namespace nil::dbms::replication::log {
    struct LogUnconfiguredParticipant;
}

namespace nil::dbms::replication::state {

    template<typename S>
    struct UnconfiguredStateManager : ReplicatedState<S>::IStateManager,
                                      std::enable_shared_from_this<UnconfiguredStateManager<S>> {
        using Factory = typename ReplicatedStateTraits<S>::FactoryType;
        using EntryType = typename ReplicatedStateTraits<S>::EntryType;
        using FollowerType = typename ReplicatedStateTraits<S>::FollowerType;
        using LeaderType = typename ReplicatedStateTraits<S>::LeaderType;
        using CoreType = typename ReplicatedStateTraits<S>::CoreType;

        using wait_for_appliedQueue = typename ReplicatedState<S>::IStateManager::wait_for_appliedQueue;
        using wait_for_appliedPromise = typename ReplicatedState<S>::IStateManager::wait_for_appliedQueue;

        UnconfiguredStateManager(std::shared_ptr<ReplicatedState<S>> const &parent,
                                 std::shared_ptr<log::LogUnconfiguredParticipant>
                                     unconfiguredParticipant,
                                 std::unique_ptr<CoreType>
                                     core,
                                 std::unique_ptr<ReplicatedStateToken>
                                     token);

        void run() noexcept override;

        [[nodiscard]] auto get_status() const -> StateStatus override;

        [[nodiscard]] auto resign() &&noexcept
            -> std::tuple<std::unique_ptr<CoreType>, std::unique_ptr<ReplicatedStateToken>, deferred_action> override;

    private:
        std::weak_ptr<ReplicatedState<S>> _parent;
        std::shared_ptr<log::LogUnconfiguredParticipant> _unconfiguredParticipant;
        std::unique_ptr<CoreType> _core;
        std::unique_ptr<ReplicatedStateToken> _token;
    };
}    // namespace nil::dbms::replication::state
