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

#include "basics/exceptions.h"
#include "basics/voc_errors.h"

#include <memory>

namespace nil::dbms::replication_sdk::replicated_log {
    struct LogUnconfiguredParticipant;
}

namespace nil::dbms::replication_sdk::replicated_state {

    template<typename S>
    struct unconfigured_state_manager : replicated_state_t<S>::is_state_manager,
                                      std::enable_shared_from_this<unconfigured_state_manager<S>> {
        using Factory = typename replicated_state_traits<S>::FactoryType;
        using EntryType = typename replicated_state_traits<S>::EntryType;
        using FollowerType = typename replicated_state_traits<S>::FollowerType;
        using LeaderType = typename replicated_state_traits<S>::LeaderType;
        using CoreType = typename replicated_state_traits<S>::CoreType;

        using WaitForAppliedQueue = typename replicated_state_t<S>::is_state_manager::WaitForAppliedQueue;
        using WaitForAppliedPromise = typename replicated_state_t<S>::is_state_manager::WaitForAppliedQueue;

        unconfigured_state_manager(std::shared_ptr<replicated_state_t<S>> const &parent,
                                 std::shared_ptr<replicated_log::LogUnconfiguredParticipant>
                                     unconfiguredParticipant,
                                 std::unique_ptr<CoreType>
                                     core,
                                 std::unique_ptr<replicated_state_token>
                                     token);

        void run() noexcept override;

        [[nodiscard]] auto getStatus() const -> state_status override;

        [[nodiscard]] auto resign() &&noexcept
            -> std::tuple<std::unique_ptr<CoreType>, std::unique_ptr<replicated_state_token>, deferred_action> override;

    private:
        std::weak_ptr<replicated_state_t<S>> _parent;
        std::shared_ptr<replicated_log::LogUnconfiguredParticipant> _unconfiguredParticipant;
        std::unique_ptr<CoreType> _core;
        std::unique_ptr<replicated_state_token> _token;
    };
}    // namespace nil::dbms::replication_sdk::replicated_state
