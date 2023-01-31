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

#include "basics/result.h"
#include "basics/result_t.h"
#include <nil/dbms/replication/state/agency_specification.hpp>
#include "state.hpp"
#include <nil/dbms/replication/state/state_token.hpp>
#include "nil/dbms/replication/log/log.hpp"

namespace nil::dbms::replication::algorithms {

    struct StateActionContext {
        virtual ~StateActionContext() = default;

        virtual auto getReplicatedStateById(log_id) noexcept
            -> std::shared_ptr<state::ReplicatedStateBase> = 0;

        virtual auto create_replicated_state(log_id, std::string_view, velocypack::Slice)
            -> ResultT<std::shared_ptr<state::ReplicatedStateBase>> = 0;

        virtual auto dropReplicatedState(log_id) -> Result = 0;
    };

    auto updateReplicatedState(StateActionContext &ctx, std::string const &serverId, log_id id,
                               state::agency::Plan const *spec,
                               state::agency::Current const *current) -> Result;
}    // namespace nil::dbms::replication::algorithms
