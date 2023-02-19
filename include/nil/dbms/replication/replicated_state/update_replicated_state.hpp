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
#include "agency_specification.hpp"
#include "replicated_state.hpp"
#include "replicated_state_token.hpp"
#include <nil/dbms/replication/replicated_log/replicated_log.hpp>

namespace nil::dbms::replication::algorithms {

    struct state_action_context {
        virtual ~state_action_context() = default;

        virtual auto get_replicated_state_by_id(LogId) noexcept
            -> std::shared_ptr<replicated_state::replicated_state_base> = 0;

        virtual auto createReplicatedState(LogId, std::string_view, velocypack::Slice)
            -> ResultT<std::shared_ptr<replicated_state::replicated_state_base>> = 0;

        virtual auto dropReplicatedState(LogId) -> Result = 0;
    };

    auto updateReplicatedState(state_action_context &ctx, std::string const &serverId, LogId id,
                               replicated_state::agency::Plan const *spec,
                               replicated_state::agency::Current const *current) -> Result;
}    // namespace nil::dbms::replication::algorithms
