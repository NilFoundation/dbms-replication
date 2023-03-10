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

#include "state_common.hpp"

namespace nil::dbms::replication::replicated_state {

    /**
     * The ReplicatedStateToken contains the snapshot information and is bound
     * to a single generation.
     */
    struct replicated_state_token {
        explicit replicated_state_token(state_generation generation) : generation(generation) {
        }

        state_generation const generation;
        snapshot_info snapshot;

        static auto withExplicitSnapshotStatus(state_generation generation, snapshot_info snapshot)
            -> replicated_state_token {
            return {generation, std::move(snapshot)};
        }

    private:
        replicated_state_token(state_generation generation, snapshot_info snapshot) :
            generation(generation), snapshot(std::move(snapshot)) {
        }
    };

}    // namespace nil::dbms::replication::replicated_state
