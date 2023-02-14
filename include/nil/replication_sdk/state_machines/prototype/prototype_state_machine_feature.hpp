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

#include <nil/replication_sdk/dbmsd/dbmsd.hpp>

namespace nil::dbms::replication_sdk::replicated_state::prototype {

    struct prototype_state_machine_feature : public DbmsdFeature {
        constexpr static const char *name() noexcept {
            return "PrototypeStateMachine";
        }

        explicit prototype_state_machine_feature(Server &server);
        void prepare() override;
        void start() override;
    };

}    // namespace nil::dbms::replication_sdk::replicated_state::prototype
