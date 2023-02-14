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

#include <features/ApplicationServer.h>
#include <nil/replication_sdk/replicated_state/replicated_state_feature.hpp>
#include <nil/replication_sdk/replicated_state/replicated_state.hpp>
#include <nil/replication_sdk/state_machines/black_hole/black_hole_state_machine_feature.hpp>
#include <nil/replication_sdk/state_machines/black_hole/black_hole_state_machine.hpp>

using namespace nil::dbms;
using namespace nil::dbms::replication_sdk;
using namespace nil::dbms::replication_sdk::replicated_state;
using namespace nil::dbms::replication_sdk::replicated_state::black_hole;

void BlackHoleStateMachineFeature::start() {
    auto &feature = server().getFeature<replicated_state_app_feature>();
    feature.registerStateType<BlackHoleState>("black-hole");
}

BlackHoleStateMachineFeature::BlackHoleStateMachineFeature(Server &server) : DbmsdFeature {server, *this} {
}
