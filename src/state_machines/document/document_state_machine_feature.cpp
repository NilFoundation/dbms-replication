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

#include <nil/dbms/replication/state_machines/document/document_state_machine_feature.hpp>
#include <nil/dbms/replication/state_machines/document/document_state_machine.hpp>
#include <nil/dbms/replication/state/state_feature.hpp>
#include <features/ApplicationServer.h>

using namespace nil::dbms::replication::state::document;

void DocumentStateMachineFeature::start() {
    auto &feature = server().getFeature<replicated_state_app_feature>();
    feature.registerStateType<DocumentState>("document");
}

DocumentStateMachineFeature::DocumentStateMachineFeature(Server &server) : DbmsdFeature {server, *this} {
}
