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

#include <nil/dbms/replication/state_machines/prototype/prototype_leader_state.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_state_machine.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_core.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_follower_state.hpp>

#include "futures/Future.h"
#include "logger/LogContextKeys.h"
#include "velocypack/Iterator.h"

#include <nil/dbms/replication/replicated_log/log_common.hpp>

#include <utility>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::replicated_state;
using namespace nil::dbms::replication::replicated_state::prototype;

auto prototype_factory::constructFollower(std::unique_ptr<prototype_core> core)
    -> std::shared_ptr<prototype_follower_state> {
    return std::make_shared<prototype_follower_state>(std::move(core), networkInterface);
}

auto prototype_factory::constructLeader(std::unique_ptr<prototype_core> core) -> std::shared_ptr<prototype_leader_state> {
    return std::make_shared<prototype_leader_state>(std::move(core));
}

auto prototype_factory::constructCore(global_log_identifier const &gid) -> std::unique_ptr<prototype_core> {
    logger_context const logContext = logger_context(Logger::REPLICATED_STATE).with<logContextKeyLogId>(gid.id);
    return std::make_unique<prototype_core>(gid, logContext, storageInterface);
}

prototype_factory::prototype_factory(std::shared_ptr<iprototype_network_interface> networkInterface,
                                   std::shared_ptr<iprototype_storage_interface>
                                       storageInterface) :
    networkInterface(std::move(networkInterface)),
    storageInterface(std::move(storageInterface)) {
}

#include <nil/dbms/replication/replicated_state/replicated_state.tpp>

template struct replicated_state::replicated_state_t<prototype_state>;
