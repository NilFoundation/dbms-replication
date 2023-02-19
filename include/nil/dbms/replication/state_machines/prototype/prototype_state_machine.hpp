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

#include "basics/result_t.h"
#include "basics/unshackled_mutex.h"
#include "futures/Future.h"
#include <nil/dbms/replication/replicated_state/replicated_state.hpp>
#include <nil/dbms/replication/replicated_state/state_interfaces.hpp>

#include <string>
#include <unordered_map>

namespace rocksdb {
    class TransactionDB;
}

namespace nil::dbms::replication::replicated_state {
    /**
     * This prototype state machine acts as a simple key value store. It is meant to
     * be used during integration tests.
     * Data is persisted.
     * Snapshot transfers are supported.
     */
    namespace prototype {

        struct prototype_factory;
        struct prototype_log_entry;
        struct prototype_leader_state;
        struct prototype_follower_state;
        struct prototype_core;
        struct prototype_dump;

        struct prototype_state {
            using LeaderType = prototype_leader_state;
            using FollowerType = prototype_follower_state;
            using EntryType = prototype_log_entry;
            using FactoryType = prototype_factory;
            using CoreType = prototype_core;
        };

        struct iprototype_leader_interface {
            virtual ~iprototype_leader_interface() = default;
            virtual auto getSnapshot(global_log_identifier const &logId, log_index waitForIndex)
                -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> = 0;
        };

        struct iprototype_network_interface {
            virtual ~iprototype_network_interface() = default;
            virtual auto getLeaderInterface(ParticipantId id)
                -> ResultT<std::shared_ptr<iprototype_leader_interface>> = 0;
        };

        struct iprototype_storage_interface {
            virtual ~iprototype_storage_interface() = default;
            virtual auto put(global_log_identifier const &logId, prototype_dump dump) -> Result = 0;
            virtual auto get(global_log_identifier const &logId) -> ResultT<prototype_dump> = 0;
        };

        struct prototype_factory {
            explicit prototype_factory(std::shared_ptr<iprototype_network_interface> networkInterface,
                                       std::shared_ptr<iprototype_storage_interface>
                                           storageInterface);

            auto constructFollower(std::unique_ptr<prototype_core> core) -> std::shared_ptr<prototype_follower_state>;
            auto constructLeader(std::unique_ptr<prototype_core> core) -> std::shared_ptr<prototype_leader_state>;
            auto constructCore(global_log_identifier const &) -> std::unique_ptr<prototype_core>;

            std::shared_ptr<iprototype_network_interface> const networkInterface;
            std::shared_ptr<iprototype_storage_interface> const storageInterface;
        };
    }    // namespace prototype

    extern template struct replicated_state::replicated_state_t<prototype::prototype_state>;
}    // namespace nil::dbms::replication::replicated_state
