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
#include <nil/dbms/replication/state_machines/prototype/prototype_core.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_follower_state.hpp>

#include "logger/LogContextKeys.h"

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::state;
using namespace nil::dbms::replication::state::prototype;

prototype_follower_state::prototype_follower_state(std::unique_ptr<prototype_core> core,
                                               std::shared_ptr<iprototype_network_interface>
                                                   networkInterface) :
    loggerContext(core->loggerContext.with<logContextKeyStateComponent>("FollowerState")),
    _log_identifier(core->getlog_id()), _networkInterface(std::move(networkInterface)), _guarded_data(std::move(core)) {
}

auto prototype_follower_state::acquireSnapshot(ParticipantId const &destination, log_index waitForIndex) noexcept
    -> futures::Future<Result> {
    ResultT<std::shared_ptr<iprototype_leader_interface>> leader = _networkInterface->getLeaderInterface(destination);
    if (leader.fail()) {
        return leader.result();
    }
    return leader.get()
        ->get_snapshot(_log_identifier, waitForIndex)
        .thenValue([self = shared_from_this()](auto &&result) mutable -> Result {
            if (result.fail()) {
                return result.result();
            }

            auto &map = result.get();
            LOG_CTX("85e5a", TRACE, self->loggerContext) << " acquired snapshot of size: " << map.size();
            self->_guarded_data.doUnderLock([&](auto &core) { core->apply_snapshot(map); });
            return TRI_ERROR_NO_ERROR;
        });
}

auto prototype_follower_state::apply_entries(std::unique_ptr<EntryIterator> ptr) noexcept -> futures::Future<Result> {
    return _guarded_data.doUnderLock([self = shared_from_this(), ptr = std::move(ptr)](auto &core) mutable {
        if (!core) {
            return Result {TRI_ERROR_CLUSTER_NOT_FOLLOWER};
        }
        core->apply_entries(std::move(ptr));
        if (core->flush()) {
            auto stream = self->getStream();
            stream->release(core->get_last_persisted_index());
        }
        return Result {TRI_ERROR_NO_ERROR};
    });
}

auto prototype_follower_state::resign() &&noexcept -> std::unique_ptr<prototype_core> {
    return _guarded_data.doUnderLock([](auto &core) {
        TRI_ASSERT(core != nullptr);
        return std::move(core);
    });
}

auto prototype_follower_state::get(std::string key, log_index waitForIndex)
    -> futures::Future<ResultT<std::optional<std::string>>> {
    return wait_for_applied(waitForIndex)
        .thenValue([key = std::move(key), weak = weak_from_this()](auto &&) -> ResultT<std::optional<std::string>> {
            auto self = weak.lock();
            if (self == nullptr) {
                return {TRI_ERROR_CLUSTER_NOT_FOLLOWER};
            }
            return self->_guarded_data.doUnderLock([&](auto &core) -> ResultT<std::optional<std::string>> {
                if (!core) {
                    return {TRI_ERROR_CLUSTER_NOT_FOLLOWER};
                }
                return core->get(key);
            });
        });
}

auto prototype_follower_state::get(std::vector<std::string> keys, log_index waitForIndex)
    -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> {
    return wait_for_applied(waitForIndex)
        .thenValue([keys = std::move(keys),
                    weak = weak_from_this()](auto &&) -> ResultT<std::unordered_map<std::string, std::string>> {
            auto self = weak.lock();
            if (self == nullptr) {
                return {TRI_ERROR_CLUSTER_NOT_FOLLOWER};
            }
            return self->_guarded_data.doUnderLock(
                [&](auto &core) -> ResultT<std::unordered_map<std::string, std::string>> {
                    if (!core) {
                        return {TRI_ERROR_CLUSTER_NOT_FOLLOWER};
                    }
                    return core->get(keys);
                });
        });
}

#include <nil/dbms/replication/state/state.tpp>
