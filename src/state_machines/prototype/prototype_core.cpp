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

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::replicated_state;
using namespace nil::dbms::replication::replicated_state::prototype;

prototype_core::prototype_core(global_log_identifier logId, logger_context loggerContext,
                             std::shared_ptr<iprototype_storage_interface> storage) :
    loggerContext(std::move(loggerContext)),
    _logId(std::move(logId)), _storage(std::move(storage)) {
    load_state_from_db();
}

bool prototype_core::flush() {
    if (_lastAppliedIndex <= _lastPersistedIndex + kFlushBatchSize) {
        // no need to flush
        return false;
    }
    auto result = _storage->put(_logId, getDump());
    if (result.ok()) {
        _lastPersistedIndex = _lastAppliedIndex;
        LOG_CTX("af38a", TRACE, loggerContext)
            << "Prototype FLUSH successful, persisted index: " << _lastPersistedIndex;
        return true;
    } else {
        LOG_CTX("af0f6", ERR, loggerContext) << "Prototype FLUSH failed: " << result;
    }
    return false;
}

void prototype_core::load_state_from_db() {
    auto result = _storage->get(_logId);
    if (result.ok()) {
        auto dump = std::move(result).get();
        _lastPersistedIndex = _lastAppliedIndex = dump.lastPersistedIndex;
        for (auto const &[k, v] : dump.map) {
            _store = _store.set(k, v);
        }
        LOG_CTX("e4cfb", TRACE, loggerContext) << "Prototype loaded state from DB, last index: " << _lastAppliedIndex;
    } else {
        THROW_DBMS_EXCEPTION(result.result());
    }
}

auto prototype_core::get_snapshot() -> std::unordered_map<std::string, std::string> {
    auto snapshot = getReadState();
    return std::unordered_map<std::string, std::string> {snapshot.begin(), snapshot.end()};
}

void prototype_core::apply_snapshot(std::unordered_map<std::string, std::string> const &snapshot) {
    // Once the first applyEntries is executed, _lastAppliedIndex will have the
    // correct value.
    for (auto &[k, v] : snapshot) {
        _store = _store.set(k, v);
    }
}

auto prototype_core::getDump() -> prototype_dump {
    // After we write to DB, we set lastPersistedIndex to lastAppliedIndex,
    // because we want to persist the already updated value of lastPersistedIndex.
    return prototype_dump {_lastAppliedIndex, get_snapshot()};
}

auto prototype_core::get(std::string const &key) noexcept -> std::optional<std::string> {
    if (auto it = getReadState().find(key); it != nullptr) {
        return *it;
    }
    return std::nullopt;
}

auto prototype_core::get(std::vector<std::string> const &keys) -> std::unordered_map<std::string, std::string> {
    std::unordered_map<std::string, std::string> result;
    auto snapshot = getReadState();
    for (auto const &it : keys) {
        if (auto found = snapshot.find(it); found != nullptr) {
            result.emplace(it, *found);
        }
    }
    return result;
}

bool prototype_core::compare(std::string const &key, std::string const &value) {
    if (auto it = _store.find(key); it != nullptr) {
        return *it == value;
    }
    return false;
}

auto prototype_core::getReadState() -> StorageType {
    if (_ongoingStates.empty()) {
        // This can happen on followers or before any entries have been applied.
        return _store;
    }
    return _ongoingStates.front().second;
}

void prototype_core::applyToOngoingState(log_index idx, prototype_log_entry const &entry) {
    apply_to_local_store(entry);
    _ongoingStates.emplace_back(idx, _store);
}

auto prototype_core::getLastPersistedIndex() const noexcept -> log_index const & {
    return _lastPersistedIndex;
}

auto prototype_core::getLogId() const noexcept -> global_log_identifier const & {
    return _logId;
}

void prototype_core::apply_to_local_store(prototype_log_entry const &entry) {
    std::visit(overload {[&](prototype_log_entry::insert_operation const &op) {
                             for (auto const &[key, value] : op.map) {
                                 _store = _store.set(key, value);
                             }
                         },
                         [&](prototype_log_entry::delete_operation const &op) {
                             for (auto const &it : op.keys) {
                                 _store = _store.erase(it);
                             }
                         },
                         [&](prototype_log_entry::compare_exchange_operation const &op) {
                             _store = _store.set(op.key, op.newValue);
                         }},
               entry.op);
}

void prototype_dump::toVelocyPack(velocypack::Builder &b) {
    velocypack::serialize<prototype_dump>(b, *this);
}

auto prototype_dump::fromVelocyPack(velocypack::Slice s) -> prototype_dump {
    return velocypack::deserialize<prototype_dump>(s);
}
