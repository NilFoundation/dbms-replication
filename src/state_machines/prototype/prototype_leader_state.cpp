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
#include <nil/dbms/replication/state_machines/prototype/prototype_follower_state.hpp>

#include "logger/LogContextKeys.h"

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::state;
using namespace nil::dbms::replication::state::prototype;

prototype_leader_state::prototype_leader_state(std::unique_ptr<prototype_core> core) :
    loggerContext(core->loggerContext.with<logContextKeyStateComponent>("LeaderState")),
    _guarded_data(*this, std::move(core)) {
}

auto prototype_leader_state::resign() &&noexcept -> std::unique_ptr<prototype_core> {
    return _guarded_data.doUnderLock([](auto &data) {
        if (data.didResign()) {
            THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_NOT_LEADER);
        }
        return std::move(data.core);
    });
}

auto prototype_leader_state::recover_entries(std::unique_ptr<EntryIterator> ptr) -> futures::Future<Result> {
    auto [result, action] = _guarded_data.doUnderLock(
        [self = shared_from_this(), ptr = std::move(ptr)](auto &data) mutable -> std::pair<Result, deferred_action> {
            if (data.didResign()) {
                return {Result {TRI_ERROR_CLUSTER_NOT_LEADER}, deferred_action {}};
            }
            auto resolvePromises = data.apply_entries(std::move(ptr));
            return std::make_pair(Result {TRI_ERROR_NO_ERROR}, std::move(resolvePromises));
        });
    return std::move(result);
}

auto prototype_leader_state::set(std::unordered_map<std::string, std::string> entries,
                               prototype_state_methods::PrototypeWriteOptions options) -> futures::Future<log_index> {
    return execute_op(prototype_log_entry::create_insert(std::move(entries)), options);
}

auto prototype_leader_state::compare_exchange(std::string key, std::string oldValue, std::string newValue,
                                           prototype_state_methods::PrototypeWriteOptions options)
    -> futures::Future<ResultT<log_index>> {
    auto [f, da] = _guarded_data.doUnderLock(
        [this, options, key = std::move(key), oldValue = std::move(oldValue), newValue = std::move(newValue)](
            auto &data) mutable -> std::pair<futures::Future<ResultT<log_index>>, deferred_action> {
            if (data.didResign()) {
                THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_NOT_LEADER);
            }

            if (!data.core->compare(key, oldValue)) {
                return std::make_pair(ResultT<log_index>::error(TRI_ERROR_DBMS_CONFLICT), deferred_action());
            }

            auto entry =
                prototype_log_entry::createcompare_exchange(std::move(key), std::move(oldValue), std::move(newValue));
            auto [idx, action] = getStream()->insertDeferred(entry);
            data.core->apply_toOngoing_state(idx, entry);

            if (options.wait_for_applied) {
                return std::make_pair(std::move(data.wait_for_applied(idx)).thenValue([idx = idx](auto &&) {
                    return ResultT<log_index>::success(idx);
                }),
                                      std::move(action));
            }
            return std::make_pair(ResultT<log_index>::success(idx), std::move(action));
        });
    da.fire();
    return std::move(f);
}

auto prototype_leader_state::remove(std::string key, prototype_state_methods::PrototypeWriteOptions options)
    -> futures::Future<log_index> {
    return remove(std::vector<std::string> {std::move(key)}, options);
}

auto prototype_leader_state::remove(std::vector<std::string> keys, prototype_state_methods::PrototypeWriteOptions options)
    -> futures::Future<log_index> {
    return execute_op(prototype_log_entry::create_delete(std::move(keys)), options);
}

auto prototype_leader_state::get(std::vector<std::string> keys, log_index wait_for_applied)
    -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> {
    auto f = _guarded_data.doUnderLock([&](auto &data) {
        if (data.didResign()) {
            THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_NOT_LEADER);
        }

        return data.wait_for_applied(wait_for_applied);
    });

    return std::move(f).thenValue([keys = std::move(keys), weak = weak_from_this()](
                                      auto &&) -> ResultT<std::unordered_map<std::string, std::string>> {
        auto self = weak.lock();
        if (self == nullptr) {
            return {TRI_ERROR_CLUSTER_NOT_LEADER};
        }

        return self->_guarded_data.doUnderLock(
            [&](guarded_data &data) -> ResultT<std::unordered_map<std::string, std::string>> {
                if (data.didResign()) {
                    return {TRI_ERROR_CLUSTER_NOT_LEADER};
                }

                return {data.core->get(keys)};
            });
    });
}

auto prototype_leader_state::get(std::string key, log_index wait_for_applied)
    -> futures::Future<ResultT<std::optional<std::string>>> {
    auto f = _guarded_data.doUnderLock([&](auto &data) {
        if (data.didResign()) {
            THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_NOT_LEADER);
        }

        return data.wait_for_applied(wait_for_applied);
    });

    return std::move(f).thenValue(
        [key = std::move(key), weak = weak_from_this()](auto &&) -> ResultT<std::optional<std::string>> {
            auto self = weak.lock();
            if (self == nullptr) {
                return {TRI_ERROR_CLUSTER_NOT_LEADER};
            }

            return self->_guarded_data.doUnderLock([&](guarded_data &data) -> ResultT<std::optional<std::string>> {
                if (data.didResign()) {
                    return {TRI_ERROR_CLUSTER_NOT_LEADER};
                }

                return {data.core->get(key)};
            });
        });
}

auto prototype_leader_state::get_snapshot(log_index waitForIndex)
    -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> {
    auto f = _guarded_data.doUnderLock([&](auto &data) {
        if (data.didResign()) {
            THROW_DBMS_EXCEPTION(TRI_ERROR_REPLICATION_REPLICATED_LOG_PARTICIPANT_GONE);
        }

        return data.wait_for_applied(waitForIndex);
    });

    return std::move(f).thenValue(
        [weak = weak_from_this()](auto &&) -> ResultT<std::unordered_map<std::string, std::string>> {
            auto self = weak.lock();
            if (self == nullptr) {
                return {TRI_ERROR_REPLICATION_REPLICATED_LOG_PARTICIPANT_GONE};
            }

            return self->_guarded_data.doUnderLock(
                [&](guarded_data &data) -> ResultT<std::unordered_map<std::string, std::string>> {
                    if (data.didResign()) {
                        return {TRI_ERROR_REPLICATION_REPLICATED_LOG_PARTICIPANT_GONE};
                    }

                    return {data.core->get_snapshot()};
                });
        });
}

auto prototype_leader_state::execute_op(prototype_log_entry const &entry,
                                     prototype_state_methods::PrototypeWriteOptions options)
    -> futures::Future<log_index> {
    auto [f, da] = _guarded_data.doUnderLock([&](auto &data) -> std::pair<futures::Future<log_index>, deferred_action> {
        if (data.didResign()) {
            THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_NOT_LEADER);
        }

        auto [idx, action] = getStream()->insertDeferred(entry);
        data.core->apply_to_ongoing_state(idx, entry);

        if (options.wait_for_applied) {
            return std::make_pair(std::move(data.wait_for_applied(idx)).thenValue([idx = idx](auto &&) { return idx; }),
                                  std::move(action));
        }
        return std::make_pair(idx, std::move(action));
    });
    da.fire();
    return std::move(f);
}

auto prototype_leader_state::poll_new_entries() {
    auto stream = getStream();
    return _guarded_data.doUnderLock([&](auto &data) { return stream->waitForIterator(data.nextWaitForIndex); });
}

void prototype_leader_state::handle_poll_result(futures::Future<std::unique_ptr<EntryIterator>> &&pollFuture) {
    std::move(pollFuture).then([weak = weak_from_this()](futures::Try<std::unique_ptr<EntryIterator>> tryResult) {
        auto self = weak.lock();
        if (self == nullptr) {
            return;
        }

        auto result = basics::catchToResultT([&] { return std::move(tryResult).get(); });
        if (result.fail()) {
            THROW_DBMS_EXCEPTION(result.result());
        }

        auto resolvePromises = self->_guarded_data.getLockedGuard()->apply_entries(std::move(result.get()));
        resolvePromises.fire();

        self->handle_poll_result(self->poll_new_entries());
    });
}

auto prototype_leader_state::guarded_data::apply_entries(std::unique_ptr<EntryIterator> ptr) -> deferred_action {
    if (didResign()) {
        THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_NOT_LEADER);
    }
    auto toIndex = ptr->range().to;
    core->update(std::move(ptr));
    nextWaitForIndex = toIndex;

    if (core->flush()) {
        auto stream = self.getStream();
        stream->release(core->get_last_persisted_index());
    }

    auto resolveQueue = std::make_unique<wait_for_appliedQueue>();

    auto const end = wait_for_appliedQueue.lower_bound(nextWaitForIndex);
    for (auto it = wait_for_appliedQueue.begin(); it != end;) {
        resolveQueue->insert(wait_for_appliedQueue.extract(it++));
    }

    return deferred_action([resolveQueue = std::move(resolveQueue)]() noexcept {
        for (auto &p : *resolveQueue) {
            p.second.setValue();
        }
    });
}

auto prototype_leader_state::guarded_data::wait_for_applied(log_index index) -> futures::Future<futures::Unit> {
    if (index < nextWaitForIndex) {
        return futures::Future<futures::Unit> {std::in_place};
    }
    auto it = wait_for_appliedQueue.emplace(index, wait_for_appliedPromise {});
    auto f = it->second.getFuture();
    TRI_ASSERT(f.valid());
    return f;
}

void prototype_leader_state::on_snapshot_completed() {
    handle_poll_result(poll_new_entries());
}

auto prototype_leader_state::wait_for_applied(log_index waitForIndex) -> futures::Future<futures::Unit> {
    return _guarded_data.getLockedGuard()->wait_for_applied(waitForIndex);
}

#include <nil/dbms/replication/state/state.tpp>
