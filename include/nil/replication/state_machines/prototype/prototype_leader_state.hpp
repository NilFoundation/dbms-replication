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

#include "inspection/vpack.h"

#include "prototype_core.hpp"
#include "prototype_state_machine.hpp"
#include "prototype_state_methods.hpp"

#include <memory>

namespace nil::dbms::replication::state::prototype {
    struct prototype_leader_state : IReplicatedLeaderState<prototype_state>,
                                  std::enable_shared_from_this<prototype_leader_state> {
        using wait_for_appliedPromise = futures::Promise<futures::Unit>;
        using wait_for_appliedQueue = std::multimap<log_index, wait_for_appliedPromise>;

        explicit prototype_leader_state(std::unique_ptr<prototype_core> core);

        [[nodiscard]] auto resign() &&noexcept -> std::unique_ptr<prototype_core> override;

        auto recover_entries(std::unique_ptr<EntryIterator> ptr) -> futures::Future<Result> override;

        void on_snapshot_completed() override;

        auto set(std::unordered_map<std::string, std::string> entries, prototype_state_methods::PrototypeWriteOptions)
            -> futures::Future<log_index>;

        auto compare_exchange(std::string key, std::string oldValue, std::string newValue,
                             prototype_state_methods::PrototypeWriteOptions options)
            -> futures::Future<ResultT<log_index>>;

        auto remove(std::string key, prototype_state_methods::PrototypeWriteOptions) -> futures::Future<log_index>;
        auto remove(std::vector<std::string> keys, prototype_state_methods::PrototypeWriteOptions)
            -> futures::Future<log_index>;

        auto get(std::string key, log_index wait_for_applied) -> futures::Future<ResultT<std::optional<std::string>>>;
        auto get(std::vector<std::string> keys, log_index wait_for_applied)
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>>;

        auto get_snapshot(log_index waitForIndex)
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>>;

        auto wait_for_applied(log_index waitForIndex) -> futures::Future<futures::Unit>;

        logger_context const loggerContext;

    private:
        auto execute_op(prototype_log_entry const &, prototype_state_methods::PrototypeWriteOptions)
            -> futures::Future<log_index>;
        auto poll_new_entries();
        void handle_poll_result(futures::Future<std::unique_ptr<EntryIterator>> &&pollFuture);

        struct guarded_data {
            explicit guarded_data(prototype_leader_state &self, std::unique_ptr<prototype_core> core) :
                self(self), core(std::move(core)), nextWaitForIndex {1} {};

            [[nodiscard]] auto apply_entries(std::unique_ptr<EntryIterator> ptr) -> deferred_action;

            auto wait_for_applied(log_index index) -> futures::Future<futures::Unit>;

            [[nodiscard]] bool didResign() const noexcept {
                return core == nullptr;
            }

            prototype_leader_state &self;
            std::unique_ptr<prototype_core> core;
            wait_for_appliedQueue wait_for_appliedQueue;
            log_index nextWaitForIndex;
        };

        Guarded<guarded_data, basics::UnshackledMutex> _guarded_data;
    };
}    // namespace nil::dbms::replication::state::prototype
