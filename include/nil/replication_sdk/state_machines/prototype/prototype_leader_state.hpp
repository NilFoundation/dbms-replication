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

namespace nil::dbms::replication_sdk::replicated_state::prototype {
    struct prototype_leader_state : ireplicated_leader_state<prototype_state>,
                                  std::enable_shared_from_this<prototype_leader_state> {
        using WaitForAppliedPromise = futures::Promise<futures::Unit>;
        using WaitForAppliedQueue = std::multimap<log_index, WaitForAppliedPromise>;

        explicit prototype_leader_state(std::unique_ptr<prototype_core> core);

        [[nodiscard]] auto resign() &&noexcept -> std::unique_ptr<prototype_core> override;

        auto recoverEntries(std::unique_ptr<EntryIterator> ptr) -> futures::Future<Result> override;

        void onSnapshotCompleted() override;

        auto set(std::unordered_map<std::string, std::string> entries, prototype_state_methods::prototype_write_options)
            -> futures::Future<log_index>;

        auto compareExchange(std::string key, std::string oldValue, std::string newValue,
                             prototype_state_methods::prototype_write_options options)
            -> futures::Future<ResultT<log_index>>;

        auto remove(std::string key, prototype_state_methods::prototype_write_options) -> futures::Future<log_index>;
        auto remove(std::vector<std::string> keys, prototype_state_methods::prototype_write_options)
            -> futures::Future<log_index>;

        auto get(std::string key, log_index waitForApplied) -> futures::Future<ResultT<std::optional<std::string>>>;
        auto get(std::vector<std::string> keys, log_index waitForApplied)
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>>;

        auto getSnapshot(log_index waitForIndex)
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>>;

        auto waitForApplied(log_index waitForIndex) -> futures::Future<futures::Unit>;

        logger_context const loggerContext;

    private:
        auto executeOp(prototype_log_entry const &, prototype_state_methods::prototype_write_options)
            -> futures::Future<log_index>;
        auto pollNewEntries();
        void handlePollResult(futures::Future<std::unique_ptr<EntryIterator>> &&pollFuture);

        struct guarded_data {
            explicit guarded_data(prototype_leader_state &self, std::unique_ptr<prototype_core> core) :
                self(self), core(std::move(core)), nextWaitForIndex {1} {};

            [[nodiscard]] auto apply_entries(std::unique_ptr<EntryIterator> ptr) -> deferred_action;

            auto waitForApplied(log_index index) -> futures::Future<futures::Unit>;

            [[nodiscard]] bool didResign() const noexcept {
                return core == nullptr;
            }

            prototype_leader_state &self;
            std::unique_ptr<prototype_core> core;
            WaitForAppliedQueue waitForAppliedQueue;
            log_index nextWaitForIndex;
        };

        Guarded<guarded_data, basics::UnshackledMutex> _guardedData;
    };
}    // namespace nil::dbms::replication_sdk::replicated_state::prototype
