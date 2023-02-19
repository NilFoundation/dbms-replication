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

#include "prototype_log_entry.hpp"

#include "basics/overload.h"
#include <nil/dbms/replication/logger_context.hpp>
#include <nil/dbms/replication/replicated_log/log_common.hpp>

#if (_MSC_VER >= 1)
// suppress warnings:
#pragma warning(push)
// conversion from 'size_t' to 'immer::detail::rbts::count_t', possible loss of
// data
#pragma warning(disable : 4267)
// result of 32-bit shift implicitly converted to 64 bits (was 64-bit shift
// intended?)
#pragma warning(disable : 4334)
#endif
#include <immer/map.hpp>
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

#include <deque>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>

namespace nil::dbms {
    struct logger_context;
}    // namespace nil::dbms

namespace nil::dbms::velocypack {
    class Builder;
    class Slice;
}    // namespace nil::dbms::velocypack

namespace nil::dbms::replication::replicated_state::prototype {

    struct iprototype_storage_interface;
    struct prototype_log_entry;

    struct prototype_dump {
        replication::log_index lastPersistedIndex;
        std::unordered_map<std::string, std::string> map;

        void toVelocyPack(velocypack::Builder &);
        static auto fromVelocyPack(velocypack::Slice) -> prototype_dump;
    };

    template<class Inspector>
    auto inspect(Inspector &f, prototype_dump &x) {
        return f.object(x).fields(f.field("lastPersistedIndex", x.lastPersistedIndex), f.field("map", x.map));
    }

    struct prototype_core {
        using StorageType = ::immer::map<std::string, std::string>;
        static constexpr std::size_t kFlushBatchSize = 1000;

        explicit prototype_core(global_log_identifier logId, logger_context loggerContext,
                                std::shared_ptr<iprototype_storage_interface> storage);

        template<typename EntryIterator>
        void apply_entries(std::unique_ptr<EntryIterator> ptr);

        template<typename EntryIterator>
        void update(std::unique_ptr<EntryIterator> ptr);

        auto get_snapshot() -> std::unordered_map<std::string, std::string>;
        void apply_snapshot(std::unordered_map<std::string, std::string> const &snapshot);

        bool flush();
        void load_state_from_db();

        auto getDump() -> prototype_dump;
        auto get(std::string const &key) noexcept -> std::optional<std::string>;
        auto get(std::vector<std::string> const &keys) -> std::unordered_map<std::string, std::string>;

        bool compare(std::string const &key, std::string const &value);

        auto getReadState() -> StorageType;
        void applyToOngoingState(log_index, prototype_log_entry const &);

        [[nodiscard]] auto getLastPersistedIndex() const noexcept -> log_index const &;
        [[nodiscard]] auto getLogId() const noexcept -> global_log_identifier const &;

        logger_context const loggerContext;

    private:
        void apply_to_local_store(prototype_log_entry const &entry);

    private:
        global_log_identifier _logId;
        log_index _lastPersistedIndex;
        log_index _lastAppliedIndex;
        StorageType _store;
        std::shared_ptr<iprototype_storage_interface> _storage;
        std::deque<std::pair<log_index, StorageType>> _ongoingStates;
    };

    /*
     * Ensure this stays idempotent.
     */
    template<typename EntryIterator>
    void prototype_core::apply_entries(std::unique_ptr<EntryIterator> ptr) {
        auto lastAppliedIndex = ptr->range().to.saturated_decrement();
        while (auto entry = ptr->next()) {
            prototype_log_entry const &logEntry = entry->second;
            apply_to_local_store(logEntry);
        }
        _lastAppliedIndex = std::move(lastAppliedIndex);
    }

    /*
     * Advances through the deque.
     */
    template<typename EntryIterator>
    void prototype_core::update(std::unique_ptr<EntryIterator> ptr) {
        // Meta-entries are never seen by the state machine, but still increase the
        // log index, creating gaps between ongoing states. Hence,
        // lastIndexToApply could be greater than the last index of the current
        // ongoing state, but smaller than that of the next ongoing state, in which
        // case we prefer to keep the current one. We have to look ahead in the
        // deque to make sure this stays correct.
        auto lastIndexToApply = ptr->range().to.saturated_decrement();
        while (_ongoingStates.size() > 1 && _ongoingStates[1].first <= lastIndexToApply) {
            _ongoingStates.pop_front();
        }
        _lastAppliedIndex = lastIndexToApply;
    }

}    // namespace nil::dbms::replication::replicated_state::prototype
