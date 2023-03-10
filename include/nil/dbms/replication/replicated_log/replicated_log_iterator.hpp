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
#include <optional>

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
#include <immer/flex_vector.hpp>
#include <immer/flex_vector_transient.hpp>
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

#include "log_common.hpp"
#include "persisted_log.hpp"

namespace nil::dbms::replication::replicated_log {

    class replicated_log_iterator : public LogRangeIterator {
    public:
        using log_type = ::immer::flex_vector<inmemory_log_entry, nil::dbms::immer::dbms_memory_policy>;

        explicit replicated_log_iterator(log_type container) :
            _container(std::move(container)), _begin(_container.begin()), _end(_container.end()) {
        }

        auto next() -> std::optional<log_entry_view> override {
            while (_begin != _end) {
                auto const &it = *_begin;
                ++_begin;
                auto const &entry = it.entry();
                if (entry.logPayload()) {
                    return log_entry_view(entry.logIndex(), *entry.logPayload());
                }
            }
            return std::nullopt;
        }

        auto range() const noexcept -> log_range override {
            if (_container.empty()) {
                return {log_index {0}, log_index {0}};
            } else {
                return {_container.front().entry().logIndex(), _container.back().entry().logIndex() + 1};
            }
        }

    private:
        log_type _container;
        log_type::const_iterator _begin;
        log_type::const_iterator _end;
    };

    class in_memory_persisted_log_iterator : public persisted_log_iterator {
    public:
        using log_type = ::immer::flex_vector<inmemory_log_entry, nil::dbms::immer::dbms_memory_policy>;

        explicit in_memory_persisted_log_iterator(log_type container) :
            _container(std::move(container)), _begin(_container.begin()), _end(_container.end()) {
        }

        auto next() -> std::optional<persisting_log_entry> override {
            if (_begin != _end) {
                auto const &it = *_begin;
                ++_begin;
                return it.entry();
            }
            return std::nullopt;
        }

    private:
        log_type _container;
        log_type::const_iterator _begin;
        log_type::const_iterator _end;
    };

    class in_memory_log_iterator : public typed_log_iterator<inmemory_log_entry> {
    public:
        using log_type = ::immer::flex_vector<inmemory_log_entry, nil::dbms::immer::dbms_memory_policy>;

        explicit in_memory_log_iterator(log_type container) :
            _container(std::move(container)), _begin(_container.begin()), _end(_container.end()) {
        }

        auto next() -> std::optional<inmemory_log_entry> override {
            if (_begin != _end) {
                auto const &it = *_begin;
                ++_begin;
                return it;
            }
            return std::nullopt;
        }

    private:
        log_type _container;
        log_type::const_iterator _begin;
        log_type::const_iterator _end;
    };

}    // namespace nil::dbms::replication::replicated_log
