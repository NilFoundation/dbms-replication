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

#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/persisted_log.hpp>

namespace nil::dbms::replication::log {

    class log_iterator : public log_rangeIterator {
    public:
        using log_type = ::immer::flex_vector<in_memory_logEntry, nil::dbms::immer::dbms_memory_policy>;

        explicit log_iterator(log_type container) :
            _container(std::move(container)), _begin(_container.begin()), _end(_container.end()) {
        }

        auto next() -> std::optional<LogEntryView> override {
            while (_begin != _end) {
                auto const &it = *_begin;
                ++_begin;
                auto const &entry = it.entry();
                if (entry.log_payload()) {
                    return LogEntryView(entry.log_index(), *entry.log_payload());
                }
            }
            return std::nullopt;
        }

        auto range() const noexcept -> log_range override {
            if (_container.empty()) {
                return {log_index {0}, log_index {0}};
            } else {
                return {_container.front().entry().log_index(), _container.back().entry().log_index() + 1};
            }
        }

    private:
        log_type _container;
        log_type::const_iterator _begin;
        log_type::const_iterator _end;
    };

    class InMemorypersisted_logIterator : public persisted_logIterator {
    public:
        using log_type = ::immer::flex_vector<in_memory_logEntry, nil::dbms::immer::dbms_memory_policy>;

        explicit InMemorypersisted_logIterator(log_type container) :
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

    class in_memory_logIterator : public TypedLogIterator<in_memory_logEntry> {
    public:
        using log_type = ::immer::flex_vector<in_memory_logEntry, nil::dbms::immer::dbms_memory_policy>;

        explicit in_memory_logIterator(log_type container) :
            _container(std::move(container)), _begin(_container.begin()), _end(_container.end()) {
        }

        auto next() -> std::optional<in_memory_logEntry> override {
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

}    // namespace nil::dbms::replication::log
