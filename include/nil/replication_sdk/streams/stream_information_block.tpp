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
#include <nil/replication_sdk/streams/stream_information_block.hpp>
#include "streams.hpp"

namespace nil::dbms::replication_sdk::streams {

    template<StreamId Id, typename Type, typename Tags>
    auto stream_information_block<stream_descriptor<Id, Type, Tags>>::get_transient_container() -> TransientType & {
        if (!std::holds_alternative<TransientType>(_container)) {
            _container = std::get<ContainerType>(_container).transient();
        }
        return std::get<TransientType>(_container);
    }

    template<StreamId Id, typename Type, typename Tags>
    auto stream_information_block<stream_descriptor<Id, Type, Tags>>::get_persistent_container() -> ContainerType & {
        if (!std::holds_alternative<ContainerType>(_container)) {
            _container = std::get<TransientType>(_container).persistent();
        }
        return std::get<ContainerType>(_container);
    }

    template<StreamId Id, typename Type, typename Tags>
    auto stream_information_block<stream_descriptor<Id, Type, Tags>>::append_entry(log_index index, Type t) {
        get_transient_container().push_back(EntryType {index, std::move(t)});
    }

    template<StreamId Id, typename Type, typename Tags>
    auto stream_information_block<stream_descriptor<Id, Type, Tags>>::get_wait_for_resolve_set(log_index commitIndex)
        -> std::multimap<log_index, futures::Promise<WaitForResult>> {
        WaitForQueue toBeResolved;
        auto const end = _waitForQueue.upper_bound(commitIndex);
        for (auto it = _waitForQueue.begin(); it != end;) {
            toBeResolved.insert(_waitForQueue.extract(it++));
        }
        return toBeResolved;
    }

    template<StreamId Id, typename Type, typename Tags>
    auto stream_information_block<stream_descriptor<Id, Type, Tags>>::register_wait_for(log_index index)
        -> futures::Future<WaitForResult> {
        return _waitForQueue.emplace(index, futures::Promise<WaitForResult> {})->second.getFuture();
    }

    template<StreamId Id, typename Type, typename Tags>
    auto stream_information_block<stream_descriptor<Id, Type, Tags>>::get_iterator() -> std::unique_ptr<Iterator> {
        auto log = get_persistent_container();

        struct Iterator : typed_log_range_iterator<StreamEntryView<Type>> {
            ContainerType log;
            typename ContainerType::iterator current;

            std::optional<StreamEntryView<Type>> next() override {
                if (current != std::end(log)) {
                    auto view = std::make_pair(current->first, std::cref(current->second));
                    ++current;
                    return view;
                }
                return std::nullopt;
            }

            [[nodiscard]] log_range range() const noexcept override {
                abort();    // TODO
            }

            explicit Iterator(ContainerType log) : log(std::move(log)), current(this->log.begin()) {
            }
        };

        return std::make_unique<Iterator>(std::move(log));
    }

    template<StreamId Id, typename Type, typename Tags>
    auto stream_information_block<stream_descriptor<Id, Type, Tags>>::get_iterator_range(log_index start, log_index stop)
        -> std::unique_ptr<Iterator> {
        TRI_ASSERT(stop >= start);

        auto const log = get_persistent_container();

        using ContainerIterator = typename ContainerType::iterator;

        struct Iterator : typed_log_range_iterator<StreamEntryView<Type>> {
            ContainerType _log;
            ContainerIterator _current;
            log_index _start, _stop;

            std::optional<StreamEntryView<Type>> next() override {
                if (_current != std::end(_log) && _current->first < _stop) {
                    auto view = std::make_pair(_current->first, std::cref(_current->second));
                    ++_current;
                    return view;
                }
                return std::nullopt;
            }
            [[nodiscard]] log_range range() const noexcept override {
                return {_start, _stop};
            }

            explicit Iterator(ContainerType log, log_index start, log_index stop) :
                _log(std::move(log)),
                _current(
                    std::lower_bound(std::begin(_log), std::end(_log), start,
                                     [](StreamEntry<Type> const &left, log_index index) { return left.first < index; })),
                _start(start), _stop(stop) {
            }
        };
        return std::make_unique<Iterator>(std::move(log), start, stop);
    }

}    // namespace nil::dbms::replication_sdk::streams
