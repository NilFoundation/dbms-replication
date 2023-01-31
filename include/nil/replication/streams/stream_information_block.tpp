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
#include <nil/dbms/replication/streams/stream_information_block.hpp>
#include "streams.hpp"

namespace nil::dbms::replication::streams {

    template<StreamId Id, typename Type, typename Tags>
    auto StreamInformationBlock<stream_descriptor<Id, Type, Tags>>::getTransientContainer() -> TransientType & {
        if (!std::holds_alternative<TransientType>(_container)) {
            _container = std::get<ContainerType>(_container).transient();
        }
        return std::get<TransientType>(_container);
    }

    template<StreamId Id, typename Type, typename Tags>
    auto StreamInformationBlock<stream_descriptor<Id, Type, Tags>>::getPersistentContainer() -> ContainerType & {
        if (!std::holds_alternative<ContainerType>(_container)) {
            _container = std::get<TransientType>(_container).persistent();
        }
        return std::get<ContainerType>(_container);
    }

    template<StreamId Id, typename Type, typename Tags>
    auto StreamInformationBlock<stream_descriptor<Id, Type, Tags>>::appendEntry(log_index index, Type t) {
        getTransientContainer().push_back(EntryType {index, std::move(t)});
    }

    template<StreamId Id, typename Type, typename Tags>
    auto StreamInformationBlock<stream_descriptor<Id, Type, Tags>>::getWaitForResolveSet(log_index commitIndex)
        -> std::multimap<log_index, futures::Promise<wait_for_result>> {
        WaitForQueue toBeResolved;
        auto const end = _waitForQueue.upper_bound(commitIndex);
        for (auto it = _waitForQueue.begin(); it != end;) {
            toBeResolved.insert(_waitForQueue.extract(it++));
        }
        return toBeResolved;
    }

    template<StreamId Id, typename Type, typename Tags>
    auto StreamInformationBlock<stream_descriptor<Id, Type, Tags>>::registerWaitFor(log_index index)
        -> futures::Future<wait_for_result> {
        return _waitForQueue.emplace(index, futures::Promise<wait_for_result> {})->second.getFuture();
    }

    template<StreamId Id, typename Type, typename Tags>
    auto StreamInformationBlock<stream_descriptor<Id, Type, Tags>>::getIterator() -> std::unique_ptr<Iterator> {
        auto log = getPersistentContainer();

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
    auto StreamInformationBlock<stream_descriptor<Id, Type, Tags>>::getIteratorRange(log_index start, log_index stop)
        -> std::unique_ptr<Iterator> {
        TRI_ASSERT(stop >= start);

        auto const log = getPersistentContainer();

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

}    // namespace nil::dbms::replication::streams
