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

#include <map>

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
#include "streams.hpp"

namespace nil::dbms::replication::streams {

    template<typename Descriptor>
    struct StreamInformationBlock;
    template<StreamId Id, typename Type, typename Tags>
    struct StreamInformationBlock<stream_descriptor<Id, Type, Tags>> {
        using StreamType = streams::Stream<Type>;
        using EntryType = StreamEntry<Type>;
        using Iterator = Typedlog_rangeIterator<StreamEntryView<Type>>;

        using ContainerType = ::immer::flex_vector<EntryType, nil::dbms::immer::dbms_memory_policy>;
        using TransientType = typename ContainerType::transient_type;
        using LogVariantType = std::variant<ContainerType, TransientType>;

        using wait_for_result = typename StreamType::wait_for_result;
        using WaitForPromise = futures::Promise<wait_for_result>;
        using WaitForQueue = std::multimap<log_index, WaitForPromise>;

        log_index _releaseIndex {0};
        LogVariantType _container;
        WaitForQueue _waitForQueue;

        auto appendEntry(log_index index, Type t);
        auto getWaitForResolveSet(log_index commitIndex) -> WaitForQueue;
        auto registerWaitFor(log_index index) -> futures::Future<wait_for_result>;
        auto getIterator() -> std::unique_ptr<Iterator>;
        auto getIteratorRange(log_index start, log_index stop) -> std::unique_ptr<Iterator>;

    private:
        auto getTransientContainer() -> TransientType &;
        auto getPersistentContainer() -> ContainerType &;
    };

}    // namespace nil::dbms::replication::streams
