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

#include <nil/replication_sdk/replicated_log/log_common.hpp>
#include "streams.hpp"

namespace nil::dbms::replication_sdk::streams {

    template<typename Descriptor>
    struct stream_information_block;
    template<StreamId Id, typename Type, typename Tags>
    struct stream_information_block<stream_descriptor<Id, Type, Tags>> {
        using StreamType = streams::Stream<Type>;
        using EntryType = StreamEntry<Type>;
        using Iterator = typed_log_range_iterator<StreamEntryView<Type>>;

        using ContainerType = ::immer::flex_vector<EntryType, nil::dbms::immer::dbms_memory_policy>;
        using TransientType = typename ContainerType::transient_type;
        using LogVariantType = std::variant<ContainerType, TransientType>;

        using WaitForResult = typename StreamType::wait_for_result;
        using WaitForPromise = futures::Promise<WaitForResult>;
        using WaitForQueue = std::multimap<log_index, WaitForPromise>;

        log_index _releaseIndex {0};
        LogVariantType _container;
        WaitForQueue _waitForQueue;

        auto append_entry(log_index index, Type t);
        auto get_wait_for_resolve_set(log_index commitIndex) -> WaitForQueue;
        auto register_wait_for(log_index index) -> futures::Future<WaitForResult>;
        auto get_iterator() -> std::unique_ptr<Iterator>;
        auto get_iterator_range(log_index start, log_index stop) -> std::unique_ptr<Iterator>;

    private:
        auto get_transient_container() -> TransientType &;
        auto get_persistent_container() -> ContainerType &;
    };

}    // namespace nil::dbms::replication_sdk::streams
