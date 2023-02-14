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

#include <nil/replication_sdk/replicated_log/log_common.hpp>
#include <nil/replication_sdk/replicated_log/log_entries.hpp>
#include <nil/replication_sdk/streams/stream_specification.hpp>

namespace nil::dbms {
    struct deferred_action;
}

namespace nil::dbms::futures {
    template<typename>
    class Future;
}

namespace nil::dbms::replication_sdk::streams {

    /**
     * Object returned by a stream iterator. Allows read only access
     * to the stored object. The view does not own the value and remains
     * valid until the iterator is destroyed or next() is called.
     * @tparam T Object Type
     */
    template<typename T>
    using StreamEntryView = std::pair<log_index, T const &>;
    template<typename T>
    using StreamEntry = std::pair<log_index, T>;

    /**
     * Consumer interface for a multiplexed object stream. Provides methods for
     * iteraction with the replicated logs stream.
     * @tparam T Object Type
     */
    template<typename T>
    struct Stream {
        virtual ~Stream() = default;

        struct wait_for_result { };
        virtual auto wait_for(log_index) -> futures::Future<wait_for_result> = 0;

        using Iterator = typed_log_range_iterator<StreamEntryView<T>>;
        virtual auto wait_for_iterator(log_index) -> futures::Future<std::unique_ptr<Iterator>> = 0;

        virtual auto release(log_index) -> void = 0;
    };

    /**
     * Producing interface for a multiplexed object stream. Besides the Stream<T>
     * methods it additionally provides a insert method.
     * @tparam T Object Type
     */
    template<typename T>
    struct producer_stream : Stream<T> {
        virtual auto insert(T const &) -> log_index = 0;
        virtual auto insert_deferred(T const &) -> std::pair<log_index, deferred_action> = 0;
    };

    /**
     * StreamGenericBase is the base for all Stream implementations. In general
     * users don't need to access this object directly. It provides more information
     * about the stream.
     * @tparam Descriptor The associated stream descriptor.
     * @tparam StreamType Either Stream<T> or ProducerStream<T>.
     * @tparam Type Object Type, default is extracted from Descriptor
     */
    template<typename Descriptor, template<typename> typename StreamType,
             typename Type = stream_descriptor_type_t<Descriptor>>
    struct stream_generic_base : StreamType<Type> {
        static_assert(is_stream_descriptor_v<Descriptor>, "Descriptor is not a valid stream descriptor");

        using Iterator = typename StreamType<Type>::Iterator;
        virtual auto get_all_entries_iterator() -> std::unique_ptr<Iterator> = 0;
    };

    template<typename Descriptor>
    using StreamBase = stream_generic_base<Descriptor, Stream>;
    template<typename Descriptor>
    using ProducerStreamBase = stream_generic_base<Descriptor, producer_stream>;

    template<typename, template<typename> typename>
    struct StreamDispatcherBase;

    /**
     * This class declares the general interface for an entity that provides a given
     * set of streams. It has the StreamBases as virtual base classes.
     * @tparam Streams
     * @tparam StreamType Either Stream<T> or ProducerStream<T>
     */
    template<typename... Streams, template<typename> typename StreamType>
    struct StreamDispatcherBase<stream_descriptor_set<Streams...>, StreamType>
        : virtual stream_generic_base<Streams, StreamType>... { };

}    // namespace nil::dbms::replication_sdk::streams
