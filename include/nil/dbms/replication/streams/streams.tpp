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
#include "streams.hpp"

namespace nil::dbms::replication::streams {

    /**
     * This is the implementation of the stream interfaces. They are just proxy
     * objects that static_cast the this pointer to the respective implementor and
     * forward the call, annotated with the stream descriptor.
     * @tparam Implementation Implementor Top Class
     * @tparam Descriptor Stream Descriptor
     * @tparam StreamInterface Stream<T> or ProducerStream<T>
     */
    template<typename Implementation, typename Descriptor, template<typename> typename StreamInterface>
    struct stream_generic_implementation_base : virtual stream_generic_base<Descriptor, StreamInterface> {
        static_assert(is_stream_descriptor_v<Descriptor>);

        using ValueType = stream_descriptor_type_t<Descriptor>;
        using Iterator = typed_log_range_iterator<StreamEntryView<ValueType>>;
        using WaitForResult = typename StreamInterface<ValueType>::wait_for_result;

        auto wait_for_iterator(log_index index) -> futures::Future<std::unique_ptr<Iterator>> override final {
            return implementation().template wait_for_iterator_internal<Descriptor>(index);
        }
        auto wait_for(log_index index) -> futures::Future<WaitForResult> override final {
            return implementation().template wait_for_internal<Descriptor>(index);
        }
        auto release(log_index index) -> void override final {
            return implementation().template release_internal<Descriptor>(index);
        }
        auto get_all_entries_iterator() -> std::unique_ptr<Iterator> override final {
            return implementation().template get_iterator_internal<Descriptor>();
        }

    private:
        auto implementation() -> Implementation & {
            return static_cast<Implementation &>(*this);
        }
    };

    /**
     * Wrapper about StreamGenericImplementationBase, that adds depending on the
     * StreamInterface more methods. Is specialized for ProducerStream<T>.
     * @tparam Implementation Implementor Top Class
     * @tparam Descriptor Stream Descriptor
     * @tparam StreamInterface Stream<T> or ProducerStream<T>
     */
    template<typename Implementation, typename Descriptor, template<typename> typename StreamInterface>
    struct stream_generic_implementation
        : stream_generic_implementation_base<Implementation, Descriptor, StreamInterface> { };
    template<typename Implementation, typename Descriptor>
    struct stream_generic_implementation<Implementation, Descriptor, producer_stream>
        : stream_generic_implementation_base<Implementation, Descriptor, producer_stream> {
        using ValueType = stream_descriptor_type_t<Descriptor>;

        auto insert(ValueType const &t) -> log_index override {
            return static_cast<Implementation *>(this)->template insertInternal<Descriptor>(t);
        }

        auto insert_deferred(ValueType const &t) -> std::pair<log_index, deferred_action> override {
            return static_cast<Implementation *>(this)->template insertInternalDeferred<Descriptor>(t);
        }
    };

    template<typename Implementation, typename Descriptor>
    using StreamImplementation = stream_generic_implementation<Implementation, Descriptor, Stream>;
    template<typename Implementation, typename Descriptor>
    using ProducerStreamImplementation = stream_generic_implementation<Implementation, Descriptor, producer_stream>;

    template<typename, typename, template<typename> typename>
    struct ProxyStreamDispatcher;

    /**
     * Class that implements all streams as virtual base classes.
     * @tparam Implementation
     * @tparam Streams
     * @tparam StreamInterface
     */
    template<typename Implementation, typename... Streams, template<typename> typename StreamInterface>
    struct ProxyStreamDispatcher<Implementation, stream_descriptor_set<Streams...>, StreamInterface>
        : stream_generic_implementation<Implementation, Streams, StreamInterface>... { };

}    // namespace nil::dbms::replication::streams
