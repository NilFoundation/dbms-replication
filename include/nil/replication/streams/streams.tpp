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
    struct StreamGenericImplementationBase : virtual StreamGenericBase<Descriptor, StreamInterface> {
        static_assert(is_stream_descriptor_v<Descriptor>);

        using ValueType = stream_descriptor_type_t<Descriptor>;
        using Iterator = Typedlog_rangeIterator<StreamEntryView<ValueType>>;
        using wait_for_result = typename StreamInterface<ValueType>::wait_for_result;

        auto waitForIterator(log_index index) -> futures::Future<std::unique_ptr<Iterator>> override final {
            return implementation().template waitForIteratorInternal<Descriptor>(index);
        }
        auto waitFor(log_index index) -> futures::Future<wait_for_result> override final {
            return implementation().template waitForInternal<Descriptor>(index);
        }
        auto release(log_index index) -> void override final {
            return implementation().template releaseInternal<Descriptor>(index);
        }
        auto getAllEntriesIterator() -> std::unique_ptr<Iterator> override final {
            return implementation().template getIteratorInternal<Descriptor>();
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
    struct StreamGenericImplementation : StreamGenericImplementationBase<Implementation, Descriptor, StreamInterface> {
    };
    template<typename Implementation, typename Descriptor>
    struct StreamGenericImplementation<Implementation, Descriptor, ProducerStream>
        : StreamGenericImplementationBase<Implementation, Descriptor, ProducerStream> {
        using ValueType = stream_descriptor_type_t<Descriptor>;

        auto insert(ValueType const &t) -> log_index override {
            return static_cast<Implementation *>(this)->template insertInternal<Descriptor>(t);
        }

        auto insertDeferred(ValueType const &t) -> std::pair<log_index, deferred_action> override {
            return static_cast<Implementation *>(this)->template insertInternalDeferred<Descriptor>(t);
        }
    };

    template<typename Implementation, typename Descriptor>
    using StreamImplementation = StreamGenericImplementation<Implementation, Descriptor, Stream>;
    template<typename Implementation, typename Descriptor>
    using ProducerStreamImplementation = StreamGenericImplementation<Implementation, Descriptor, ProducerStream>;

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
        : StreamGenericImplementation<Implementation, Streams, StreamInterface>... { };

}    // namespace nil::dbms::replication::streams
