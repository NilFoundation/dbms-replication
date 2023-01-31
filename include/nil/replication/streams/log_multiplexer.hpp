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
#include <utility>

#include <futures/Future.h>

#include <nil/dbms/replication/log/ilog_interfaces.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/types.hpp>

#include <nil/dbms/replication/streams/stream_specification.hpp>
#include "streams.hpp"

namespace nil::dbms::replication::log {
    struct ILogFollower;
    struct Ilog_leader;
}    // namespace nil::dbms::replication::log

namespace nil::dbms::replication::streams {

    /**
     * Common stream dispatcher class for Multiplexer and Demultiplexer. You can
     * obtain a stream given its id using getStreamById. Alternatively, you can
     * static_cast the a pointer to StreamBase<Descriptor> for the given stream.
     * @tparam Self
     * @tparam Spec
     * @tparam StreamType
     */
    template<typename Self, typename Spec, template<typename> typename StreamType>
    struct LogMultiplexerStreamDispatcher : std::enable_shared_from_this<Self>, StreamDispatcherBase<Spec, StreamType> {
        template<StreamId Id, typename Descriptor = stream_descriptor_by_id_t<Id, Spec>>
        auto getStreamBaseById() -> std::shared_ptr<StreamGenericBase<Descriptor, StreamType>> {
            return getStreamByDescriptor<Descriptor>();
        }

        template<StreamId Id>
        auto getStreamById() -> std::shared_ptr<StreamType<stream_type_by_id_t<Id, Spec>>> {
            return getStreamByDescriptor<stream_descriptor_by_id_t<Id, Spec>>();
        }

        template<typename Descriptor>
        auto getStreamByDescriptor() -> std::shared_ptr<StreamGenericBase<Descriptor, StreamType>> {
            return std::static_pointer_cast<StreamGenericBase<Descriptor, StreamType>>(this->shared_from_this());
        }
    };

    /**
     * Demultiplexer class. Use ::construct to create an instance.
     * @tparam Spec Log specification
     */
    template<typename Spec>
    struct LogDemultiplexer : LogMultiplexerStreamDispatcher<LogDemultiplexer<Spec>, Spec, Stream> {
        virtual auto digestIterator(LogIterator &iter) -> void = 0;

        /*
         * After construction the demultiplexer is not yet in a listen state. You have
         * to call `listen` once.
         */
        virtual auto listen() -> void = 0;

        static std::shared_ptr<LogDemultiplexer>
            construct(std::shared_ptr<nil::dbms::replication::log::ILogParticipant>);

    protected:
        LogDemultiplexer() = default;
    };

    /**
     * Multiplexer class. Use ::construct to create an instance.
     * @tparam Spec Log specification
     */
    template<typename Spec>
    struct LogMultiplexer : LogMultiplexerStreamDispatcher<LogMultiplexer<Spec>, Spec, ProducerStream> {
        static std::shared_ptr<LogMultiplexer>
            construct(std::shared_ptr<nil::dbms::replication::log::Ilog_leader> leader);

        /*
         * After construction the multiplexer has an empty internal state. To populate
         * it with the existing state in the replicated log, call
         * `digestAvailableEntries`.
         */
        virtual void digestAvailableEntries() = 0;

    protected:
        LogMultiplexer() = default;
    };

}    // namespace nil::dbms::replication::streams
