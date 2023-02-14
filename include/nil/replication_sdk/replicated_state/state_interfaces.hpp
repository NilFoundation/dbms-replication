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

#include <nil/replication_sdk/replicated_state/replicated_state_token.hpp>
#include <nil/replication_sdk/replicated_state/replicated_state_traits.hpp>
#include <nil/replication_sdk/replicated_state/state_status.hpp>
#include "nil/replication_sdk/streams/streams.hpp"

namespace nil::dbms::futures {
    struct Unit;
    template<typename T>
    class Future;
}    // namespace nil::dbms::futures
namespace nil::dbms {
    class Result;
}
namespace nil::dbms::replication_sdk {
    namespace replicated_log {
        struct replicated_log_t;
        struct ilog_follower;
        struct ilog_leader;
    }    // namespace replicated_log

    namespace replicated_state {

        template<typename S>
        struct follower_state_manager;

        struct ireplicated_leader_state_base {
            virtual ~ireplicated_leader_state_base() = default;
        };

        struct ireplicated_follower_state_base {
            virtual ~ireplicated_follower_state_base() = default;
        };

        template<typename S>
        struct ireplicated_leader_state : ireplicated_leader_state_base {
            using EntryType = typename replicated_state_traits<S>::EntryType;
            using CoreType = typename replicated_state_traits<S>::CoreType;
            using Stream = streams::producer_stream<EntryType>;
            using EntryIterator = typename Stream::Iterator;

            // TODO make functions protected
            /**
             * This function is called once on a leader instance. The iterator contains
             * all log entries currently present in the replicated log. The state machine
             * manager awaits the return value. If the result is ok, the leader instance
             * is made available to the outside world.
             *
             * If the recovery fails, the server aborts.
             * @return Future to be fulfilled when recovery is done.
             */
            virtual auto recoverEntries(std::unique_ptr<EntryIterator>) -> futures::Future<Result> = 0;

            auto getStream() const -> std::shared_ptr<Stream> const &;

            [[nodiscard]] virtual auto resign() &&noexcept -> std::unique_ptr<CoreType> = 0;

            /**
             * This hook is called after leader recovery is completed and the internal
             * state has been updated. The underlying stream is guaranteed to have been
             * initialized.
             */
            virtual void onSnapshotCompleted() {};

            // TODO make private
            std::shared_ptr<Stream> _stream;
        };

        template<typename S>
        struct ireplicated_follower_state : ireplicated_follower_state_base {
            using EntryType = typename replicated_state_traits<S>::EntryType;
            using CoreType = typename replicated_state_traits<S>::CoreType;
            using Stream = streams::Stream<EntryType>;
            using EntryIterator = typename Stream::Iterator;

            using WaitForAppliedFuture = futures::Future<futures::Unit>;
            [[nodiscard]] auto waitForApplied(log_index index) -> WaitForAppliedFuture;

        protected:
            /**
             * Called by the state machine manager if new log entries have been committed
             * and are ready to be applied to the state machine. The implementation
             * ensures that this function is not called again until the future returned is
             * fulfilled.
             *
             * Entries are not released after they are consumed by this function. Its the
             * state machines implementations responsibility to call release on the
             * stream.
             *
             * @return Future with Result value. If the result contains an error, the
             *    operation is retried.
             */
            virtual auto apply_entries(std::unique_ptr<EntryIterator>) noexcept -> futures::Future<Result> = 0;

            /**
             * Called by the state machine manager if a follower is requested to pull
             * data from the leader in order to transfer the snapshot.
             * @param leader
             * @param localCommitIndex
             * @return Future with Result value. If the result contains an error,
             *    the operation is eventually retried.
             */
            virtual auto acquireSnapshot(ParticipantId const &leader, log_index localCommitIndex) noexcept
                -> futures::Future<Result> = 0;

            /**
             * TODO Comment missing
             * @return
             */
            [[nodiscard]] virtual auto resign() &&noexcept -> std::unique_ptr<CoreType> = 0;

        protected:
            [[nodiscard]] auto getStream() const -> std::shared_ptr<Stream> const &;

        private:
            friend struct follower_state_manager<S>;

            void setStateManager(std::shared_ptr<follower_state_manager<S>> manager) noexcept;

            std::weak_ptr<follower_state_manager<S>> _manager;
            std::shared_ptr<Stream> _stream;
        };
    }    // namespace replicated_state
}    // namespace nil::dbms::replication_sdk
