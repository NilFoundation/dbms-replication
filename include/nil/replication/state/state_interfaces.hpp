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

#include <nil/dbms/replication/state/state_token.hpp>
#include <nil/dbms/replication/state/state_traits.hpp>
#include <nil/dbms/replication/state/state_status.hpp>
#include "nil/dbms/replication/streams/streams.hpp"

namespace nil::dbms::futures {
    struct Unit;
    template<typename T>
    class Future;
}    // namespace nil::dbms::futures
namespace nil::dbms {
    class Result;
}
namespace nil::dbms::replication {
    namespace log {
        struct ReplicatedLog;
        struct ILogFollower;
        struct Ilog_leader;
    }    // namespace log

    namespace state {

        template<typename S>
        struct FollowerStateManager;

        struct IReplicatedLeaderStateBase {
            virtual ~IReplicatedLeaderStateBase() = default;
        };

        struct IReplicatedFollowerStateBase {
            virtual ~IReplicatedFollowerStateBase() = default;
        };

        template<typename S>
        struct IReplicatedLeaderState : IReplicatedLeaderStateBase {
            using EntryType = typename ReplicatedStateTraits<S>::EntryType;
            using CoreType = typename ReplicatedStateTraits<S>::CoreType;
            using Stream = streams::ProducerStream<EntryType>;
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
            virtual auto recover_entries(std::unique_ptr<EntryIterator>) -> futures::Future<Result> = 0;

            auto getStream() const -> std::shared_ptr<Stream> const &;

            [[nodiscard]] virtual auto resign() &&noexcept -> std::unique_ptr<CoreType> = 0;

            /**
             * This hook is called after leader recovery is completed and the internal
             * state has been updated. The underlying stream is guaranteed to have been
             * initialized.
             */
            virtual void on_snapshot_completed() {};

            // TODO make private
            std::shared_ptr<Stream> _stream;
        };

        template<typename S>
        struct IReplicatedFollowerState : IReplicatedFollowerStateBase {
            using EntryType = typename ReplicatedStateTraits<S>::EntryType;
            using CoreType = typename ReplicatedStateTraits<S>::CoreType;
            using Stream = streams::Stream<EntryType>;
            using EntryIterator = typename Stream::Iterator;

            using wait_for_appliedFuture = futures::Future<futures::Unit>;
            [[nodiscard]] auto wait_for_applied(log_index index) -> wait_for_appliedFuture;

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
            friend struct FollowerStateManager<S>;

            void setStateManager(std::shared_ptr<FollowerStateManager<S>> manager) noexcept;

            std::weak_ptr<FollowerStateManager<S>> _manager;
            std::shared_ptr<Stream> _stream;
        };
    }    // namespace state
}    // namespace nil::dbms::replication
