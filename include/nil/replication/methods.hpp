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

#include <nil/dbms/agency/agency_common.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/log_entries.hpp>
#include <nil/dbms/replication/log/log_status.hpp>
#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/state/agency_specification.hpp>

#include <string>
#include <variant>
#include <vector>

namespace nil::dbms {
    class Result;
    namespace futures {
        template<typename T>
        class Future;
    }
}    // namespace nil::dbms

namespace nil::dbms::replication {

    namespace agency {
        struct LogTarget;
    }

    namespace log {
        struct AppendEntriesRequest;
        struct AppendEntriesResult;
        struct wait_for_result;
    }    // namespace log

    /**
     * This is a collection functions that is used by the RestHandler and the V8
     * interface. It covers two different implementations. One for dbservers that
     * actually execute the commands and one for coordinators that forward the
     * request to the leader.
     */
    struct replicated_log_methods {
        virtual ~replicated_log_methods() = default;

        static constexpr auto kDefaultLimit = std::size_t {10};

        using Genericlog_status =
            std::variant<replication::log::log_status, replication::log::global_status>;
        using ParticipantsList = std::vector<std::string>;

        struct CreateOptions {
            bool waitForReady {true};
            std::optional<log_id> id;
            std::optional<agency::LogTargetConfig> config;
            std::optional<ParticipantId> leader;
            std::vector<ParticipantId> servers;
        };

        struct CreateResult {
            log_id id;
            std::vector<ParticipantId> servers;
        };

        virtual auto create_replicated_log(CreateOptions spec) const -> futures::Future<ResultT<CreateResult>> = 0;

        virtual auto delete_replicated_log(log_id id) const -> futures::Future<Result> = 0;
        virtual auto get_replicated_logs() const
            -> futures::Future<std::unordered_map<nil::dbms::replication::log_id,
                                                  std::variant<log::log_status, ParticipantsList>>> = 0;
        virtual auto get_local_status(log_id) const -> futures::Future<replication::log::log_status> = 0;
        virtual auto get_global_status(log_id, log::global_status::SpecificationSource) const
            -> futures::Future<replication::log::global_status> = 0;
        virtual auto get_status(log_id) const -> futures::Future<Genericlog_status> = 0;

        virtual auto get_log_entry_by_index(log_id, log_index) const
            -> futures::Future<std::optional<persisting_log_entry>> = 0;

        virtual auto slice(log_id, log_index start, log_index stop) const
            -> futures::Future<std::unique_ptr<persisted_logIterator>> = 0;
        virtual auto poll(log_id, log_index, std::size_t limit) const
            -> futures::Future<std::unique_ptr<persisted_logIterator>> = 0;
        virtual auto head(log_id, std::size_t limit) const -> futures::Future<std::unique_ptr<persisted_logIterator>> = 0;
        virtual auto tail(log_id, std::size_t limit) const -> futures::Future<std::unique_ptr<persisted_logIterator>> = 0;

        virtual auto insert(log_id, log_payload, bool waitForSync) const
            -> futures::Future<std::pair<log_index, log::wait_for_result>> = 0;
        virtual auto insert(log_id, TypedLogIterator<log_payload> &iter, bool waitForSync) const
            -> futures::Future<std::pair<std::vector<log_index>, log::wait_for_result>> = 0;

        // Insert an entry without waiting for the corresponding log_index to be
        // committed.
        virtual auto insert_without_commit(log_id, log_payload, bool waitForSync) const -> futures::Future<log_index> = 0;

        virtual auto release(log_id, log_index) const -> futures::Future<Result> = 0;

        /*
         * Wait until the supervision reports that the replicated log has converged
         * to the given version.
         */
        [[nodiscard]] virtual auto wait_for_log_ready(log_id, std::uint64_t version) const
            -> futures::Future<ResultT<consensus::index_t>> = 0;

        static auto create_instance(TRI_vocbase_t &vocbase) -> std::shared_ptr<replicated_log_methods>;

    private:
        virtual auto create_replicated_log(agency::LogTarget spec) const -> futures::Future<Result> = 0;
    };

    template<class Inspector>
    auto inspect(Inspector &f, replicated_log_methods::CreateOptions &x) {
        return f.object(x).fields(f.field("waitForReady", x.waitForReady).fallback(true), f.field("id", x.id),
                                  f.field("config", x.config), f.field("leader", x.leader),
                                  f.field("servers", x.servers).fallback(std::vector<ParticipantId> {}));
    }

    template<class Inspector>
    auto inspect(Inspector &f, replicated_log_methods::CreateResult &x) {
        return f.object(x).fields(f.field("id", x.id), f.field("servers", x.servers));
    }

    struct replicated_state_methods {
        virtual ~replicated_state_methods() = default;

        [[nodiscard]] virtual auto wait_for_state_ready(log_id, std::uint64_t version)
            -> futures::Future<ResultT<consensus::index_t>> = 0;

        virtual auto create_replicated_state(state::agency::Target spec) const -> futures::Future<Result> = 0;
        virtual auto delete_replicated_state(log_id id) const -> futures::Future<Result> = 0;

        virtual auto get_local_status(log_id) const -> futures::Future<state::StateStatus> = 0;

        struct ParticipantSnapshotStatus {
            state::SnapshotInfo status;
            state::StateGeneration generation;
        };

        using GlobalSnapshotStatus = std::unordered_map<ParticipantId, ParticipantSnapshotStatus>;

        virtual auto get_global_snapshot_status(log_id) const -> futures::Future<ResultT<GlobalSnapshotStatus>> = 0;

        static auto create_instance(TRI_vocbase_t &vocbase) -> std::shared_ptr<replicated_state_methods>;

        static auto create_instanceDBServer(TRI_vocbase_t &vocbase) -> std::shared_ptr<replicated_state_methods>;

        static auto create_instanceCoordinator(DbmsdServer &server, std::string databaseName)
            -> std::shared_ptr<replicated_state_methods>;

        [[nodiscard]] virtual auto replace_participant(log_id, ParticipantId const &participantToRemove,
                                                      ParticipantId const &participantToAdd,
                                                      std::optional<ParticipantId> const &currentLeader) const
            -> futures::Future<Result> = 0;

        [[nodiscard]] virtual auto set_leader(log_id id, std::optional<ParticipantId> const &leaderId) const
            -> futures::Future<Result> = 0;
    };

    template<class Inspector>
    auto inspect(Inspector &f, replicated_state_methods::ParticipantSnapshotStatus &x) {
        return f.object(x).fields(f.field("status", x.status), f.field("generation", x.generation));
    }

}    // namespace nil::dbms::replication
