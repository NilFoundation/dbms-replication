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

#include <nil/dbms/replication/replicated_log/log_common.hpp>
#include <nil/dbms/replication/replicated_log/log_entries.hpp>
#include <nil/dbms/replication/replicated_log/log_status.hpp>
#include <nil/dbms/replication/replicated_log/agency_log_specification.hpp>
#include <nil/dbms/replication/replicated_state/agency_specification.hpp>

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

namespace nil::dbms::consensus {
    typedef uint64_t index_t;
}

namespace nil::dbms::replication {
    namespace agency {
        struct log_target;
    }

    namespace replicated_log {
        struct append_entries_request;
        struct append_entries_result;
        struct wait_for_result;
    }    // namespace replicated_log

    /**
     * This is a collection functions that is used by the RestHandler and the V8
     * interface. It covers two different implementations. One for dbservers that
     * actually execute the commands and one for coordinators that forward the
     * request to the leader.
     */
    struct replicated_log_methods {
        virtual ~replicated_log_methods() = default;

        static constexpr auto kDefaultLimit = std::size_t {10};

        using GenericLogStatus =
            std::variant<replication::replicated_log::log_status, replication::replicated_log::global_status>;
        using ParticipantsList = std::vector<std::string>;

        struct create_options {
            bool waitForReady {true};
            std::optional<LogId> id;
            std::optional<agency::log_target_config> config;
            std::optional<ParticipantId> leader;
            std::vector<ParticipantId> servers;
        };

        struct create_result {
            LogId id;
            std::vector<ParticipantId> servers;
        };

        virtual auto create_replicated_log(create_options spec) const -> futures::Future<ResultT<create_result>> = 0;

        virtual auto delete_replicated_log(LogId id) const -> futures::Future<Result> = 0;
        virtual auto get_replicated_logs() const
            -> futures::Future<std::unordered_map<nil::dbms::replication::LogId,
                                                  std::variant<replicated_log::log_status, ParticipantsList>>> = 0;
        virtual auto getLocalStatus(LogId) const -> futures::Future<replication::replicated_log::log_status> = 0;
        virtual auto getGlobalStatus(LogId, replicated_log::global_status::SpecificationSource) const
            -> futures::Future<replication::replicated_log::global_status> = 0;
        virtual auto getStatus(LogId) const -> futures::Future<GenericLogStatus> = 0;

        virtual auto getLogEntryByIndex(LogId, log_index) const
            -> futures::Future<std::optional<persisting_log_entry>> = 0;

        virtual auto slice(LogId, log_index start, log_index stop) const
            -> futures::Future<std::unique_ptr<persisted_log_iterator>> = 0;
        virtual auto poll(LogId, log_index, std::size_t limit) const
            -> futures::Future<std::unique_ptr<persisted_log_iterator>> = 0;
        virtual auto head(LogId, std::size_t limit) const
            -> futures::Future<std::unique_ptr<persisted_log_iterator>> = 0;
        virtual auto tail(LogId, std::size_t limit) const
            -> futures::Future<std::unique_ptr<persisted_log_iterator>> = 0;

        virtual auto insert(LogId, log_payload, bool waitForSync) const
            -> futures::Future<std::pair<log_index, replicated_log::wait_for_result>> = 0;
        virtual auto insert(LogId, typed_log_iterator<log_payload> &iter, bool waitForSync) const
            -> futures::Future<std::pair<std::vector<log_index>, replicated_log::wait_for_result>> = 0;

        // Insert an entry without waiting for the corresponding LogIndex to be
        // committed.
        // TODO This could be merged with `insert()` by using a common result type,
        //      Future<InsertResult> or so, which internally differentiates between
        //      the variants.
        //      See https://arangodb.atlassian.net/browse/CINFRA-278.
        // TODO Implement this for a list of payloads as well, as insert() does.
        //      See https://arangodb.atlassian.net/browse/CINFRA-278.
        virtual auto insertWithoutCommit(LogId, log_payload, bool waitForSync) const -> futures::Future<log_index> = 0;

        virtual auto release(LogId, log_index) const -> futures::Future<Result> = 0;

        /*
         * Wait until the supervision reports that the replicated log has converged
         * to the given version.
         */
        [[nodiscard]] virtual auto waitForLogReady(LogId, std::uint64_t version) const
            -> futures::Future<ResultT<consensus::index_t>> = 0;

        static auto createInstance(TRI_vocbase_t &vocbase) -> std::shared_ptr<replicated_log_methods>;

    private:
        virtual auto createReplicatedLog(agency::log_target spec) const -> futures::Future<Result> = 0;
    };

    template<class Inspector>
    auto inspect(Inspector &f, replicated_log_methods::create_options &x) {
        return f.object(x).fields(f.field("waitForReady", x.waitForReady).fallback(true), f.field("id", x.id),
                                  f.field("config", x.config), f.field("leader", x.leader),
                                  f.field("servers", x.servers).fallback(std::vector<ParticipantId> {}));
    }

    template<class Inspector>
    auto inspect(Inspector &f, replicated_log_methods::create_result &x) {
        return f.object(x).fields(f.field("id", x.id), f.field("servers", x.servers));
    }

    struct replicated_state_methods {
        virtual ~replicated_state_methods() = default;

        [[nodiscard]] virtual auto waitForStateReady(LogId, std::uint64_t version)
            -> futures::Future<ResultT<consensus::index_t>> = 0;

        virtual auto create_replicated_state(replicated_state::agency::Target spec) const -> futures::Future<Result> = 0;
        virtual auto deleteReplicatedState(LogId id) const -> futures::Future<Result> = 0;

        virtual auto getLocalStatus(LogId) const -> futures::Future<replicated_state::state_status> = 0;

        struct ParticipantSnapshotStatus {
            replicated_state::snapshot_info status;
            replicated_state::state_generation generation;
        };

        using GlobalSnapshotStatus = std::unordered_map<ParticipantId, ParticipantSnapshotStatus>;

        virtual auto getGlobalSnapshotStatus(LogId) const -> futures::Future<ResultT<GlobalSnapshotStatus>> = 0;

        static auto createInstance(TRI_vocbase_t &vocbase) -> std::shared_ptr<replicated_state_methods>;

        static auto createInstanceDBServer(TRI_vocbase_t &vocbase) -> std::shared_ptr<replicated_state_methods>;

        static auto createInstanceCoordinator(DbmsdServer &server, std::string databaseName)
            -> std::shared_ptr<replicated_state_methods>;

        [[nodiscard]] virtual auto replaceParticipant(LogId, ParticipantId const &participantToRemove,
                                                      ParticipantId const &participantToAdd,
                                                      std::optional<ParticipantId> const &currentLeader) const
            -> futures::Future<Result> = 0;

        [[nodiscard]] virtual auto setLeader(LogId id, std::optional<ParticipantId> const &leaderId) const
            -> futures::Future<Result> = 0;
    };

    template<class Inspector>
    auto inspect(Inspector &f, replicated_state_methods::ParticipantSnapshotStatus &x) {
        return f.object(x).fields(f.field("status", x.status), f.field("generation", x.generation));
    }

}    // namespace nil::dbms::replication
