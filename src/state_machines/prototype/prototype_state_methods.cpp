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

#include <random>

#include <basics/result_t.h>
#include <basics/exceptions.h>
#include <basics/exceptions.tpp>
#include <basics/voc_errors.h>
#include <futures/Future.h>

#include <features/ApplicationServer.h>

#include <nil/dbms/cluster/cluster_feature.hpp>
#include <nil/dbms/cluster/cluster_info.hpp>
#include <nil/dbms/cluster/server_state.hpp>

#include <nil/dbms/replication/exceptions/participant_resigned_exception.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include "nil/dbms/replication/state/state.hpp"
#include <nil/dbms/replication/methods.hpp>

#include <nil/dbms/network/methods.hpp>
#include "nil/dbms/vocbase/vocbase.hpp"
#include "random/RandomGenerator.h"

#include <nil/dbms/replication/state_machines/prototype/prototype_leader_state.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_state_machine.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_state_methods.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_follower_state.hpp>
#include <nil/dbms/replication/state/agency_specification.hpp>
#include "inspection/vpack.h"

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::log;
using namespace nil::dbms::replication::state;
using namespace nil::dbms::replication::state::prototype;

struct prototype_state_methodsDBServer final : prototype_state_methods {
    explicit prototype_state_methodsDBServer(TRI_vocbase_t &vocbase) : _vocbase(vocbase) {
    }

    [[nodiscard]] auto compare_exchange(log_id id, std::string key, std::string oldValue, std::string newValue,
                                       PrototypeWriteOptions options) const
        -> futures::Future<ResultT<log_index>> override {
        auto leader = getprototype_stateLeaderById(id);
        return leader->compare_exchange(std::move(key), std::move(oldValue), std::move(newValue), options);
    }

    [[nodiscard]] auto insert(log_id id, std::unordered_map<std::string, std::string> const &entries,
                              PrototypeWriteOptions options) const -> futures::Future<log_index> override {
        auto leader = getprototype_stateLeaderById(id);
        return leader->set(entries, options);
    };

    auto get(log_id id, std::vector<std::string> keys, ReadOptions const &readOptions) const
        -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> override {
        auto stateMachine =
            std::dynamic_pointer_cast<ReplicatedState<prototype_state>>(_vocbase.getReplicatedStateById(id));
        if (stateMachine == nullptr) {
            THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL,
                                         basics::StringUtils::concatT("Failed to get ProtoypeState with id ", id));
        }

        if (readOptions.readFrom.has_value()) {
            THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_NOT_IMPLEMENTED,
                                         "reading on followers is not implemented on dbservers");
        }

        auto leader = stateMachine->getLeader();
        if (leader != nullptr) {
            return leader->get(std::move(keys), readOptions.wait_for_applied);
        }
        auto follower = stateMachine->getFollower();
        if (follower != nullptr) {
            return follower->get(std::move(keys), readOptions.wait_for_applied);
        }

        THROW_DBMS_EXCEPTION(TRI_ERROR_REPLICATION_REPLICATED_LOG_PARTICIPANT_GONE);
    }

    virtual auto wait_for_applied(log_id id, log_index waitForIndex) const -> futures::Future<Result> override {
        auto stateMachine =
            std::dynamic_pointer_cast<ReplicatedState<prototype_state>>(_vocbase.getReplicatedStateById(id));
        if (stateMachine == nullptr) {
            THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL,
                                         basics::StringUtils::concatT("Failed to get ProtoypeState with id ", id));
        }
        auto leader = stateMachine->getLeader();
        if (leader != nullptr) {
            return leader->wait_for_applied(waitForIndex).thenValue([](auto &&) { return Result {}; });
        }
        auto follower = stateMachine->getFollower();
        if (follower != nullptr) {
            return follower->wait_for_applied(waitForIndex).thenValue([](auto &&) { return Result {}; });
        }
        THROW_DBMS_EXCEPTION(TRI_ERROR_REPLICATION_REPLICATED_LOG_PARTICIPANT_GONE);
    }

    [[nodiscard]] auto get_snapshot(log_id id, log_index waitForIndex) const
        -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> override {
        auto leader = getprototype_stateLeaderById(id);
        return leader->get_snapshot(waitForIndex);
    }

    [[nodiscard]] auto remove(log_id id, std::string key, PrototypeWriteOptions options) const
        -> futures::Future<log_index> override {
        auto leader = getprototype_stateLeaderById(id);
        return leader->remove(std::move(key), options);
    }

    [[nodiscard]] auto remove(log_id id, std::vector<std::string> keys, PrototypeWriteOptions options) const
        -> futures::Future<log_index> override {
        auto leader = getprototype_stateLeaderById(id);
        return leader->remove(std::move(keys), options);
    }

    auto status(log_id id) const -> futures::Future<ResultT<PrototypeStatus>> override {
        std::ignore = getprototype_stateLeaderById(id);
        return PrototypeStatus {id};    // TODO
    }

    auto createState(CreateOptions options) const -> futures::Future<ResultT<CreateResult>> override {
        THROW_DBMS_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
    }

    auto drop(log_id id) const -> futures::Future<Result> override {
        THROW_DBMS_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
    }

private:
    [[nodiscard]] auto getprototype_stateLeaderById(log_id id) const -> std::shared_ptr<prototype_leader_state> {
        auto stateMachine =
            std::dynamic_pointer_cast<ReplicatedState<prototype_state>>(_vocbase.getReplicatedStateById(id));
        if (stateMachine == nullptr) {
            using namespace fmt::literals;
            throw basics::Exception::fmt(ADB_HERE, TRI_ERROR_REPLICATION_REPLICATED_STATE_NOT_FOUND, "id"_a = id,
                                         "type"_a = "prototype_state");
        }
        auto leader = stateMachine->getLeader();
        if (leader == nullptr) {
            THROW_DBMS_EXCEPTION_MESSAGE(
                TRI_ERROR_CLUSTER_NOT_LEADER,
                basics::StringUtils::concatT("Failed to get leader of ProtoypeState with id ", id));
        }
        return leader;
    }

private:
    TRI_vocbase_t &_vocbase;
};

struct prototype_state_methodsCoordinator final : prototype_state_methods,
                                                std::enable_shared_from_this<prototype_state_methodsCoordinator> {
    [[nodiscard]] auto compare_exchange(log_id id, std::string key, std::string oldValue, std::string newValue,
                                       PrototypeWriteOptions options) const
        -> futures::Future<ResultT<log_index>> override {
        auto path = basics::StringUtils::joinT("/", "_api/prototype-state", id, "cmp-ex");
        network::RequestOptions opts;
        opts.database = _vocbase.name();
        opts.param("wait_for_applied", std::to_string(options.wait_for_applied));
        opts.param("waitForSync", std::to_string(options.waitForSync));
        opts.param("waitForCommit", std::to_string(options.waitForCommit));

        VPackBuilder builder {};
        {
            VPackObjectBuilder ob {&builder};
            builder.add(VPackValue(key));
            {
                VPackObjectBuilder ob2 {&builder};
                builder.add("oldValue", VPackValue(oldValue));
                builder.add("newValue", VPackValue(newValue));
            }
        }

        return network::sendRequest(_pool, "server:" + getlog_leader(id), fuerte::RestVerb::Put, path,
                                    builder.bufferRef(), opts)
            .thenValue([](network::Response &&resp) -> ResultT<log_index> {
                if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                    auto r = resp.combinedResult();
                    r = r.mapError([&](result::Error error) {
                        error.appendErrorMessage(" while contacting server " + resp.serverId());
                        return error;
                    });
                    return r;
                } else {
                    auto slice = resp.slice();
                    if (auto result = slice.get("result"); result.isObject() && result.length() == 1) {
                        return result.get("index").extract<log_index>();
                    }
                    THROW_DBMS_EXCEPTION_MESSAGE(
                        TRI_ERROR_INTERNAL,
                        basics::StringUtils::concatT("expected result containing index in leader response: ",
                                                     slice.toJson()));
                }
            });
    }

    [[nodiscard]] auto insert(log_id id, std::unordered_map<std::string, std::string> const &entries,
                              PrototypeWriteOptions options) const -> futures::Future<log_index> override {
        auto path = basics::StringUtils::joinT("/", "_api/prototype-state", id, "insert");
        network::RequestOptions opts;
        opts.database = _vocbase.name();
        opts.param("wait_for_applied", std::to_string(options.wait_for_applied));
        opts.param("waitForSync", std::to_string(options.waitForSync));
        opts.param("waitForCommit", std::to_string(options.waitForCommit));

        VPackBuilder builder {};
        {
            VPackObjectBuilder ob {&builder};
            for (auto const &[key, value] : entries) {
                builder.add(key, VPackValue(value));
            }
        }
        return network::sendRequest(_pool, "server:" + getlog_leader(id), fuerte::RestVerb::Post, path,
                                    builder.bufferRef(), opts)
            .thenValue([](network::Response &&resp) -> log_index { return processlog_indexResponse(std::move(resp)); });
    }

    auto get(log_id id, std::vector<std::string> keys, ReadOptions const &readOptions) const
        -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> override {
        auto path = basics::StringUtils::joinT("/", "_api/prototype-state", id, "multi-get");
        network::RequestOptions opts;
        opts.database = _vocbase.name();
        opts.param("wait_for_applied", std::to_string(readOptions.wait_for_applied.value));

        VPackBuilder builder {};
        {
            VPackArrayBuilder ab {&builder};
            for (auto const &key : keys) {
                builder.add(VPackValue(key));
            }
        }

        auto server = std::invoke([&]() -> ParticipantId {
            if (readOptions.readFrom) {
                return *readOptions.readFrom;
            }
            return getlog_leader(id);
        });

        return network::sendRequest(_pool, "server:" + server, fuerte::RestVerb::Post, path, builder.bufferRef(), opts)
            .thenValue([](network::Response &&resp) -> ResultT<std::unordered_map<std::string, std::string>> {
                if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                    auto r = resp.combinedResult();
                    r = r.mapError([&](result::Error error) {
                        error.appendErrorMessage(" while contacting server " + resp.serverId());
                        return error;
                    });
                    return r;
                } else {
                    auto slice = resp.slice();
                    if (auto result = slice.get("result"); result.isObject()) {
                        std::unordered_map<std::string, std::string> map;
                        for (auto it : VPackObjectIterator {result}) {
                            map.emplace(it.key.copyString(), it.value.copyString());
                        }
                        return {map};
                    }
                    THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL,
                                                 basics::StringUtils::concatT("expected result containing map "
                                                                              "in leader response: ",
                                                                              slice.toJson()));
                }
            });
    }

    [[nodiscard]] auto get_snapshot(log_id id, log_index waitForIndex) const
        -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> override {
        auto path = basics::StringUtils::joinT("/", "_api/prototype-state", id, "snapshot");
        network::RequestOptions opts;
        opts.database = _vocbase.name();
        opts.param("waitForIndex", std::to_string(waitForIndex.value));

        return network::sendRequest(_pool, "server:" + getlog_leader(id), fuerte::RestVerb::Get, path, {}, opts)
            .thenValue([](network::Response &&resp) -> ResultT<std::unordered_map<std::string, std::string>> {
                if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                    THROW_DBMS_EXCEPTION(resp.combinedResult());
                } else {
                    auto slice = resp.slice();
                    if (auto result = slice.get("result"); result.isObject()) {
                        std::unordered_map<std::string, std::string> map;
                        for (auto it : VPackObjectIterator {result}) {
                            map.emplace(it.key.copyString(), it.value.copyString());
                        }
                        return map;
                    }
                    THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL,
                                                 basics::StringUtils::concatT("expected result containing map "
                                                                              "in leader response: ",
                                                                              slice.toJson()));
                }
            });
    }

    [[nodiscard]] auto remove(log_id id, std::string key, PrototypeWriteOptions options) const
        -> futures::Future<log_index> override {
        auto path = basics::StringUtils::joinT("/", "_api/prototype-state", id, "entry", key);
        network::RequestOptions opts;
        opts.database = _vocbase.name();
        opts.param("wait_for_applied", std::to_string(options.wait_for_applied));
        opts.param("waitForSync", std::to_string(options.waitForSync));
        opts.param("waitForCommit", std::to_string(options.waitForCommit));

        return network::sendRequest(_pool, "server:" + getlog_leader(id), fuerte::RestVerb::Delete, path, {}, opts)
            .thenValue([](network::Response &&resp) -> log_index { return processlog_indexResponse(std::move(resp)); });
    }

    [[nodiscard]] auto remove(log_id id, std::vector<std::string> keys, PrototypeWriteOptions options) const
        -> futures::Future<log_index> override {
        auto path = basics::StringUtils::joinT("/", "_api/prototype-state", id, "multi-remove");
        network::RequestOptions opts;
        opts.database = _vocbase.name();
        opts.param("wait_for_applied", std::to_string(options.wait_for_applied));
        opts.param("waitForSync", std::to_string(options.waitForSync));
        opts.param("waitForCommit", std::to_string(options.waitForCommit));

        VPackBuilder builder {};
        {
            VPackArrayBuilder ab {&builder};
            for (auto const &key : keys) {
                builder.add(VPackValue(key));
            }
        }
        return network::sendRequest(_pool, "server:" + getlog_leader(id), fuerte::RestVerb::Delete, path,
                                    builder.bufferRef(), opts)
            .thenValue([](network::Response &&resp) -> log_index { return processlog_indexResponse(std::move(resp)); });
    }

    auto drop(log_id id) const -> futures::Future<Result> override {
        auto methods = replication::replicated_state_methods::create_instance(_vocbase);
        return methods->delete_replicated_state(id);
    }

    auto wait_for_applied(log_id id, log_index waitForIndex) const -> futures::Future<Result> override {
        auto path = basics::StringUtils::joinT("/", "_api/prototype-state", id, "wait-for-applied", waitForIndex);
        network::RequestOptions opts;
        opts.database = _vocbase.name();

        return network::sendRequest(_pool, "server:" + getlog_leader(id), fuerte::RestVerb::Get, path, {}, opts)
            .thenValue([](network::Response &&resp) -> Result { return resp.combinedResult(); });
    }

    void fillCreateOptions(CreateOptions &options) const {
        if (!options.id.has_value()) {
            options.id = log_id {_clusterInfo.uniqid()};
        }

        auto dbservers = _clusterInfo.getCurrentDBServers();
        std::size_t expectedNumberOfServers = std::min(dbservers.size(), std::size_t {3});
        if (!options.servers.empty()) {
            expectedNumberOfServers = options.servers.size();
        }

        if (!options.config.has_value()) {
            options.config = nil::dbms::replication::agency::LogTargetConfig {2, expectedNumberOfServers, false};
        }

        if (expectedNumberOfServers > dbservers.size()) {
            THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_INSUFFICIENT_DBSERVERS);
        }

        if (options.servers.size() < expectedNumberOfServers) {
            auto newEnd = dbservers.end();
            if (!options.servers.empty()) {
                newEnd = std::remove_if(dbservers.begin(), dbservers.end(), [&](ParticipantId const &server) {
                    return std::find(options.servers.begin(), options.servers.end(), server) != options.servers.end();
                });
            }

            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(dbservers.begin(), newEnd, g);
            std::copy_n(dbservers.begin(),
                        expectedNumberOfServers - options.servers.size(),
                        std::back_inserter(options.servers));
        }
    }

    static auto stateTargetFromCreateOptions(CreateOptions const &opts)
        -> replication::state::agency::Target {
        auto target = state::agency::Target {};
        target.id = opts.id.value();
        target.properties.implementation.type = "prototype";
        target.config = opts.config.value();
        target.version = 1;
        for (auto const &server : opts.servers) {
            target.participants[server];
        }
        return target;
    }

    auto status(log_id id) const -> futures::Future<ResultT<PrototypeStatus>> override {
        auto path = basics::StringUtils::joinT("/", "_api/prototype-state", id);
        network::RequestOptions opts;
        opts.database = _vocbase.name();

        return network::sendRequest(_pool, "server:" + getlog_leader(id), fuerte::RestVerb::Get, path, {}, opts)
            .thenValue([](network::Response &&resp) -> ResultT<PrototypeStatus> {
                if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                    THROW_DBMS_EXCEPTION(resp.combinedResult());
                } else {
                    return velocypack::deserialize<PrototypeStatus>(resp.slice().get("result"));
                }
            });
    }

    auto createState(CreateOptions options) const -> futures::Future<ResultT<CreateResult>> override {
        fillCreateOptions(options);
        TRI_ASSERT(options.id.has_value());
        auto target = stateTargetFromCreateOptions(options);
        auto methods = replication::replicated_state_methods::create_instance(_vocbase);

        return methods->create_replicated_state(std::move(target))
            .thenValue([options = std::move(options), methods,
                        self = shared_from_this()](auto &&result) mutable -> futures::Future<ResultT<CreateResult>> {
                auto response = CreateResult {*options.id, std::move(options.servers)};
                if (!result.ok()) {
                    return {result};
                }

                if (options.waitForReady) {
                    // wait for the state to be ready
                    return methods->waitForStateReady(*options.id, 1)
                        .thenValue([self, resp = std::move(response)](
                                       auto &&result) mutable -> futures::Future<ResultT<CreateResult>> {
                            if (result.fail()) {
                                return {result.result()};
                            }
                            return self->_clusterInfo.waitForPlan(result.get())
                                .thenValue([resp = std::move(resp)](auto &&result) mutable -> ResultT<CreateResult> {
                                    if (result.fail()) {
                                        return {result};
                                    }
                                    return std::move(resp);
                                });
                        });
                }
                return response;
            });
    }

    explicit prototype_state_methodsCoordinator(TRI_vocbase_t &vocbase) :
        _vocbase(vocbase), _clusterInfo(vocbase.server().getFeature<ClusterFeature>().clusterInfo()),
        _pool(vocbase.server().getFeature<NetworkFeature>().pool()) {
    }

private:
    [[nodiscard]] auto getlog_leader(log_id id) const -> ServerID {
        auto leader = _clusterInfo.getReplicatedlog_leader(_vocbase.name(), id);
        if (leader.fail()) {
            if (leader.is(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED)) {
                throw participant_resigned_exception(leader.result(), ADB_HERE);
            } else {
                THROW_DBMS_EXCEPTION(leader.result());
            }
        }
        return *leader;
    }

    static auto processlog_indexResponse(network::Response &&resp) -> log_index {
        if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
            auto r = resp.combinedResult();
            r = r.mapError([&](result::Error error) {
                error.appendErrorMessage(" while contacting server " + resp.serverId());
                return error;
            });
            THROW_DBMS_EXCEPTION(r);
        } else {
            auto slice = resp.slice();
            if (auto result = slice.get("result"); result.isObject() && result.length() == 1) {
                return result.get("index").extract<log_index>();
            }
            THROW_DBMS_EXCEPTION_MESSAGE(
                TRI_ERROR_INTERNAL,
                basics::StringUtils::concatT("expected result containing index in leader response: ", slice.toJson()));
        }
    }

public:
    TRI_vocbase_t &_vocbase;
    ClusterInfo &_clusterInfo;
    network::ConnectionPool *_pool;
};

auto prototype_state_methods::create_instance(TRI_vocbase_t &vocbase) -> std::shared_ptr<prototype_state_methods> {
    switch (ServerState::instance()->getRole()) {
        case ServerState::ROLE_COORDINATOR:
            return std::make_shared<prototype_state_methodsCoordinator>(vocbase);
        case ServerState::ROLE_DBSERVER:
            return std::make_shared<prototype_state_methodsDBServer>(vocbase);
        default:
            THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_NOT_IMPLEMENTED, "api only on available coordinators or dbservers");
    }
}

[[nodiscard]] auto prototype_state_methods::get(log_id id, std::string key, log_index wait_for_applied) const
    -> futures::Future<ResultT<std::optional<std::string>>> {
    return get(id, std::move(key), {wait_for_applied, std::nullopt});
}

[[nodiscard]] auto prototype_state_methods::get(log_id id, std::vector<std::string> keys, log_index wait_for_applied) const
    -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> {
    return get(id, std::move(keys), {wait_for_applied, std::nullopt});
}

[[nodiscard]] auto prototype_state_methods::get(log_id id, std::string key, ReadOptions const &readOptions) const
    -> futures::Future<ResultT<std::optional<std::string>>> {
    return get(id, std::vector {key}, readOptions)
        .thenValue([key](ResultT<std::unordered_map<std::string, std::string>> &&result)
                       -> ResultT<std::optional<std::string>> {
            if (result.ok()) {
                auto &map = result.get();
                if (auto iter = map.find(key); iter != std::end(map)) {
                    return {std::move(iter->second)};
                } else {
                    return {std::nullopt};
                }
            }
            return result.result();
        });
}
