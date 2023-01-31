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

#include <basics/exceptions.h>
#include <basics/voc_errors.h>
#include <futures/Future.h>

#include <features/ApplicationServer.h>
#include <nil/dbms/cluster/cluster_feature.hpp>
#include <nil/dbms/cluster/cluster_info.hpp>
#include <nil/dbms/cluster/server_state.hpp>
#include "inspection/vpack.h"
#include <nil/dbms/network/methods.hpp>
#include <nil/dbms/replication/agency_methods.hpp>
#include <nil/dbms/replication/exceptions/participant_resigned_exception.hpp>
#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/log/agency_specification_inspectors.hpp>
#include <nil/dbms/replication/log/log_leader.hpp>
#include <nil/dbms/replication/log/log_status.hpp>
#include "nil/dbms/replication/log/log.hpp"
#include "nil/dbms/replication/state/state.hpp"
#include "nil/dbms/vocbase/vocbase.hpp"

#include <nil/dbms/agency/agency_paths.hpp>
#include <nil/dbms/agency/async_agency_comm.hpp>
#include <nil/dbms/replication/methods.hpp>
#include "random/RandomGenerator.h"

#include "basics/result.h"
#include "basics/result.tpp"

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::log;

namespace {
    struct replicated_log_methodsDBServer final : replicated_log_methods,
                                                std::enable_shared_from_this<replicated_log_methodsDBServer> {
        explicit replicated_log_methodsDBServer(TRI_vocbase_t &vocbase) : vocbase(vocbase) {
        }
        auto wait_for_log_ready(log_id id, std::uint64_t version) const
            -> futures::Future<ResultT<consensus::index_t>> override {
            THROW_DBMS_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
        }

        auto create_replicated_log(CreateOptions spec) const -> futures::Future<ResultT<CreateResult>> override {
            THROW_DBMS_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
        }

        auto create_replicated_log(replication::agency::LogTarget spec) const -> futures::Future<Result> override {
            THROW_DBMS_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
        }

        auto delete_replicated_log(log_id id) const -> futures::Future<Result> override {
            THROW_DBMS_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
        }

        auto get_replicated_logs() const
            -> futures::Future<std::unordered_map<nil::dbms::replication::log_id,
                                                  std::variant<log::log_status, ParticipantsList>>> override {
            auto result = std::unordered_map<nil::dbms::replication::log_id,
                                             std::variant<log::log_status, ParticipantsList>> {};
            for (auto &replicatedLog : vocbase.get_replicated_logs()) {
                result[replicatedLog.first] = std::move(replicatedLog.second);
            }
            return result;
        }

        auto get_local_status(log_id id) const -> futures::Future<replication::log::log_status> override {
            return vocbase.getReplicatedLogById(id)->getParticipant()->get_status();
        }

        [[noreturn]] auto get_global_status(log_id id, log::global_status::SpecificationSource) const
            -> futures::Future<replication::log::global_status> override {
            THROW_DBMS_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
        }

        auto get_status(log_id id) const -> futures::Future<Genericlog_status> override {
            return get_local_status(id).thenValue([](log_status &&status) { return Genericlog_status(std::move(status)); });
        }

        auto get_log_entry_by_index(log_id id, log_index index) const
            -> futures::Future<std::optional<PersistingLogEntry>> override {
            auto entry = vocbase.getReplicatedLogById(id)->getParticipant()->copyInMemoryLog().getEntryByIndex(index);
            if (entry.has_value()) {
                return entry->entry();
            } else {
                return std::nullopt;
            }
        }

        auto slice(log_id id, log_index start, log_index stop) const
            -> futures::Future<std::unique_ptr<persisted_logIterator>> override {
            return vocbase.getReplicatedLogById(id)->getParticipant()->copyInMemoryLog().getInternalIteratorRange(start,
                                                                                                                  stop);
        }

        auto poll(log_id id, log_index index, std::size_t limit) const
            -> futures::Future<std::unique_ptr<persisted_logIterator>> override {
            auto leader = vocbase.getReplicatedlog_leaderById(id);
            return vocbase.getReplicatedLogById(id)->getParticipant()->waitFor(index).thenValue(
                [index, limit, leader = std::move(leader),
                 self = shared_from_this()](auto &&) -> std::unique_ptr<persisted_logIterator> {
                    auto log = leader->copyInMemoryLog();
                    return log.getInternalIteratorRange(index, index + limit);
                });
        }

        auto tail(log_id id, std::size_t limit) const
            -> futures::Future<std::unique_ptr<persisted_logIterator>> override {
            auto log = vocbase.getReplicatedLogById(id)->getParticipant()->copyInMemoryLog();
            auto stop = log.getNextIndex();
            auto start = stop.saturatedDecrement(limit);
            return log.getInternalIteratorRange(start, stop);
        }

        auto head(log_id id, std::size_t limit) const
            -> futures::Future<std::unique_ptr<persisted_logIterator>> override {
            auto log = vocbase.getReplicatedLogById(id)->getParticipant()->copyInMemoryLog();
            auto start = log.getFirstIndex();
            return log.getInternalIteratorRange(start, start + limit);
        }

        auto insert(log_id id, log_payload payload, bool waitForSync) const
            -> futures::Future<std::pair<log_index, log::wait_for_result>> override {
            auto log = vocbase.getReplicatedlog_leaderById(id);
            auto idx = log->insert(std::move(payload), waitForSync);
            return log->waitFor(idx).thenValue(
                [idx](auto &&result) { return std::make_pair(idx, std::forward<decltype(result)>(result)); });
        }

        auto insert(log_id id, TypedLogIterator<log_payload> &iter, bool waitForSync) const
            -> futures::Future<std::pair<std::vector<log_index>, log::wait_for_result>> override {
            auto log = vocbase.getReplicatedlog_leaderById(id);
            auto indexes = std::vector<log_index> {};
            while (auto payload = iter.next()) {
                auto idx = log->insert(std::move(*payload));
                indexes.push_back(idx);
            }
            if (indexes.empty()) {
                THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_BAD_PARAMETER, "multi insert list must not be empty");
            }

            return log->waitFor(indexes.back()).thenValue([indexes = std::move(indexes)](auto &&result) mutable {
                return std::make_pair(std::move(indexes), std::forward<decltype(result)>(result));
            });
        }

        auto insert_without_commit(log_id id, log_payload payload, bool waitForSync) const
            -> futures::Future<log_index> override {
            auto log = vocbase.getReplicatedlog_leaderById(id);
            auto idx = log->insert(std::move(payload), waitForSync);
            return {idx};
        }

        auto release(log_id id, log_index index) const -> futures::Future<Result> override {
            auto log = vocbase.getReplicatedLogById(id);
            return log->getParticipant()->release(index);
        }
        TRI_vocbase_t &vocbase;
    };

    namespace {
        struct VPackLogIterator final : persisted_logIterator {
            explicit VPackLogIterator(std::shared_ptr<velocypack::Buffer<uint8_t>> buffer_ptr) :
                buffer(std::move(buffer_ptr)), iter(VPackSlice(buffer->data()).get("result")), end(iter.end()) {
            }

            auto next() -> std::optional<PersistingLogEntry> override {
                while (iter != end) {
                    return PersistingLogEntry::from_velocy_pack(*iter++);
                }
                return std::nullopt;
            }

        private:
            std::shared_ptr<velocypack::Buffer<uint8_t>> buffer;
            VPackArrayIterator iter;
            VPackArrayIterator end;
        };

    }    // namespace

    struct replicated_log_methodsCoordinator final : replicated_log_methods,
                                                   std::enable_shared_from_this<replicated_log_methodsCoordinator> {
        auto wait_for_log_ready(log_id id, std::uint64_t version) const
            -> futures::Future<ResultT<consensus::index_t>> override {
            struct Context {
                explicit Context(uint64_t version) : version(version) {
                }
                futures::Promise<ResultT<consensus::index_t>> promise;
                std::uint64_t version;
            };

            auto ctx = std::make_shared<Context>(version);
            auto f = ctx->promise.getFuture();

            using namespace cluster::paths;
            // register an agency callback and wait for the given version to appear in
            // target (or bigger)
            auto path = aliases::current()->replicatedLogs()->database(vocbase.name())->log(id)->supervision();
            auto cb = std::make_shared<AgencyCallback>(
                vocbase.server(), path->str(SkipComponents(1)),
                [ctx](velocypack::Slice slice, consensus::index_t index) -> bool {
                    if (slice.isNone()) {
                        return false;
                    }

                    auto supervision = velocypack::deserialize<replication::agency::LogCurrentSupervision>(slice);
                    if (supervision.targetVersion >= ctx->version) {
                        ctx->promise.setValue(ResultT<consensus::index_t> {index});
                        return true;
                    }
                    return false;
                },
                true, true);
            if (auto result = clusterFeature.agencyCallbackRegistry()->registerCallback(cb, true); result.fail()) {
                return {result};
            }

            return std::move(f).then([self = shared_from_this(), cb](auto &&result) {
                self->clusterFeature.agencyCallbackRegistry()->unregisterCallback(cb);
                return std::move(result.get());
            });
        }

        void fillCreateOptions(CreateOptions &options) const {
            if (!options.id.has_value()) {
                options.id = log_id {clusterInfo.uniqid()};
            }

            auto dbservers = clusterInfo.getCurrentDBServers();

            auto expectedNumberOfServers = std::min(dbservers.size(), std::size_t {3});
            if (!options.servers.empty()) {
                expectedNumberOfServers = options.servers.size();
            }

            if (!options.config.has_value()) {
                options.config = nil::dbms::replication::agency::LogTargetConfig {2, expectedNumberOfServers, false};
            }

            if (expectedNumberOfServers > dbservers.size()) {
                THROW_DBMS_EXCEPTION(TRI_ERROR_CLUSTER_INSUFFICIENT_DBSERVERS);
            }

            // always make sure that the wished leader is part of the set of servers
            if (options.leader) {
                if (auto iter = std::find(options.servers.begin(), options.servers.end(), *options.leader);
                    iter == options.servers.end()) {
                    options.servers.emplace_back(*options.leader);
                }
            }

            if (options.servers.size() < expectedNumberOfServers) {
                auto newEnd = dbservers.end();
                if (!options.servers.empty()) {
                    newEnd = std::remove_if(dbservers.begin(), dbservers.end(), [&](ParticipantId const &server) {
                        return std::find(options.servers.begin(), options.servers.end(), server) !=
                               options.servers.end();
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

        static auto createTargetFromCreateOptions(CreateOptions const &options) -> replication::agency::LogTarget {
            replication::agency::LogTarget target;
            target.id = options.id.value();
            target.config = options.config.value();
            target.leader = options.leader;
            target.version = 1;
            for (auto const &server : options.servers) {
                target.participants[server];
            }
            return target;
        }

        auto create_replicated_log(CreateOptions options) const -> futures::Future<ResultT<CreateResult>> override {
            fillCreateOptions(options);
            TRI_ASSERT(options.id.has_value());
            auto target = createTargetFromCreateOptions(options);

            return create_replicated_log(std::move(target))
                .thenValue([options = std::move(options), self = shared_from_this()](
                               auto &&result) mutable -> futures::Future<ResultT<CreateResult>> {
                    auto response = CreateResult {*options.id, std::move(options.servers)};
                    if (!result.ok()) {
                        return {result};
                    }

                    if (options.waitForReady) {
                        // wait for the state to be ready
                        return self->wait_for_log_ready(*options.id, 1)
                            .thenValue([self, resp = std::move(response)](
                                           auto &&result) mutable -> futures::Future<ResultT<CreateResult>> {
                                if (result.fail()) {
                                    return {result.result()};
                                }
                                return self->clusterInfo.waitForPlan(result.get())
                                    .thenValue(
                                        [resp = std::move(resp)](auto &&result) mutable -> ResultT<CreateResult> {
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

        auto create_replicated_log(replication::agency::LogTarget spec) const -> futures::Future<Result> override {
            return replication::agency::methods::create_replicated_log(vocbase.name(), spec)
                .thenValue([self = shared_from_this()](ResultT<uint64_t> &&res) -> futures::Future<Result> {
                    if (res.fail()) {
                        return futures::Future<Result> {std::in_place, res.result()};
                    }

                    return self->clusterInfo.waitForPlan(res.get());
                });
        }

        auto delete_replicated_log(log_id id) const -> futures::Future<Result> override {
            return replication::agency::methods::delete_replicated_log(vocbase.name(), id)
                .thenValue([self = shared_from_this()](ResultT<uint64_t> &&res) -> futures::Future<Result> {
                    if (res.fail()) {
                        return futures::Future<Result> {std::in_place, res.result()};
                    }

                    return self->clusterInfo.waitForPlan(res.get());
                });
        }

        auto get_replicated_logs() const
            -> futures::Future<std::unordered_map<nil::dbms::replication::log_id,
                                                  std::variant<log::log_status, ParticipantsList>>> override {
            auto logsParticipants = clusterInfo.get_replicated_logsParticipants(vocbase.name());

            if (logsParticipants.fail()) {
                THROW_DBMS_EXCEPTION(logsParticipants.result());
            }

            auto result = std::unordered_map<nil::dbms::replication::log_id,
                                             std::variant<log::log_status, ParticipantsList>> {};
            for (auto &replicatedLog : logsParticipants.get()) {
                result[replicatedLog.first] = std::move(replicatedLog.second);
            }
            return result;
        }

        [[noreturn]] auto get_local_status(log_id id) const
            -> futures::Future<replication::log::log_status> override {
            THROW_DBMS_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
        }

        auto get_global_status(log_id id, log::global_status::SpecificationSource source) const
            -> futures::Future<replication::log::global_status> override {
            // 1. Determine which source to use for gathering information
            // 2. Query information from all sources
            auto futureSpec = loadLogSpecification(vocbase.name(), id, source);
            return std::move(futureSpec)
                .thenValue([self = shared_from_this(),
                            source](ResultT<std::shared_ptr<replication::agency::LogPlanSpecification const>> result) {
                    if (result.fail()) {
                        THROW_DBMS_EXCEPTION(result.result());
                    }

                    auto const &spec = result.get();
                    TRI_ASSERT(spec != nullptr);
                    return self->collectglobal_statusUsingSpec(spec, source);
                });
        }

        auto get_status(log_id id) const -> futures::Future<Genericlog_status> override {
            return get_global_status(id, global_status::SpecificationSource::kRemoteAgency)
                .thenValue([](global_status &&status) { return Genericlog_status(std::move(status)); });
        }

        auto get_log_entry_by_index(log_id id, log_index index) const
            -> futures::Future<std::optional<PersistingLogEntry>> override {
            auto path = basics::StringUtils::joinT("/", "_api/log", id, "entry", index.value);
            network::RequestOptions opts;
            opts.database = vocbase.name();
            return network::sendRequest(pool, "server:" + getlog_leader(id), fuerte::RestVerb::Get, path, {}, opts)
                .thenValue([](network::Response &&resp) {
                    if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                        THROW_DBMS_EXCEPTION(resp.combinedResult());
                    }
                    auto entry = PersistingLogEntry::from_velocy_pack(resp.slice().get("result"));
                    return std::optional<PersistingLogEntry>(std::move(entry));
                });
        }

        auto slice(log_id id, log_index start, log_index stop) const
            -> futures::Future<std::unique_ptr<persisted_logIterator>> override {
            auto path = basics::StringUtils::joinT("/", "_api/log", id, "slice");

            network::RequestOptions opts;
            opts.database = vocbase.name();
            opts.parameters["start"] = to_string(start);
            opts.parameters["stop"] = to_string(stop);
            return network::sendRequest(pool, "server:" + getlog_leader(id), fuerte::RestVerb::Get, path, {}, opts)
                .thenValue([](network::Response &&resp) -> std::unique_ptr<persisted_logIterator> {
                    if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                        THROW_DBMS_EXCEPTION(resp.combinedResult());
                    }

                    return std::make_unique<VPackLogIterator>(resp.response().stealPayload());
                });
        }

        auto poll(log_id id, log_index index, std::size_t limit) const
            -> futures::Future<std::unique_ptr<persisted_logIterator>> override {
            auto path = basics::StringUtils::joinT("/", "_api/log", id, "poll");

            network::RequestOptions opts;
            opts.database = vocbase.name();
            opts.parameters["first"] = to_string(index);
            opts.parameters["limit"] = std::to_string(limit);
            return network::sendRequest(pool, "server:" + getlog_leader(id), fuerte::RestVerb::Get, path, {}, opts)
                .thenValue([](network::Response &&resp) -> std::unique_ptr<persisted_logIterator> {
                    if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                        THROW_DBMS_EXCEPTION(resp.combinedResult());
                    }

                    return std::make_unique<VPackLogIterator>(resp.response().stealPayload());
                });
        }

        auto tail(log_id id, std::size_t limit) const
            -> futures::Future<std::unique_ptr<persisted_logIterator>> override {
            auto path = basics::StringUtils::joinT("/", "_api/log", id, "tail");

            network::RequestOptions opts;
            opts.database = vocbase.name();
            opts.parameters["limit"] = std::to_string(limit);
            return network::sendRequest(pool, "server:" + getlog_leader(id), fuerte::RestVerb::Get, path, {}, opts)
                .thenValue([](network::Response &&resp) -> std::unique_ptr<persisted_logIterator> {
                    if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                        THROW_DBMS_EXCEPTION(resp.combinedResult());
                    }

                    return std::make_unique<VPackLogIterator>(resp.response().stealPayload());
                });
        }

        auto head(log_id id, std::size_t limit) const
            -> futures::Future<std::unique_ptr<persisted_logIterator>> override {
            auto path = basics::StringUtils::joinT("/", "_api/log", id, "head");

            network::RequestOptions opts;
            opts.database = vocbase.name();
            opts.parameters["limit"] = std::to_string(limit);
            return network::sendRequest(pool, "server:" + getlog_leader(id), fuerte::RestVerb::Get, path, {}, opts)
                .thenValue([](network::Response &&resp) -> std::unique_ptr<persisted_logIterator> {
                    if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                        THROW_DBMS_EXCEPTION(resp.combinedResult());
                    }

                    return std::make_unique<VPackLogIterator>(resp.response().stealPayload());
                });
        }

        auto insert(log_id id, log_payload payload, bool waitForSync) const
            -> futures::Future<std::pair<log_index, log::wait_for_result>> override {
            auto path = basics::StringUtils::joinT("/", "_api/log", id, "insert");

            network::RequestOptions opts;
            opts.database = vocbase.name();
            opts.param(StaticStrings::WaitForSyncString, waitForSync ? "true" : "false");
            return network::sendRequest(pool, "server:" + getlog_leader(id), fuerte::RestVerb::Post, path,
                                        payload.copy_buffer(), opts)
                .thenValue([](network::Response &&resp) {
                    if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                        THROW_DBMS_EXCEPTION(resp.combinedResult());
                    }
                    auto result = resp.slice().get("result");
                    auto waitResult = result.get("result");

                    auto quorum =
                        std::make_shared<replication::log::QuorumData const>(waitResult.get("quorum"));
                    auto commitIndex = waitResult.get("commitIndex").extract<log_index>();
                    auto index = result.get("index").extract<log_index>();
                    return std::make_pair(index, log::wait_for_result(commitIndex, std::move(quorum)));
                });
        }

        auto insert(log_id id, TypedLogIterator<log_payload> &iter, bool waitForSync) const
            -> futures::Future<std::pair<std::vector<log_index>, log::wait_for_result>> override {
            auto path = basics::StringUtils::joinT("/", "_api/log", id, "multi-insert");

            std::size_t payloadSize {0};
            VPackBuilder builder {};
            {
                VPackArrayBuilder arrayBuilder {&builder};
                while (auto const payload = iter.next()) {
                    builder.add(payload->slice());
                    ++payloadSize;
                }
            }

            network::RequestOptions opts;
            opts.database = vocbase.name();
            opts.param(StaticStrings::WaitForSyncString, waitForSync ? "true" : "false");
            return network::sendRequest(pool, "server:" + getlog_leader(id), fuerte::RestVerb::Post, path,
                                        builder.bufferRef(), opts)
                .thenValue([payloadSize](network::Response &&resp) {
                    if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                        THROW_DBMS_EXCEPTION(resp.combinedResult());
                    }
                    auto result = resp.slice().get("result");
                    auto waitResult = result.get("result");

                    auto quorum =
                        std::make_shared<replication::log::QuorumData const>(waitResult.get("quorum"));
                    auto commitIndex = waitResult.get("commitIndex").extract<log_index>();

                    std::vector<log_index> indexes;
                    indexes.reserve(payloadSize);
                    auto indexIter = velocypack::ArrayIterator(result.get("indexes"));
                    std::transform(indexIter.begin(), indexIter.end(), std::back_inserter(indexes),
                                   [](auto const &it) { return it.template extract<log_index>(); });
                    return std::make_pair(std::move(indexes),
                                          log::wait_for_result(commitIndex, std::move(quorum)));
                });
        }

        auto insert_without_commit(log_id id, log_payload payload, bool waitForSync) const
            -> futures::Future<log_index> override {
            auto path = basics::StringUtils::joinT("/", "_api/log", id, "insert");

            network::RequestOptions opts;
            opts.database = vocbase.name();
            opts.param(StaticStrings::WaitForSyncString, waitForSync ? "true" : "false");
            opts.param(StaticStrings::DontWaitForCommit, "true");
            return network::sendRequest(pool, "server:" + getlog_leader(id), fuerte::RestVerb::Post, path,
                                        payload.copy_buffer(), opts)
                .thenValue([](network::Response &&resp) {
                    if (resp.fail() || !fuerte::statusIsSuccess(resp.statusCode())) {
                        THROW_DBMS_EXCEPTION(resp.combinedResult());
                    }
                    auto result = resp.slice().get("result");
                    auto index = result.get("index").extract<log_index>();
                    return index;
                });
        }

        auto release(log_id id, log_index index) const -> futures::Future<Result> override {
            auto path = basics::StringUtils::joinT("/", "_api/log", id, "release");

            VPackBufferUInt8 body;
            {
                VPackBuilder builder(body);
                builder.add(VPackSlice::emptyObjectSlice());
            }

            network::RequestOptions opts;
            opts.database = vocbase.name();
            opts.parameters["index"] = to_string(index);
            return network::sendRequest(pool, "server:" + getlog_leader(id), fuerte::RestVerb::Post, path,
                                        std::move(body), opts)
                .thenValue([](network::Response &&resp) { return resp.combinedResult(); });
        }

        explicit replicated_log_methodsCoordinator(TRI_vocbase_t &vocbase) :
            vocbase(vocbase), clusterFeature(vocbase.server().getFeature<ClusterFeature>()),
            clusterInfo(clusterFeature.clusterInfo()), pool(vocbase.server().getFeature<NetworkFeature>().pool()) {
        }

    private:
        auto getlog_leader(log_id id) const -> ServerID {
            auto leader = clusterInfo.getReplicatedlog_leader(vocbase.name(), id);
            if (leader.fail()) {
                if (leader.is(TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED)) {
                    throw participant_resigned_exception(leader.result(), ADB_HERE);
                } else {
                    THROW_DBMS_EXCEPTION(leader.result());
                }
            }

            return *leader;
        }

        auto loadLogSpecification(DatabaseID const &database, replication::log_id id,
                                  global_status::SpecificationSource source) const
            -> futures::Future<ResultT<std::shared_ptr<nil::dbms::replication::agency::LogPlanSpecification const>>> {
            if (source == global_status::SpecificationSource::kLocalCache) {
                return clusterInfo.getReplicatedLogPlanSpecification(database, id);
            } else {
                AsyncAgencyComm ac;
                auto f = ac.getValues(
                    nil::dbms::cluster::paths::aliases::plan()->replicatedLogs()->database(database)->log(id),
                    std::chrono::seconds {5});

                return std::move(f).then(
                    [self = shared_from_this(), id](futures::Try<AgencyReadResult> &&tryResult)
                        -> ResultT<std::shared_ptr<nil::dbms::replication::agency::LogPlanSpecification const>> {
                        auto result = basics::catchToResultT([&] { return std::move(tryResult.get()); });

                        if (result.fail()) {
                            return result.result();
                        }

                        if (result->value().isNone()) {
                            return Result::fmt(TRI_ERROR_REPLICATION_REPLICATED_LOG_NOT_FOUND, id);
                        }

                        auto spec = velocypack::deserialize<nil::dbms::replication::agency::LogPlanSpecification>(
                            result->value());

                        return {
                            std::make_shared<nil::dbms::replication::agency::LogPlanSpecification>(std::move(spec))};
                    });
            }
        }

        auto readSupervisionStatus(replication::log_id id) const -> futures::Future<global_status::SupervisionStatus> {
            AsyncAgencyComm ac;
            using Status = global_status::SupervisionStatus;
            // TODO move this into the agency methods
            auto f = ac.getValues(nil::dbms::cluster::paths::aliases::current()
                                      ->replicatedLogs()
                                      ->database(vocbase.name())
                                      ->log(id)
                                      ->supervision(),
                                  std::chrono::seconds {5});
            return std::move(f).then([self = shared_from_this()](futures::Try<AgencyReadResult> &&tryResult) {
                auto result = basics::catchToResultT([&] { return std::move(tryResult.get()); });

                auto const statusFromResult = [](Result const &res) {
                    return Status {
                        .connection = {.error = res.errorNumber(), .errorMessage = std::string {res.errorMessage()}},
                        .response = std::nullopt};
                };

                if (result.fail()) {
                    return statusFromResult(result.result());
                }
                auto &read = result.get();
                auto status = statusFromResult(read.asResult());
                if (read.ok() && !read.value().isNone()) {
                    status.response.emplace(
                        velocypack::deserialize<nil::dbms::replication::agency::LogCurrentSupervision>(read.value()));
                }

                return status;
            });
        }

        auto queryParticipantsStatus(replication::log_id id, replication::ParticipantId const &participant) const
            -> futures::Future<global_status::participant_status> {
            using Status = global_status::participant_status;
            auto path = basics::StringUtils::joinT("/", "_api/log", id, "local-status");
            network::RequestOptions opts;
            opts.database = vocbase.name();
            opts.timeout = std::chrono::seconds {5};
            return network::sendRequest(pool, "server:" + participant, fuerte::RestVerb::Get, path, {}, opts)
                .then([](futures::Try<network::Response> &&tryResult) mutable {
                    auto result = basics::catchToResultT([&] { return std::move(tryResult.get()); });

                    auto const statusFromResult = [](Result const &res) {
                        return Status {.connection = {.error = res.errorNumber(),
                                                      .errorMessage = std::string {res.errorMessage()}},
                                       .response = std::nullopt};
                    };

                    if (result.fail()) {
                        return statusFromResult(result.result());
                    }

                    auto &response = result.get();
                    auto status = statusFromResult(response.combinedResult());
                    if (response.combinedResult().ok()) {
                        status.response = global_status::participant_status::Response {
                            .value = replication::log::log_status::from_velocy_pack(
                                response.slice().get("result"))};
                    }
                    return status;
                });
        }

        auto collectglobal_statusUsingSpec(std::shared_ptr<replication::agency::LogPlanSpecification const> spec,
                                          global_status::SpecificationSource source) const
            -> futures::Future<global_status> {
            // send of a request to all participants

            auto psf = std::invoke([&] {
                auto const &participants = spec->participants_config.participants;
                std::vector<futures::Future<global_status::participant_status>> pfs;
                pfs.reserve(participants.size());
                for (auto const &[id, flags] : participants) {
                    pfs.emplace_back(queryParticipantsStatus(spec->id, id));
                }
                return futures::collectAll(pfs);
            });
            auto af = readSupervisionStatus(spec->id);
            return futures::collect(std::move(af), std::move(psf)).thenValue([spec, source](auto &&pairResult) {
                auto &[agency, participantResults] = pairResult;

                auto leader = std::optional<ParticipantId> {};
                if (spec->currentTerm && spec->currentTerm->leader) {
                    leader = spec->currentTerm->leader->serverId;
                }
                auto participantsMap = std::unordered_map<ParticipantId, global_status::participant_status> {};

                auto const &participants = spec->participants_config.participants;
                std::size_t idx = 0;
                for (auto const &[id, flags] : participants) {
                    auto &result = participantResults.at(idx++);
                    participantsMap[id] = std::move(result.get());
                }

                return global_status {.supervision = agency,
                                     .participants = participantsMap,
                                     .specification = {.source = source, .plan = *spec},
                                     .leaderId = std::move(leader)};
            });
        }

        TRI_vocbase_t &vocbase;
        ClusterFeature &clusterFeature;
        ClusterInfo &clusterInfo;
        network::ConnectionPool *pool;
    };

    struct ReplicatedStateDBServerMethods : std::enable_shared_from_this<ReplicatedStateDBServerMethods>,
                                            replicated_state_methods {
        explicit ReplicatedStateDBServerMethods(TRI_vocbase_t &vocbase) : vocbase(vocbase) {
        }

        auto create_replicated_state(state::agency::Target spec) const -> futures::Future<Result> override {
            THROW_DBMS_EXCEPTION(TRI_ERROR_HTTP_NOT_IMPLEMENTED);
        }

        auto delete_replicated_state(log_id id) const -> futures::Future<Result> override {
            THROW_DBMS_EXCEPTION(TRI_ERROR_HTTP_NOT_IMPLEMENTED);
        }

        [[nodiscard]] auto waitForStateReady(log_id, std::uint64_t)
            -> futures::Future<ResultT<consensus::index_t>> override {
            THROW_DBMS_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
        }

        auto get_local_status(log_id id) const -> futures::Future<state::StateStatus> override {
            auto state = vocbase.getReplicatedStateById(id);
            if (auto status = state->get_status(); status.has_value()) {
                return std::move(*status);
            }
            THROW_DBMS_EXCEPTION(TRI_ERROR_HTTP_NOT_IMPLEMENTED);
        }

        auto replaceParticipant(log_id log_id, ParticipantId const &participantToRemove,
                                ParticipantId const &participantToAdd,
                                std::optional<ParticipantId> const &currentLeader) const
            -> futures::Future<Result> override {
            // Only available on the coordinator
            THROW_DBMS_EXCEPTION(TRI_ERROR_HTTP_NOT_IMPLEMENTED);
        }

        auto setLeader(log_id id, std::optional<ParticipantId> const &leaderId) const
            -> futures::Future<Result> override {
            // Only available on the coordinator
            THROW_DBMS_EXCEPTION(TRI_ERROR_HTTP_NOT_IMPLEMENTED);
        }

        auto get_global_snapshot_status(log_id) const -> futures::Future<ResultT<GlobalSnapshotStatus>> override {
            // Only available on the coordinator
            THROW_DBMS_EXCEPTION(TRI_ERROR_HTTP_NOT_IMPLEMENTED);
        }

        TRI_vocbase_t &vocbase;
    };

    struct ReplicatedStateCoordinatorMethods : std::enable_shared_from_this<ReplicatedStateCoordinatorMethods>,
                                               replicated_state_methods {
        explicit ReplicatedStateCoordinatorMethods(DbmsdServer &server, std::string databaseName) :
            server(server), clusterFeature(server.getFeature<ClusterFeature>()),
            clusterInfo(clusterFeature.clusterInfo()), databaseName(std::move(databaseName)) {
        }

        auto create_replicated_state(state::agency::Target spec) const -> futures::Future<Result> override {
            return replication::agency::methods::create_replicated_state(databaseName, spec)
                .thenValue([self = shared_from_this()](ResultT<uint64_t> &&res) -> futures::Future<Result> {
                    if (res.fail()) {
                        return futures::Future<Result> {std::in_place, res.result()};
                    }

                    return self->clusterInfo.waitForPlan(res.get());
                });
        }

        [[nodiscard]] virtual auto waitForStateReady(log_id id, std::uint64_t version)
            -> futures::Future<ResultT<consensus::index_t>> override {
            struct Context {
                explicit Context(uint64_t version) : version(version) {
                }
                futures::Promise<ResultT<consensus::index_t>> promise;
                std::uint64_t version;
            };

            auto ctx = std::make_shared<Context>(version);
            auto f = ctx->promise.getFuture();

            using namespace cluster::paths;
            // register an agency callback and wait for the given version to appear in
            // target (or bigger)
            auto path = aliases::current()->replicatedStates()->database(databaseName)->state(id)->supervision();
            auto cb = std::make_shared<AgencyCallback>(
                server, path->str(SkipComponents(1)),
                [ctx](velocypack::Slice slice, consensus::index_t index) -> bool {
                    if (slice.isNone()) {
                        return false;
                    }

                    auto supervision = velocypack::deserialize<state::agency::Current::Supervision>(slice);
                    if (supervision.version >= ctx->version) {
                        ctx->promise.setValue(ResultT<consensus::index_t> {index});
                        return true;
                    }
                    return false;
                },
                true, true);
            if (auto result = clusterFeature.agencyCallbackRegistry()->registerCallback(cb, true); result.fail()) {
                return {result};
            }

            return std::move(f).then([self = shared_from_this(), cb](auto &&result) {
                self->clusterFeature.agencyCallbackRegistry()->unregisterCallback(cb);
                return std::move(result.get());
            });
        }

        auto delete_replicated_state(log_id id) const -> futures::Future<Result> override {
            return replication::agency::methods::delete_replicated_state(databaseName, id)
                .thenValue([self = shared_from_this()](ResultT<uint64_t> &&res) -> futures::Future<Result> {
                    if (res.fail()) {
                        return futures::Future<Result> {std::in_place, res.result()};
                    }

                    return self->clusterInfo.waitForPlan(res.get());
                });
        }

        auto get_local_status(log_id id) const -> futures::Future<state::StateStatus> override {
            THROW_DBMS_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
        }

        auto replaceParticipant(log_id id, ParticipantId const &participantToRemove,
                                ParticipantId const &participantToAdd,
                                std::optional<ParticipantId> const &currentLeader) const
            -> futures::Future<Result> override {
            return replication::agency::methods::replace_replicated_state_participant(
                databaseName, id, participantToRemove, participantToAdd, currentLeader);
        }

        auto setLeader(log_id id, std::optional<ParticipantId> const &leaderId) const
            -> futures::Future<Result> override {
            return replication::agency::methods::replace_replicated_set_leader(databaseName, id, leaderId);
        }

        auto get_global_snapshot_status(log_id id) const -> futures::Future<ResultT<GlobalSnapshotStatus>> override {
            AsyncAgencyComm ac;
            auto f = ac.getValues(
                nil::dbms::cluster::paths::aliases::current()->replicatedStates()->database(databaseName)->state(id),
                std::chrono::seconds {5});
            return std::move(f).then([self = shared_from_this(),
                                      id](futures::Try<AgencyReadResult> &&tryResult) -> ResultT<GlobalSnapshotStatus> {
                auto result = basics::catchToResultT([&] { return std::move(tryResult.get()); });

                if (result.fail()) {
                    return result.result();
                }
                if (result->value().isNone()) {
                    return Result::fmt(TRI_ERROR_REPLICATION_REPLICATED_LOG_NOT_FOUND, id.id());
                }
                auto current = velocypack::deserialize<state::agency::Current>(result->value());

                GlobalSnapshotStatus status;
                for (auto const &[p, s] : current.participants) {
                    status[p] = ParticipantSnapshotStatus {.status = s.snapshot, .generation = s.generation};
                }

                return status;
            });
        }

        DbmsdServer &server;
        ClusterFeature &clusterFeature;
        ClusterInfo &clusterInfo;
        std::string const databaseName;
    };

}    // namespace

auto replicated_log_methods::create_instance(TRI_vocbase_t &vocbase) -> std::shared_ptr<replicated_log_methods> {
    switch (ServerState::instance()->getRole()) {
        case ServerState::ROLE_COORDINATOR:
            return std::make_shared<replicated_log_methodsCoordinator>(vocbase);
        case ServerState::ROLE_DBSERVER:
            return std::make_shared<replicated_log_methodsDBServer>(vocbase);
        default:
            THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_NOT_IMPLEMENTED, "api only on available coordinators or dbservers");
    }
}

auto replicated_state_methods::create_instance(TRI_vocbase_t &vocbase) -> std::shared_ptr<replicated_state_methods> {
    switch (ServerState::instance()->getRole()) {
        case ServerState::ROLE_DBSERVER:
            return create_instanceDBServer(vocbase);
        case ServerState::ROLE_COORDINATOR:
            return create_instanceCoordinator(vocbase.server(), vocbase.name());
        default:
            THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_NOT_IMPLEMENTED, "api only on available coordinators or dbservers");
    }
}

auto replicated_state_methods::create_instanceDBServer(TRI_vocbase_t &vocbase) -> std::shared_ptr<replicated_state_methods> {
    ADB_PROD_ASSERT(ServerState::instance()->getRole() == ServerState::ROLE_DBSERVER);
    return std::make_shared<ReplicatedStateDBServerMethods>(vocbase);
}

auto replicated_state_methods::create_instanceCoordinator(DbmsdServer &server, std::string databaseName)
    -> std::shared_ptr<replicated_state_methods> {
    ADB_PROD_ASSERT(ServerState::instance()->getRole() == ServerState::ROLE_COORDINATOR);
    return std::make_shared<ReplicatedStateCoordinatorMethods>(server, std::move(databaseName));
}
