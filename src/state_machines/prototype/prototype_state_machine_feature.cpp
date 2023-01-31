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

#include <nil/dbms/replication/state/state_feature.hpp>

#include <nil/dbms/replication/state_machines/prototype/prototype_core.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_follower_state.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_leader_state.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_state_machine.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_state_machine_feature.hpp>

#include <features/ApplicationServer.h>
#include <nil/dbms/cluster/server_state.hpp>
#include <nil/dbms/network/methods.hpp>
#include <nil/dbms/network/network_feature.hpp>
#include <nil/dbms/engine/dcdb/dcdb_engine.hpp>
#include <nil/dbms/engine/storage/engine_selector_feature.hpp>
#include "velocypack/Iterator.h"

#include <rocksdb/utilities/transaction_db.h>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::state;
using namespace nil::dbms::replication::state::prototype;

namespace {
    class PrototypeLeaderInterface : public iprototype_leader_interface {
    public:
        PrototypeLeaderInterface(ParticipantId participantId, network::ConnectionPool *pool) :
            _participantId(std::move(participantId)), _pool(pool) {};

        auto get_snapshot(global_log_identifier const &log_id, log_index waitForIndex)
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> override {
            auto path = basics::StringUtils::joinT("/", "_api/prototype-state", log_id.id, "snapshot");
            network::RequestOptions opts;
            opts.database = log_id.database;
            opts.param("waitForIndex", std::to_string(waitForIndex.value));

            return network::sendRequest(_pool, "server:" + _participantId, fuerte::RestVerb::Get, path, {}, opts)
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

    private:
        ParticipantId _participantId;
        network::ConnectionPool *_pool;
    };

    class PrototypeNetworkInterface : public iprototype_network_interface {
    public:
        explicit PrototypeNetworkInterface(network::ConnectionPool *pool) : _pool(pool) {};

        auto getLeaderInterface(ParticipantId id) -> ResultT<std::shared_ptr<iprototype_leader_interface>> override {
            return ResultT<std::shared_ptr<iprototype_leader_interface>>::success(
                std::make_shared<PrototypeLeaderInterface>(id, _pool));
        }

    private:
        network::ConnectionPool *_pool {nullptr};
    };

    class PrototypeRocksDBInterface : public iprototype_storage_interface {
    public:
        explicit PrototypeRocksDBInterface(rocksdb::TransactionDB *db) : _db(db) {
        }

        auto put(const global_log_identifier &log_id, prototype_dump dump) -> Result override {
            auto key = getDBKey(log_id);
            VPackBuilder builder;
            dump.to_velocy_pack(builder);
            auto value =
                rocksdb::Slice(reinterpret_cast<char const *>(builder.slice().start()), builder.slice().byte_size());
            auto opt = rocksdb::WriteOptions {};
            auto status = _db->Put(opt, rocksdb::Slice(key), value);
            if (status.ok()) {
                return TRI_ERROR_NO_ERROR;
            } else {
                return {TRI_ERROR_WAS_ERLAUBE, status.ToString()};
            }
        }

        auto get(const global_log_identifier &log_id) -> ResultT<prototype_dump> override {
            auto key = getDBKey(log_id);
            auto opt = rocksdb::ReadOptions {};
            std::string buffer;
            auto status = _db->Get(opt, rocksdb::Slice(key), &buffer);
            if (status.ok()) {
                VPackSlice slice {reinterpret_cast<uint8_t const *>(buffer.data())};
                return prototype_dump::from_velocy_pack(slice);
            } else if (status.code() == rocksdb::Status::kNotFound) {
                prototype_dump dump;
                dump.lastPersistedIndex = log_index {0};
                return dump;
            } else {
                LOG_TOPIC("db12d", ERR, nil::dbms::Logger::REPLICATED_STATE)
                    << "Error occurred while reading Prototype From RocksDB: " << status.ToString();
                return ResultT<prototype_dump>::error(TRI_ERROR_WAS_ERLAUBE, status.ToString());
            }
        }

        static std::string getDBKey(const global_log_identifier &log_id) {
            return "prototype-core-" + std::to_string(log_id.id.id());
        }

    private:
        rocksdb::TransactionDB *_db;
    };
}    // namespace

prototype_state_machine_feature::prototype_state_machine_feature(Server &server) : DbmsdFeature {server, *this} {
    setOptional(true);
    startsAfter<EngineSelectorFeature>();
    startsAfter<NetworkFeature>();
    startsAfter<RocksDBEngine>();
    startsAfter<ReplicatedStateAppFeature>();
    onlyEnabledWith<EngineSelectorFeature>();
    onlyEnabledWith<ReplicatedStateAppFeature>();
}

void prototype_state_machine_feature::prepare() {
    bool const enabled = ServerState::instance()->isDBServer();
    setEnabled(enabled);
}

void prototype_state_machine_feature::start() {
    auto &replicatedStateFeature = server().getFeature<ReplicatedStateAppFeature>();
    auto &networkFeature = server().getFeature<NetworkFeature>();
    auto &engine = server().getFeature<EngineSelectorFeature>().engine<RocksDBEngine>();

    rocksdb::TransactionDB *db = engine.db();
    TRI_ASSERT(db != nullptr);

    replicatedStateFeature.registerStateType<prototype_state>(
        "prototype",
        std::make_shared<PrototypeNetworkInterface>(networkFeature.pool()),
        std::make_shared<PrototypeRocksDBInterface>(db));
}
