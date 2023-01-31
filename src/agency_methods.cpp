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

#include <nil/dbms/replication/agency_methods.hpp>

#include <cstdint>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <velocypack/velocypack-common.h>

#include <nil/dbms/agency/agency_paths.hpp>
#include <nil/dbms/agency/async_agency_comm.hpp>
#include <nil/dbms/agency/transaction_builder.hpp>
#include <features/ApplicationServer.h>
#include "basics/exceptions.h"
#include "basics/string_utils.h"
#include <nil/dbms/cluster/agency_cache.hpp>
#include <nil/dbms/cluster/cluster_types.hpp>
#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/log/agency_specification_inspectors.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/vocbase/voc-types.hpp>
#include "nil/dbms/vocbase/vocbase.hpp"
#include "inspection/vpack.h"

namespace nil {
    namespace dbms {
        class Result;
    }
}    // namespace nil::dbms

using namespace std::chrono_literals;

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::agency;

namespace paths = nil::dbms::cluster::paths::aliases;

namespace {
    auto sendAgencyWriteTransaction(VPackBufferUInt8 trx) -> futures::Future <ResultT<uint64_t>> {
        AsyncAgencyComm ac;
        return ac.sendWriteTransaction(120s, std::move(trx))
                .thenValue([](AsyncAgencyCommResult &&res) -> ResultT <uint64_t> {
                    if (res.fail()) {
                        return res.asResult();
                    }

                    // extract raft index
                    auto slice = res.slice().get("results");
                    TRI_ASSERT(slice.isArray());
                    TRI_ASSERT(!slice.isEmptyArray());
                    return slice.at(slice.length() - 1).getNumericValue<uint64_t>();
                });
    }
}    // namespace

auto methods::update_term_specification_trx(nil::dbms::agency::envelope envelope, DatabaseID const &database, log_id id,
                                         LogPlanTermSpecification const &spec, std::optional <log_term> prevTerm)
-> nil::dbms::agency::envelope {
    auto path = paths::plan()->replicatedLogs()->database(database)->log(to_string(id));
    auto logPath = path->str();
    auto termPath = path->currentTerm()->str();

    return envelope.write()
            .emplace_object(termPath, [&](VPackBuilder &builder) { velocypack::serialize(builder, spec); })
            .inc(paths::plan()->version()->str())
            .precs()
            .isNotEmpty(logPath)
            .cond(
                    prevTerm.has_value(),
                    [&](auto &&precs) {
                        return std::move(precs).isEqual(path->currentTerm()->term()->str(), prevTerm->value);
                    })
            .end();
}

auto methods::update_participants_config_trx(nil::dbms::agency::envelope envelope, DatabaseID const &database, log_id id,
                                           participants_config const &participants_config,
                                           participants_config const &prevConfig) -> nil::dbms::agency::envelope {
    auto const logPath = paths::plan()->replicatedLogs()->database(database)->log(to_string(id));

    return envelope.write()
            .emplace_object(logPath->participants_config()->str(),
                            [&](VPackBuilder &builder) { velocypack::serialize(builder, participants_config); })
            .inc(paths::plan()->version()->str())
            .precs()
            .isNotEmpty(logPath->str())
            .end();
}

auto methods::update_term_specification(DatabaseID const &database, log_id id, LogPlanTermSpecification const &spec,
                                      std::optional <log_term> prevTerm) -> futures::Future <ResultT<uint64_t>> {
    VPackBufferUInt8 trx;
    {
        VPackBuilder builder(trx);
        update_term_specification_trx(nil::dbms::agency::envelope::into_builder(builder), database, id, spec, prevTerm)
                .done();
    }

    return sendAgencyWriteTransaction(std::move(trx));
}

auto methods::delete_replicated_log_trx(nil::dbms::agency::envelope envelope, DatabaseID const &database, log_id id)
-> nil::dbms::agency::envelope {
    auto planPath = paths::plan()->replicatedLogs()->database(database)->log(id)->str();
    auto targetPath = paths::target()->replicatedLogs()->database(database)->log(id)->str();
    auto currentPath = paths::current()->replicatedLogs()->database(database)->log(id)->str();

    return envelope.write()
            .remove(planPath)
            .inc(paths::plan()->version()->str())
            .remove(targetPath)
            .inc(paths::target()->version()->str())
            .remove(currentPath)
            .inc(paths::current()->version()->str())
            .end();
}

auto methods::delete_replicated_log(DatabaseID const &database, log_id id) -> futures::Future <ResultT<uint64_t>> {
    VPackBufferUInt8 trx;
    {
        VPackBuilder builder(trx);
        delete_replicated_log_trx(nil::dbms::agency::envelope::into_builder(builder), database, id).done();
    }

    return sendAgencyWriteTransaction(std::move(trx));
}

auto methods::delete_replicated_state_trx(nil::dbms::agency::envelope envelope, DatabaseID const &database, log_id id)
-> nil::dbms::agency::envelope {
    auto planPath = paths::plan()->replicatedStates()->database(database)->state(id)->str();
    auto targetPath = paths::target()->replicatedStates()->database(database)->state(id)->str();
    auto currentPath = paths::current()->replicatedStates()->database(database)->state(id)->str();

    return envelope.write()
            .remove(planPath)
            .inc(paths::plan()->version()->str())
            .remove(targetPath)
            .inc(paths::target()->version()->str())
            .remove(currentPath)
            .inc(paths::current()->version()->str())
            .end();
}

auto methods::delete_replicated_state(DatabaseID const &database, log_id id) -> futures::Future <ResultT<uint64_t>> {
    VPackBufferUInt8 trx;
    {
        VPackBuilder builder(trx);
        delete_replicated_state_trx(nil::dbms::agency::envelope::into_builder(builder), database, id).done();
    }

    return sendAgencyWriteTransaction(std::move(trx));
}

auto methods::create_replicated_log_trx(nil::dbms::agency::envelope envelope,
                                     DatabaseID const &database,
                                     LogTarget const &spec) -> nil::dbms::agency::envelope {
    auto path = paths::target()->replicatedLogs()->database(database)->log(spec.id)->str();

    return envelope.write()
            .emplace_object(path, [&](VPackBuilder &builder) { velocypack::serialize(builder, spec); })
            .inc(paths::target()->version()->str())
            .precs()
            .isEmpty(path)
            .end();
}

auto methods::create_replicated_log(DatabaseID const &database, LogTarget const &spec)
-> futures::Future <ResultT<uint64_t>> {
    VPackBufferUInt8 trx;
    {
        VPackBuilder builder(trx);
        create_replicated_log_trx(nil::dbms::agency::envelope::into_builder(builder), database, spec).done();
    }
    return sendAgencyWriteTransaction(std::move(trx));
}

auto methods::remove_election_result(nil::dbms::agency::envelope envelope, DatabaseID const &database, log_id id)
-> nil::dbms::agency::envelope {
    auto path = paths::current()->replicatedLogs()->database(database)->log(to_string(id))->str();

    return envelope.write().remove(path + "/supervision/election").inc(paths::current()->version()->str()).end();
}

auto methods::update_election_result(nil::dbms::agency::envelope envelope, DatabaseID const &database, log_id id,
                                   LogCurrentSupervisionElection const &result) -> nil::dbms::agency::envelope {
    auto path = paths::current()->replicatedLogs()->database(database)->log(to_string(id))->str();

    return envelope.write()
            .emplace_object(path + "/supervision/election",
                            [&](VPackBuilder &builder) { velocypack::serialize(builder, result); })
            .inc(paths::current()->version()->str())
            .end();
}

auto methods::get_current_supervision(TRI_vocbase_t &vocbase, log_id id) -> LogCurrentSupervision {
    auto &agencyCache = vocbase.server().getFeature<ClusterFeature>().agencyCache();
    VPackBuilder builder;
    agencyCache.get(builder,
                    basics::StringUtils::concatT("Current/ReplicatedLogs/", vocbase.name(), "/", id, "/supervision"));
    return velocypack::deserialize<LogCurrentSupervision>(builder.slice());
}

namespace {
    auto create_state_trx(nil::dbms::agency::envelope envelope,
                                  DatabaseID const &database,
                                  state::agency::Target const &spec) -> nil::dbms::agency::envelope {
        auto path = paths::target()->replicatedStates()->database(database)->state(spec.id)->str();

        return envelope.write()
                .emplace_object(path, [&](VPackBuilder &builder) { velocypack::serialize(builder, spec); })
                .inc(paths::target()->version()->str())
                .precs()
                .isEmpty(path)
                .end();
    }
}    // namespace

auto methods::create_replicated_state(DatabaseID const &database, state::agency::Target const &spec)
-> futures::Future <ResultT<uint64_t>> {
    VPackBufferUInt8 trx;
    {
        VPackBuilder builder(trx);
        create_state_trx(nil::dbms::agency::envelope::into_builder(builder), database, spec).done();
    }
    return sendAgencyWriteTransaction(std::move(trx));
}

auto methods::replace_replicated_state_participant(std::string const &databaseName, log_id id,
                                                ParticipantId const &participantToRemove,
                                                ParticipantId const &participantToAdd,
                                                std::optional <ParticipantId> const &currentLeader)
-> futures::Future <Result> {
    auto path = paths::target()->replicatedStates()->database(databaseName)->state(id);

    bool replaceLeader = currentLeader == participantToRemove;
    // note that replaceLeader => currentLeader.has_value()
    TRI_ASSERT(!replaceLeader || currentLeader.has_value());

    VPackBufferUInt8 trx;
    {
        VPackBuilder builder(trx);
        nil::dbms::agency::envelope::into_builder(builder)
                .write()
                        // remove the old participant
                .remove(*path->participants()->server(participantToRemove))
                        // add the new participant
                .set(*path->participants()->server(participantToAdd), VPackSlice::emptyObjectSlice())
                        // if the old participant was the leader, set the leader to the new one
                .cond(replaceLeader,
                      [&](auto &&write) { return std::move(write).set(*path->leader(), participantToAdd); })
                .inc(*paths::target()->version())
                .precs()
                        // assert that the old participant actually was a participant
                .isNotEmpty(*path->participants()->server(participantToRemove))
                        // assert that the new participant didn't exist
                .isEmpty(*path->participants()->server(participantToAdd))
                        // if the old participant was the leader, assert that it
                .cond(replaceLeader,
                      [&](auto &&precs) { return std::move(precs).isEqual(*path->leader(), *currentLeader); })
                .end()
                .done();
    }

    return sendAgencyWriteTransaction(std::move(trx)).thenValue([](ResultT <std::uint64_t> &&resultT) {
        if (resultT.ok() && *resultT == 0) {
            return Result(TRI_ERROR_HTTP_PRECONDITION_FAILED,
                          "Refused to replace participant. Either the to-be-replaced one "
                          "is "
                          "not part of the participants, or the new one already was.");
        }
        return resultT.result();
    });
}

auto methods::replace_replicated_set_leader(std::string const &databaseName, log_id id,
                                         std::optional <ParticipantId> const &leaderId) -> futures::Future <Result> {
    auto path = paths::target()->replicatedStates()->database(databaseName)->state(id);

    VPackBufferUInt8 trx;
    {
        VPackBuilder builder(trx);
        nil::dbms::agency::envelope::into_builder(builder)
                .write()
                .cond(leaderId.has_value(),
                      [&](auto &&write) { return std::move(write).set(*path->leader(), *leaderId); })
                .cond(!leaderId.has_value(), [&](auto &&write) { return std::move(write).remove(*path->leader()); })
                .inc(*paths::target()->version())
                .precs()
                .cond(leaderId.has_value(),
                      [&](auto &&precs) {
                          return std::move(precs).isNotEmpty(*path->participants()->server(*leaderId));
                      })
                .end()
                .done();
    }

    return sendAgencyWriteTransaction(std::move(trx)).thenValue([](ResultT <std::uint64_t> &&resultT) {
        if (resultT.ok() && *resultT == 0) {
            return Result(TRI_ERROR_HTTP_PRECONDITION_FAILED,
                          "Refused to set the new leader: It's not part of the "
                          "participants.");
        }
        return resultT.result();
    });
}
