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

#include <features/ApplicationServer.h>
#include "basics/application_exit.h"
#include "program_options/Parameters.h"
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/log_metrics.hpp>
#include <nil/dbms/replication/log/log_feature.hpp>
#include <nil/dbms/metrics/metrics_feature.hpp>
#include "logger/LogMacros.h"
#include <nil/dbms/cluster/server_state.hpp>

#include <memory>

using namespace nil::dbms::options;
using namespace nil::dbms;
using namespace nil::dbms::application_features;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::log;

log_feature::log_feature(Server &server) :
    DbmsdFeature {server, *this},
    _replicatedLogMetrics(std::make_shared<ReplicatedLogMetrics>(server.getFeature<metrics::MetricsFeature>())),
    _options(std::make_shared<ReplicatedLogGlobalSettings>()) {
    static_assert(Server::isCreatedAfter<log_feature, metrics::MetricsFeature>());

    setOptional(true);
    startsAfter<CommunicationFeaturePhase>();
    startsAfter<DatabaseFeaturePhase>();
}

auto log_feature::metrics() const noexcept
    -> std::shared_ptr<replication::log::ReplicatedLogMetrics> const & {
    return _replicatedLogMetrics;
}

auto log_feature::options() const noexcept
    -> std::shared_ptr<replication::ReplicatedLogGlobalSettings const> {
    return _options;
}

void log_feature::prepare() {
    if (ServerState::instance()->isCoordinator() || ServerState::instance()->isAgent()) {
        setEnabled(false);
        return;
    }
}

void log_feature::collectOptions(std::shared_ptr<ProgramOptions> options) {
#if defined(DBMS_ENABLE_MAINTAINER_MODE)
    options->addSection("replicatedlog", "Options for replicated logs");

    options->addOption("--replicatedlog.threshold-network-batch-size",
                       "send a batch of log updates early when threshold "
                       "(in bytes) is exceeded",
                       new SizeTParameter(&_options->_thresholdNetworkBatchSize, /*base*/ 1, /*minValue*/
                                          ReplicatedLogGlobalSettings::minThresholdNetworkBatchSize));
    options->addOption("--replicatedlog.threshold-rocksdb-write-batch-size",
                       "write a batch of log updates to RocksDB early "
                       "when threshold (in bytes) is exceeded",
                       new SizeTParameter(&_options->_thresholdRocksDBWriteBatchSize, /*base*/ 1, /*minValue*/
                                          ReplicatedLogGlobalSettings::minThresholdRocksDBWriteBatchSize));
#endif
}

log_feature::~log_feature() = default;
