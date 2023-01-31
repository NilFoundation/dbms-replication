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

#include <nil/dbms/replication/log/log_metrics_declarations.hpp>
#include <nil/dbms/replication/log/log_metrics.hpp>
#include <nil/dbms/metrics/metrics_feature.hpp>

using namespace nil::dbms::replication::log;

ReplicatedLogMetrics::ReplicatedLogMetrics(metrics::MetricsFeature &metricsFeature) :
    ReplicatedLogMetrics(&metricsFeature) {
}

template<typename Builder, bool mock>
auto ReplicatedLogMetrics::createMetric(metrics::MetricsFeature *metricsFeature)
    -> std::shared_ptr<typename Builder::MetricT> {
    TRI_ASSERT((metricsFeature == nullptr) == mock);
    if constexpr (!mock) {
        return metricsFeature->addShared(Builder {});
    } else {
        return std::dynamic_pointer_cast<typename Builder::MetricT>(Builder {}.build());
    }
}

template<
    typename MFP,
    std::enable_if_t<std::is_same_v<nil::dbms::metrics::MetricsFeature *, MFP> || std::is_null_pointer_v<MFP>, int>,
    bool mock>
ReplicatedLogMetrics::ReplicatedLogMetrics(MFP metricsFeature) :
    replicatedLogNumber(createMetric<dbms_replication_log_number, mock>(metricsFeature)),
    replicatedLogAppendEntriesRttUs(
        createMetric<dbms_replication_log_append_entries_rtt, mock>(metricsFeature)),
    replicatedLogFollowerAppendEntriesRtUs(
        createMetric<dbms_replication_log_follower_append_entries_rt, mock>(metricsFeature)),
    replicatedLogCreationNumber(createMetric<dbms_replication_log_creation_total, mock>(metricsFeature)),
    replicatedLogDeletionNumber(createMetric<dbms_replication_log_deletion_total, mock>(metricsFeature)),
    replicatedlog_leaderNumber(createMetric<dbms_replication_log_leader_number, mock>(metricsFeature)),
    replicatedLogFollowerNumber(createMetric<dbms_replication_log_follower_number, mock>(metricsFeature)),
    replicatedLogInactiveNumber(createMetric<dbms_replication_log_inactive_number, mock>(metricsFeature)),
    replicatedlog_leaderTookOverNumber(
        createMetric<dbms_replication_log_leader_took_over_total, mock>(metricsFeature)),
    replicatedLogStartedFollowingNumber(
        createMetric<dbms_replication_log_started_following_total, mock>(metricsFeature)),
    replicatedLogInsertsBytes(createMetric<dbms_replication_log_inserts_bytes, mock>(metricsFeature)),
    replicatedLogInsertsRtt(createMetric<dbms_replication_log_inserts_rtt, mock>(metricsFeature)) {
#ifndef DBMS_USE_GOOGLE_TESTS
    static_assert(!mock);
    static_assert(!std::is_null_pointer_v<MFP>);
#endif
}

MeasureTimeGuard::MeasureTimeGuard(std::shared_ptr<metrics::Histogram<metrics::LogScale<std::uint64_t>>>
                                       histogram) noexcept :
    _start(std::chrono::steady_clock::now()),
    _histogram(std::move(histogram)) {
}

void MeasureTimeGuard::fire() {
    if (_histogram) {
        auto const endTime = std::chrono::steady_clock::now();
        auto const duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - _start);
        _histogram->count(duration.count());
        _histogram.reset();
    }
}

MeasureTimeGuard::~MeasureTimeGuard() {
    fire();
}

template nil::dbms::replication::log::ReplicatedLogMetrics::ReplicatedLogMetrics(
    nil::dbms::metrics::MetricsFeature *);
#ifdef DBMS_USE_GOOGLE_TESTS
template nil::dbms::replication::log::ReplicatedLogMetrics::ReplicatedLogMetrics(std::nullptr_t);
#endif
