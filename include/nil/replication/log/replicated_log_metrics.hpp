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

#include <nil/dbms/metrics/fwd.hpp>

#include <chrono>
#include <cstdint>
#include <memory>

namespace nil::dbms::replication::log {

    struct ReplicatedLogMetrics {
        explicit ReplicatedLogMetrics(metrics::MetricsFeature &metricsFeature);

    private:
        template<typename Builder, bool mock = false>
        static auto createMetric(metrics::MetricsFeature *metricsFeature) -> std::shared_ptr<typename Builder::MetricT>;

    protected:
        template<
            typename MFP,
            std::enable_if_t<std::is_same_v<metrics::MetricsFeature *, MFP> || std::is_null_pointer_v<MFP>, int> = 0,
            bool mock = std::is_null_pointer_v<MFP>>
        explicit ReplicatedLogMetrics(MFP metricsFeature);

    public:
        std::shared_ptr<metrics::Gauge<uint64_t>> const replicatedLogNumber;
        std::shared_ptr<metrics::Histogram<metrics::LogScale<std::uint64_t>>> const replicatedLogAppendEntriesRttUs;
        std::shared_ptr<metrics::Histogram<metrics::LogScale<std::uint64_t>>> const
            replicatedLogFollowerAppendEntriesRtUs;
        std::shared_ptr<metrics::Counter> const replicatedLogCreationNumber;
        std::shared_ptr<metrics::Counter> const replicatedLogDeletionNumber;
        std::shared_ptr<metrics::Gauge<std::uint64_t>> const replicatedlog_leaderNumber;
        std::shared_ptr<metrics::Gauge<std::uint64_t>> const replicatedLogFollowerNumber;
        std::shared_ptr<metrics::Gauge<std::uint64_t>> const replicatedLogInactiveNumber;
        std::shared_ptr<metrics::Counter> const replicatedlog_leaderTookOverNumber;
        std::shared_ptr<metrics::Counter> const replicatedLogStartedFollowingNumber;
        std::shared_ptr<metrics::Histogram<metrics::LogScale<std::uint64_t>>> const replicatedLogInsertsBytes;
        std::shared_ptr<metrics::Histogram<metrics::LogScale<std::uint64_t>>> const replicatedLogInsertsRtt;
    };

    struct MeasureTimeGuard {
        explicit MeasureTimeGuard(std::shared_ptr<metrics::Histogram<metrics::LogScale<std::uint64_t>>>
                                      histogram) noexcept;
        MeasureTimeGuard(MeasureTimeGuard const &) = delete;
        MeasureTimeGuard(MeasureTimeGuard &&) = default;
        ~MeasureTimeGuard();

        void fire();

    private:
        std::chrono::steady_clock::time_point const _start;
        std::shared_ptr<metrics::Histogram<metrics::LogScale<std::uint64_t>>> _histogram;
    };

}    // namespace nil::dbms::replication::log
