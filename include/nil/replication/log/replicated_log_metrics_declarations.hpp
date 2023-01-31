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

#include <nil/dbms/metrics/counter_builder.hpp>
#include <nil/dbms/metrics/gauge_builder.hpp>
#include <nil/dbms/metrics/histogram_builder.hpp>
#include <nil/dbms/metrics/log_scale.hpp>

#include <cstdint>

namespace nil::dbms {

    struct append_entries_rtt_scale {
        using scale_t = metrics::LogScale<std::uint64_t>;
        static scale_t scale() {
            // values in us, smallest bucket is up to 1ms, scales up to 2^16ms =~ 65s.
            return {scale_t::kSupplySmallestBucket, 2, 0, 1'000, 16};
        }
    };

    struct InsertBytesScale {
        using scale_t = metrics::LogScale<std::uint64_t>;
        static scale_t scale() {
            // 1 byte up to 16GiB (1 * 4^17 = 16 * 2^30).
            return {scale_t::kSupplySmallestBucket, 4, 0, 1, 17};
        }
    };

    DECLARE_GAUGE(dbms_replication_log_number, std::uint64_t,
                  "Number of replicated logs on this dbmsd instance");

    DECLARE_HISTOGRAM(dbms_replication_log_append_entries_rtt, append_entries_rtt_scale,
                      "RTT for AppendEntries requests [us]");
    DECLARE_HISTOGRAM(dbms_replication_log_follower_append_entries_rt, append_entries_rtt_scale,
                      "RT for AppendEntries call [us]");

    DECLARE_COUNTER(dbms_replication_log_creation_total,
                    "Number of replicated logs created since server start");

    DECLARE_COUNTER(dbms_replication_log_deletion_total,
                    "Number of replicated logs deleted since server start");

    DECLARE_GAUGE(dbms_replication_log_leader_number, std::uint64_t,
                  "Number of replicated logs this server has, and is currently a leader of");

    DECLARE_GAUGE(dbms_replication_log_follower_number,
                  std::uint64_t,
                  "Number of replicated logs this server has, and is currently a "
                  "follower of");

    DECLARE_GAUGE(dbms_replication_log_inactive_number,
                  std::uint64_t,
                  "Number of replicated logs this server has, and is currently "
                  "neither leader nor follower of");

    DECLARE_COUNTER(dbms_replication_log_leader_took_over_total,
                    "Number of times a replicated log on this server took over as "
                    "leader in a term");

    DECLARE_COUNTER(dbms_replication_log_started_following_total,
                    "Number of times a replicated log on this server started "
                    "following a leader in a term");

    DECLARE_HISTOGRAM(dbms_replication_log_inserts_bytes,
                      InsertBytesScale,
                      "Number of bytes per insert in replicated log leader "
                      "instances on this server [bytes]");

    DECLARE_HISTOGRAM(dbms_replication_log_inserts_rtt, append_entries_rtt_scale,
                      "Histogram of round-trip times of replicated log inserts [us]");

}    // namespace nil::dbms
