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

#include <nil/replication_sdk/cluster/cluster_types.hpp>
#include <nil/replication_sdk/replicated_log/log_common.hpp>

namespace nil::dbms::replication_sdk::replicated_log {

    struct participant_health {
        RebootId rebootId;
        bool notIsFailed;

        bool operator==(const participant_health &rhs) const {
            return rebootId == rhs.rebootId && notIsFailed == rhs.notIsFailed;
        }
        bool operator!=(const participant_health &rhs) const {
            return !(rhs == *this);
        }
    };

    struct participants_health {
        auto notIsFailed(ParticipantId const &participant) const -> bool {
            if (auto it = _health.find(participant); it != std::end(_health)) {
                return it->second.notIsFailed;
            }
            return false;
        };
        auto validRebootId(ParticipantId const &participant, RebootId rebootId) const -> bool {
            if (auto it = _health.find(participant); it != std::end(_health)) {
                return it->second.rebootId == rebootId;
            }
            return false;
        };
        auto getRebootId(ParticipantId const &participant) const -> std::optional<RebootId> {
            if (auto it = _health.find(participant); it != std::end(_health)) {
                return it->second.rebootId;
            }
            return std::nullopt;
        }
        auto contains(ParticipantId const &participant) const -> bool {
            return _health.find(participant) != _health.end();
        }

        bool operator==(const participants_health &rhs) const {
            return _health == rhs._health;
        }
        bool operator!=(const participants_health &rhs) const {
            return !(rhs == *this);
        }

        std::unordered_map<ParticipantId, participant_health> _health;
    };

}    // namespace nil::dbms::replication_sdk::replicated_log
