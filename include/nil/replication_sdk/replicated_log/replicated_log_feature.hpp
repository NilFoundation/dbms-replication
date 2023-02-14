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

#include <nil/replication_sdk/replicated_log/log_common.hpp>
#include <nil/replication_sdk/dbmsd/dbmsd.hpp>

#include <cstdint>

namespace nil::dbms::replication_sdk::replicated_log {
    struct replicated_log_metrics;
}

namespace nil::dbms {
    class replicated_log_feature final : public DbmsdFeature {
    public:
        constexpr static const char *name() noexcept {
            return "ReplicatedLog";
        }

        explicit replicated_log_feature(Server &server);
        ~replicated_log_feature() override;

        auto metrics() const noexcept -> std::shared_ptr<replication_sdk::replicated_log::replicated_log_metrics> const &;
        auto options() const noexcept -> std::shared_ptr<replication_sdk::replicated_log_global_settings const>;

        void collectOptions(std::shared_ptr<options::ProgramOptions>) override final;
        void prepare() override;

    private:
        std::shared_ptr<replication_sdk::replicated_log::replicated_log_metrics> _replicatedLogMetrics;
        std::shared_ptr<replication_sdk::replicated_log_global_settings> _options;
    };

}    // namespace nil::dbms
