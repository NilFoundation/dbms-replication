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

#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/dbmsd.hpp>

#include <cstdint>

namespace nil::dbms::replication::log {
    struct ReplicatedLogMetrics;
}

namespace nil::dbms {
    class log_feature final : public DbmsdFeature {
    public:
        constexpr static const char *name() noexcept {
            return "ReplicatedLog";
        }

        explicit log_feature(Server &server);
        ~log_feature() override;

        auto metrics() const noexcept -> std::shared_ptr<replication::log::ReplicatedLogMetrics> const &;
        auto options() const noexcept -> std::shared_ptr<replication::ReplicatedLogGlobalSettings const>;

        void collectOptions(std::shared_ptr<options::ProgramOptions>) override final;
        void prepare() override;

    private:
        std::shared_ptr<replication::log::ReplicatedLogMetrics> _replicatedLogMetrics;
        std::shared_ptr<replication::ReplicatedLogGlobalSettings> _options;
    };

}    // namespace nil::dbms
