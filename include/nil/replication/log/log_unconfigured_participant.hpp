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

#include <nil/dbms/replication/log/ilog_interfaces.hpp>
#include <nil/dbms/replication/log/log_metrics.hpp>
#include <nil/dbms/replication/log/wait_for_bag.hpp>

#include <basics/guarded.h>

namespace nil::dbms::replication::log {

    /**
     * @brief Unconfigured log participant, i.e. currently neither a leader nor
     * follower. Holds a log_core, does nothing else.
     */
    struct LogUnconfiguredParticipant final : std::enable_shared_from_this<LogUnconfiguredParticipant>,
                                              nil::dbms::replication::log::ILogParticipant {
        ~LogUnconfiguredParticipant() override;
        explicit LogUnconfiguredParticipant(std::unique_ptr<nil::dbms::replication::log::log_core> log_core,
                                            std::shared_ptr<
                                                nil::dbms::replication::log::ReplicatedLogMetrics>
                                                logMetrics);

        [[nodiscard]] auto get_status() const -> nil::dbms::replication::log::log_status override;
        [[nodiscard]] auto getQuickStatus() const -> nil::dbms::replication::log::quick_log_status override;
        [[nodiscard]] auto resign() && -> std::tuple<std::unique_ptr<nil::dbms::replication::log::log_core>,
                                                     nil::dbms::deferred_action> override;
        [[nodiscard]] auto waitFor(nil::dbms::replication::log_index) -> WaitForFuture override;
        [[nodiscard]] auto release(nil::dbms::replication::log_index doneWithIdx) -> nil::dbms::Result override;
        [[nodiscard]] auto waitForIterator(nil::dbms::replication::log_index index) -> WaitForIteratorFuture override;
        [[nodiscard]] auto waitForResign() -> futures::Future<futures::Unit> override;
        [[nodiscard]] auto getCommitIndex() const noexcept -> nil::dbms::replication::log_index override;

        [[nodiscard]] auto copyin_memory_log() const -> in_memory_log override;

    private:
        std::shared_ptr<nil::dbms::replication::log::ReplicatedLogMetrics> const _logMetrics;

        struct guarded_data {
            explicit guarded_data(std::unique_ptr<nil::dbms::replication::log::log_core> log_core);

            [[nodiscard]] auto
                resign() && -> std::tuple<std::unique_ptr<nil::dbms::replication::log::log_core>,
                                          nil::dbms::deferred_action>;

            [[nodiscard]] auto didResign() const noexcept -> bool;

            [[nodiscard]] auto waitForResign() -> std::pair<futures::Future<futures::Unit>, deferred_action>;

            std::unique_ptr<nil::dbms::replication::log::log_core> _log_core;
            WaitForBag _waitForResignQueue;
        };

        nil::dbms::Guarded<guarded_data> _guarded_data;
    };

}    // namespace nil::dbms::replication::log
