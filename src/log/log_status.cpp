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

#include "inspection/vpack.h"
#include <nil/dbms/replication/log/log_status.hpp>
#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/log/agency_specification_inspectors.hpp>
#include <basics/exceptions.h>
#include <basics/static_strings.h>
#include <basics/application_exit.h>
#include <basics/debugging.h>
#include <basics/overload.h>
#include <logger/LogMacros.h>
#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>

using namespace nil::dbms::replication;
using namespace nil::dbms::replication::log;

auto quick_log_status::get_current_term() const noexcept -> std::optional<log_term> {
    if (role == participant_role::kUnconfigured) {
        return std::nullopt;
    }
    return term;
}

auto quick_log_status::get_local_statistics() const noexcept -> std::optional<LogStatistics> {
    if (role == participant_role::kUnconfigured) {
        return std::nullopt;
    }
    return local;
}

auto log::operator==(FollowerStatistics const &left, FollowerStatistics const &right) noexcept -> bool {
    return left.lastErrorReason == right.lastErrorReason && left.lastRequestLatencyMS == right.lastRequestLatencyMS &&
           left.internalState.value.index() == right.internalState.value.index();
}

auto log::operator!=(FollowerStatistics const &left, FollowerStatistics const &right) noexcept -> bool {
    return !(left == right);
}

auto log_status::get_current_term() const noexcept -> std::optional<log_term> {
    return std::visit(overload {
                          [&](log::UnconfiguredStatus) -> std::optional<log_term> { return std::nullopt; },
                          [&](log::LeaderStatus const &s) -> std::optional<log_term> { return s.term; },
                          [&](log::FollowerStatus const &s) -> std::optional<log_term> { return s.term; },
                      },
                      _variant);
}

auto log_status::get_local_statistics() const noexcept -> std::optional<LogStatistics> {
    return std::visit(
        overload {
            [&](log::UnconfiguredStatus const &s) -> std::optional<LogStatistics> { return std::nullopt; },
            [&](log::LeaderStatus const &s) -> std::optional<LogStatistics> { return s.local; },
            [&](log::FollowerStatus const &s) -> std::optional<LogStatistics> { return s.local; },
        },
        _variant);
}

auto log_status::from_velocy_pack(VPackSlice slice) -> log_status {
    auto role = slice.get("role");
    if (role.isEqualString(StaticStrings::Leader)) {
        return log_status {velocypack::deserialize<LeaderStatus>(slice)};
    } else if (role.isEqualString(StaticStrings::Follower)) {
        return log_status {velocypack::deserialize<FollowerStatus>(slice)};
    } else {
        return log_status {velocypack::deserialize<UnconfiguredStatus>(slice)};
    }
}

log_status::log_status(UnconfiguredStatus status) noexcept : _variant(status) {
}
log_status::log_status(LeaderStatus status) noexcept : _variant(std::move(status)) {
}
log_status::log_status(FollowerStatus status) noexcept : _variant(std::move(status)) {
}

auto log_status::getVariant() const noexcept -> VariantType const & {
    return _variant;
}

auto log_status::to_velocy_pack(velocypack::Builder &builder) const -> void {
    std::visit([&](auto const &s) { velocypack::serialize(builder, s); }, _variant);
}

auto log_status::asLeaderStatus() const noexcept -> LeaderStatus const * {
    return std::get_if<LeaderStatus>(&_variant);
}
auto log_status::asFollowerStatus() const noexcept -> FollowerStatus const * {
    return std::get_if<FollowerStatus>(&_variant);
}

namespace {
    constexpr static const char *kSupervision = "supervision";
    constexpr static const char *kLeaderId = "leaderId";
}    // namespace

auto global_status::to_velocy_pack(velocypack::Builder &builder) const -> void {
    VPackObjectBuilder ob(&builder);
    builder.add(VPackValue(kSupervision));
    supervision.to_velocy_pack(builder);
    {
        VPackObjectBuilder ob2(&builder, StaticStrings::Participants);
        for (auto const &[id, status] : participants) {
            builder.add(VPackValue(id));
            status.to_velocy_pack(builder);
        }
    }
    builder.add(VPackValue("specification"));
    specification.to_velocy_pack(builder);
    if (leaderId.has_value()) {
        builder.add(kLeaderId, VPackValue(*leaderId));
    }
}

auto global_status::from_velocy_pack(VPackSlice slice) -> global_status {
    global_status status;
    auto sup = slice.get(kSupervision);
    TRI_ASSERT(!sup.isNone()) << "expected " << kSupervision << " key in global_status";
    status.supervision = SupervisionStatus::from_velocy_pack(sup);
    status.specification = Specification::from_velocy_pack(slice.get("specification"));
    for (auto [key, value] : VPackObjectIterator(slice.get(StaticStrings::Participants))) {
        auto id = ParticipantId {key.copyString()};
        auto stat = participant_status::from_velocy_pack(value);
        status.participants.emplace(std::move(id), stat);
    }
    if (auto leaderId = slice.get(kLeaderId); !leaderId.isNone()) {
        status.leaderId = leaderId.copyString();
    }
    return status;
}

void global_status_connection::to_velocy_pack(nil::dbms::velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(StaticStrings::ErrorCode, VPackValue(error));
    if (!errorMessage.empty()) {
        b.add(StaticStrings::ErrorMessage, VPackValue(errorMessage));
    }
}

auto global_status_connection::from_velocy_pack(nil::dbms::velocypack::Slice slice) -> global_status_connection {
    auto code = ErrorCode(slice.get(StaticStrings::ErrorCode).extract<int>());
    auto message = std::string {};
    if (auto ms = slice.get(StaticStrings::ErrorMessage); !ms.isNone()) {
        message = ms.copyString();
    }
    return global_status_connection {.error = code, .errorMessage = message};
}

void global_status::participant_status::Response::to_velocy_pack(nil::dbms::velocypack::Builder &b) const {
    if (std::holds_alternative<log_status>(value)) {
        std::get<log_status>(value).to_velocy_pack(b);
    } else {
        TRI_ASSERT(std::holds_alternative<velocypack::UInt8Buffer>(value));
        auto slice = VPackSlice(std::get<velocypack::UInt8Buffer>(value).data());
        b.add(slice);
    }
}

auto global_status::participant_status::Response::from_velocy_pack(nil::dbms::velocypack::Slice s)
    -> global_status::participant_status::Response {
    if (s.hasKey("role")) {
        return Response {.value = log_status::from_velocy_pack(s)};
    } else {
        auto buffer = velocypack::UInt8Buffer(s.byte_size());
        buffer.append(s.start(), s.byte_size());
        return Response {.value = buffer};
    }
}

void global_status::participant_status::to_velocy_pack(nil::dbms::velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(VPackValue("connection"));
    connection.to_velocy_pack(b);
    if (response.has_value()) {
        b.add(VPackValue("response"));
        response->to_velocy_pack(b);
    }
}

auto global_status::participant_status::from_velocy_pack(nil::dbms::velocypack::Slice s)
    -> global_status::participant_status {
    auto connection = global_status_connection::from_velocy_pack(s.get("connection"));
    auto response = std::optional<Response> {};
    if (auto rs = s.get("response"); !rs.isNone()) {
        response = Response::from_velocy_pack(rs);
    }
    return participant_status {.connection = std::move(connection), .response = std::move(response)};
}

void global_status::SupervisionStatus::to_velocy_pack(nil::dbms::velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(VPackValue("connection"));
    connection.to_velocy_pack(b);
    if (response.has_value()) {
        b.add(VPackValue("response"));
        velocypack::serialize(b, response);
    }
}

auto global_status::SupervisionStatus::from_velocy_pack(nil::dbms::velocypack::Slice s)
    -> global_status::SupervisionStatus {
    auto connection = global_status_connection::from_velocy_pack(s.get("connection"));
    auto const response = std::invoke([&s]() -> std::optional<agency::LogCurrentSupervision> {
        if (auto rs = s.get("response"); !rs.isNone()) {
            return velocypack::deserialize<agency::LogCurrentSupervision>(rs);
        }
        return std::nullopt;
    });
    return SupervisionStatus {.connection = std::move(connection), .response = std::move(response)};
}

auto log::to_string(global_status::SpecificationSource source) -> std::string_view {
    switch (source) {
        case global_status::SpecificationSource::kLocalCache:
            return "LocalCache";
        case global_status::SpecificationSource::kRemoteAgency:
            return "RemoteAgency";
        default:
            return "(unknown)";
    }
}

auto log::to_string(participant_role role) noexcept -> std::string_view {
    switch (role) {
        case participant_role::kUnconfigured:
            return "Unconfigured";
        case participant_role::kLeader:
            return "Leader";
        case participant_role::kFollower:
            return "Follower";
    }
    LOG_TOPIC("e3242", ERR, Logger::replication)
        << "Unhandled participant role: " << static_cast<std::underlying_type_t<decltype(role)>>(role);
    TRI_ASSERT(false);
    return "(unknown status code)";
}

void global_status::Specification::to_velocy_pack(nil::dbms::velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(VPackValue("plan"));
    velocypack::serialize(b, plan);
    b.add("source", VPackValue(to_string(source)));
}

auto global_status::Specification::from_velocy_pack(nil::dbms::velocypack::Slice s) -> Specification {
    auto plan = velocypack::deserialize<agency::LogPlanSpecification>(s.get("plan"));
    auto source = global_status::SpecificationSource::kLocalCache;
    if (s.get("source").isEqualString("RemoteAgency")) {
        source = global_status::SpecificationSource::kRemoteAgency;
    }
    return Specification {.source = source, .plan = std::move(plan)};
}

auto participant_roleStringTransformer::toSerialized(participant_role source, std::string &target) const
    -> nil::dbms::inspection::Status {
    target = to_string(source);
    return {};
}

auto participant_roleStringTransformer::fromSerialized(std::string const &source, participant_role &target) const
    -> nil::dbms::inspection::Status {
    if (source == "Unconfigured") {
        target = participant_role::kUnconfigured;
    } else if (source == "Leader") {
        target = participant_role::kLeader;
    } else if (source == "Follower") {
        target = participant_role::kFollower;
    } else {
        return inspection::Status {"Invalid participant role name: " + std::string {source}};
    }
    return {};
}
