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
#include <nil/replication_sdk/replicated_log/log_status.hpp>
#include <nil/replication_sdk/replicated_log/agency_log_specification.hpp>
#include <nil/replication_sdk/replicated_log/agency_specification_inspectors.hpp>
#include <basics/exceptions.h>
#include <basics/static_strings.h>
#include <basics/application_exit.h>
#include <basics/debugging.h>
#include <basics/overload.h>
#include <logger/LogMacros.h>
#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>

using namespace nil::dbms::replication_sdk;
using namespace nil::dbms::replication_sdk::replicated_log;

auto quick_log_status::getCurrentTerm() const noexcept -> std::optional<log_term> {
    if (role == ParticipantRole::kUnconfigured) {
        return std::nullopt;
    }
    return term;
}

auto quick_log_status::getLocalStatistics() const noexcept -> std::optional<log_statistics> {
    if (role == ParticipantRole::kUnconfigured) {
        return std::nullopt;
    }
    return local;
}

auto replicated_log::operator==(follower_statistics const &left, follower_statistics const &right) noexcept -> bool {
    return left.lastErrorReason == right.lastErrorReason && left.lastRequestLatencyMS == right.lastRequestLatencyMS &&
           left.internalState.value.index() == right.internalState.value.index();
}

auto replicated_log::operator!=(follower_statistics const &left, follower_statistics const &right) noexcept -> bool {
    return !(left == right);
}

auto log_status::getCurrentTerm() const noexcept -> std::optional<log_term> {
    return std::visit(overload {
                          [&](replicated_log::UnconfiguredStatus) -> std::optional<log_term> { return std::nullopt; },
                          [&](replicated_log::leader_status const &s) -> std::optional<log_term> { return s.term; },
                          [&](replicated_log::follower_status const &s) -> std::optional<log_term> { return s.term; },
                      },
                      _variant);
}

auto log_status::getLocalStatistics() const noexcept -> std::optional<log_statistics> {
    return std::visit(
        overload {
            [&](replicated_log::UnconfiguredStatus const &s) -> std::optional<log_statistics> { return std::nullopt; },
            [&](replicated_log::leader_status const &s) -> std::optional<log_statistics> { return s.local; },
            [&](replicated_log::follower_status const &s) -> std::optional<log_statistics> { return s.local; },
        },
        _variant);
}

auto log_status::fromVelocyPack(VPackSlice slice) -> log_status {
    auto role = slice.get("role");
    if (role.isEqualString(StaticStrings::Leader)) {
        return log_status {velocypack::deserialize<leader_status>(slice)};
    } else if (role.isEqualString(StaticStrings::Follower)) {
        return log_status {velocypack::deserialize<follower_status>(slice)};
    } else {
        return log_status {velocypack::deserialize<UnconfiguredStatus>(slice)};
    }
}

log_status::log_status(UnconfiguredStatus status) noexcept : _variant(status) {
}
log_status::log_status(leader_status status) noexcept : _variant(std::move(status)) {
}
log_status::log_status(follower_status status) noexcept : _variant(std::move(status)) {
}

auto log_status::getVariant() const noexcept -> VariantType const & {
    return _variant;
}

auto log_status::toVelocyPack(velocypack::Builder &builder) const -> void {
    std::visit([&](auto const &s) { velocypack::serialize(builder, s); }, _variant);
}

auto log_status::asLeaderStatus() const noexcept -> leader_status const * {
    return std::get_if<leader_status>(&_variant);
}
auto log_status::asFollowerStatus() const noexcept -> follower_status const * {
    return std::get_if<follower_status>(&_variant);
}

namespace {
    constexpr static const char *kSupervision = "supervision";
    constexpr static const char *kLeaderId = "leaderId";
}    // namespace

auto global_status::toVelocyPack(velocypack::Builder &builder) const -> void {
    VPackObjectBuilder ob(&builder);
    builder.add(VPackValue(kSupervision));
    supervision.toVelocyPack(builder);
    {
        VPackObjectBuilder ob2(&builder, StaticStrings::Participants);
        for (auto const &[id, status] : participants) {
            builder.add(VPackValue(id));
            status.toVelocyPack(builder);
        }
    }
    builder.add(VPackValue("specification"));
    specification.toVelocyPack(builder);
    if (leaderId.has_value()) {
        builder.add(kLeaderId, VPackValue(*leaderId));
    }
}

auto global_status::fromVelocyPack(VPackSlice slice) -> global_status {
    global_status status;
    auto sup = slice.get(kSupervision);
    TRI_ASSERT(!sup.isNone()) << "expected " << kSupervision << " key in GlobalStatus";
    status.supervision = SupervisionStatus::fromVelocyPack(sup);
    status.specification = Specification::fromVelocyPack(slice.get("specification"));
    for (auto [key, value] : VPackObjectIterator(slice.get(StaticStrings::Participants))) {
        auto id = ParticipantId {key.copyString()};
        auto stat = ParticipantStatus::fromVelocyPack(value);
        status.participants.emplace(std::move(id), stat);
    }
    if (auto leaderId = slice.get(kLeaderId); !leaderId.isNone()) {
        status.leaderId = leaderId.copyString();
    }
    return status;
}

void global_status_connection::toVelocyPack(nil::dbms::velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(StaticStrings::ErrorCode, VPackValue(error));
    if (!errorMessage.empty()) {
        b.add(StaticStrings::ErrorMessage, VPackValue(errorMessage));
    }
}

auto global_status_connection::fromVelocyPack(nil::dbms::velocypack::Slice slice) -> global_status_connection {
    auto code = ErrorCode(slice.get(StaticStrings::ErrorCode).extract<int>());
    auto message = std::string {};
    if (auto ms = slice.get(StaticStrings::ErrorMessage); !ms.isNone()) {
        message = ms.copyString();
    }
    return global_status_connection {.error = code, .errorMessage = message};
}

void global_status::ParticipantStatus::Response::toVelocyPack(nil::dbms::velocypack::Builder &b) const {
    if (std::holds_alternative<log_status>(value)) {
        std::get<log_status>(value).toVelocyPack(b);
    } else {
        TRI_ASSERT(std::holds_alternative<velocypack::UInt8Buffer>(value));
        auto slice = VPackSlice(std::get<velocypack::UInt8Buffer>(value).data());
        b.add(slice);
    }
}

auto global_status::ParticipantStatus::Response::fromVelocyPack(nil::dbms::velocypack::Slice s)
    -> global_status::ParticipantStatus::Response {
    if (s.hasKey("role")) {
        return Response {.value = log_status::fromVelocyPack(s)};
    } else {
        auto buffer = velocypack::UInt8Buffer(s.byteSize());
        buffer.append(s.start(), s.byteSize());
        return Response {.value = buffer};
    }
}

void global_status::ParticipantStatus::toVelocyPack(nil::dbms::velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(VPackValue("connection"));
    connection.toVelocyPack(b);
    if (response.has_value()) {
        b.add(VPackValue("response"));
        response->toVelocyPack(b);
    }
}

auto global_status::ParticipantStatus::fromVelocyPack(nil::dbms::velocypack::Slice s)
    -> global_status::ParticipantStatus {
    auto connection = global_status_connection::fromVelocyPack(s.get("connection"));
    auto response = std::optional<Response> {};
    if (auto rs = s.get("response"); !rs.isNone()) {
        response = Response::fromVelocyPack(rs);
    }
    return ParticipantStatus {.connection = std::move(connection), .response = std::move(response)};
}

void global_status::SupervisionStatus::toVelocyPack(nil::dbms::velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(VPackValue("connection"));
    connection.toVelocyPack(b);
    if (response.has_value()) {
        b.add(VPackValue("response"));
        velocypack::serialize(b, response);
    }
}

auto global_status::SupervisionStatus::fromVelocyPack(nil::dbms::velocypack::Slice s)
    -> global_status::SupervisionStatus {
    auto connection = global_status_connection::fromVelocyPack(s.get("connection"));
    auto const response = std::invoke([&s]() -> std::optional<agency::log_current_supervision> {
        if (auto rs = s.get("response"); !rs.isNone()) {
            return velocypack::deserialize<agency::log_current_supervision>(rs);
        }
        return std::nullopt;
    });
    return SupervisionStatus {.connection = std::move(connection), .response = std::move(response)};
}

auto replicated_log::to_string(global_status::SpecificationSource source) -> std::string_view {
    switch (source) {
        case global_status::SpecificationSource::kLocalCache:
            return "LocalCache";
        case global_status::SpecificationSource::kRemoteAgency:
            return "RemoteAgency";
        default:
            return "(unknown)";
    }
}

auto replicated_log::to_string(ParticipantRole role) noexcept -> std::string_view {
    switch (role) {
        case ParticipantRole::kUnconfigured:
            return "Unconfigured";
        case ParticipantRole::kLeader:
            return "Leader";
        case ParticipantRole::kFollower:
            return "Follower";
    }
    LOG_TOPIC("e3242", ERR, Logger::REPLICATION2)
        << "Unhandled participant role: " << static_cast<std::underlying_type_t<decltype(role)>>(role);
    TRI_ASSERT(false);
    return "(unknown status code)";
}

void global_status::Specification::toVelocyPack(nil::dbms::velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(VPackValue("plan"));
    velocypack::serialize(b, plan);
    b.add("source", VPackValue(to_string(source)));
}

auto global_status::Specification::fromVelocyPack(nil::dbms::velocypack::Slice s) -> Specification {
    auto plan = velocypack::deserialize<agency::log_plan_specification>(s.get("plan"));
    auto source = global_status::SpecificationSource::kLocalCache;
    if (s.get("source").isEqualString("RemoteAgency")) {
        source = global_status::SpecificationSource::kRemoteAgency;
    }
    return Specification {.source = source, .plan = std::move(plan)};
}

auto participant_role_string_transformer::toSerialized(ParticipantRole source, std::string &target) const
    -> nil::dbms::inspection::Status {
    target = to_string(source);
    return {};
}

auto participant_role_string_transformer::fromSerialized(std::string const &source, ParticipantRole &target) const
    -> nil::dbms::inspection::Status {
    if (source == "Unconfigured") {
        target = ParticipantRole::kUnconfigured;
    } else if (source == "Leader") {
        target = ParticipantRole::kLeader;
    } else if (source == "Follower") {
        target = ParticipantRole::kFollower;
    } else {
        return inspection::Status {"Invalid participant role name: " + std::string {source}};
    }
    return {};
}
