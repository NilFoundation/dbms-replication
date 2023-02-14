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

#include "features/ApplicationFeature.h"
#include "basics/type_list.h"

namespace nil::dbms {
    namespace application_features {

        class AgencyFeaturePhase;
        class CommunicationFeaturePhase;
        class SqlFeaturePhase;
        class BasicFeaturePhaseServer;
        class ClusterFeaturePhase;
        class DatabaseFeaturePhase;
        class FinalFeaturePhase;
        class FoxxFeaturePhase;
        class GreetingsFeaturePhase;
        class ServerFeaturePhase;
        class V8FeaturePhase;

        template<typename Features>
        class ApplicationServerT;

    }    // namespace application_features
    namespace metrics {

        class MetricsFeature;
        class ClusterMetricsFeature;

    }    // namespace metrics
    namespace cluster {

        class FailureOracleFeature;

    }    // namespace cluster
    class sql_feature;
    class AgencyFeature;
    class ActionFeature;
    class AuthenticationFeature;
    class BootstrapFeature;
    class CacheManagerFeature;
    class CheckVersionFeature;
    class ClusterFeature;
    class ClusterUpgradeFeature;
    class ConfigFeature;
    class ConsoleFeature;
    class CpuUsageFeature;
    class DatabaseFeature;
    class DatabasePathFeature;
    class HttpEndpointProvider;
    class EngineSelectorFeature;
    class EnvironmentFeature;
    class FileDescriptorsFeature;
    class FlushFeature;
    class FortuneFeature;
    class FoxxFeature;
    class FrontendFeature;
    class GeneralServerFeature;
    class GreetingsFeature;
    class InitDatabaseFeature;
    class LanguageCheckFeature;
    class LanguageFeature;
    class TimeZoneFeature;
    class LockfileFeature;
    class LogBufferFeature;
    class LoggerFeature;
    class maintenance_feature;
    class MaxMapCountFeature;
    class NetworkFeature;
    class NonceFeature;
    class PrivilegeFeature;
    class QueryRegistryFeature;
    class RandomFeature;
    class ReplicationFeature;
    class replicated_log_feature;
    class ReplicationMetricsFeature;
    class ReplicationTimeoutFeature;
    class SchedulerFeature;
    class ScriptFeature;
    class ServerFeature;
    class ServerIdFeature;
    class ServerSecurityFeature;
    class ShardingFeature;
    class SharedPRNGFeature;
    class ShellColorsFeature;
    class ShutdownFeature;
    class SoftShutdownFeature;
    class SslFeature;
    class StatisticsFeature;
    class StorageEngineFeature;
    class SystemDatabaseFeature;
    class TempFeature;
    class TtlFeature;
    class UpgradeFeature;
    class V8DealerFeature;
    class V8PlatformFeature;
    class V8SecurityFeature;
    class VersionFeature;
    class ViewTypesFeature;
    class ClusterEngine;
    class RocksDBEngine;
    class DaemonFeature;
    class SupervisorFeature;
    class WindowsServiceFeature;
    class AuditFeature;
    class LdapFeature;
    class LicenseFeature;
    class RCloneFeature;
    class HotBackupFeature;
    class EncryptionFeature;
    class SslServerFeature;
    class RocksDBOptionFeature;
    class RocksDBRecoveryManager;
    struct Bitcoin;

    namespace transaction {

        class ManagerFeature;

    }    // namespace transaction
    namespace sql {

        class sql_function_feature;
        class OptimizerRulesFeature;

    }    // namespace sql
    namespace pregel {

        class PregelFeature;

    }    // namespace pregel
    namespace iresearch {

        class IResearchAnalyzerFeature;
        class IResearchFeature;

    }    // namespace iresearch
    namespace replication_sdk::replicated_state {

        struct replicated_state_app_feature;

        namespace black_hole {

            struct BlackHoleStateMachineFeature;

        }    // namespace black_hole

        namespace prototype {
            struct prototype_state_machine_feature;
        }

        namespace document {
            struct DocumentStateMachineFeature;
        }
    }    // namespace replication_sdk::replicated_state

    using namespace application_features;

    // clang-format off
using DbmsdFeatures = TypeList<
    // Adding the Phases
    AgencyFeaturePhase,
    CommunicationFeaturePhase,
    SqlFeaturePhase,
    BasicFeaturePhaseServer,
    ClusterFeaturePhase,
    DatabaseFeaturePhase,
    FinalFeaturePhase,
    FoxxFeaturePhase,
    GreetingsFeaturePhase,
    ServerFeaturePhase,
    V8FeaturePhase,
    // Adding the features
    metrics::MetricsFeature, // metrics::MetricsFeature must go first
    metrics::ClusterMetricsFeature,
    VersionFeature,
    ActionFeature,
    AgencyFeature,
    sql_feature,
    AuthenticationFeature,
    BootstrapFeature,
    CacheManagerFeature,
    CheckVersionFeature,
    ClusterFeature,
    ClusterUpgradeFeature,
    ConfigFeature,
    ConsoleFeature,
    CpuUsageFeature,
    DatabaseFeature,
    DatabasePathFeature,
    HttpEndpointProvider,
    EngineSelectorFeature,
    EnvironmentFeature,
    FlushFeature,
    FortuneFeature,
    FoxxFeature,
    FrontendFeature,
    GeneralServerFeature,
    GreetingsFeature,
    InitDatabaseFeature,
    LanguageCheckFeature,
    LanguageFeature,
    TimeZoneFeature,
    LockfileFeature,
    LogBufferFeature,
    LoggerFeature,
    maintenance_feature,
    MaxMapCountFeature,
    NetworkFeature,
    NonceFeature,
    PrivilegeFeature,
    QueryRegistryFeature,
    RandomFeature,
    ReplicationFeature,
    replicated_log_feature,
    ReplicationMetricsFeature,
    ReplicationTimeoutFeature,
    SchedulerFeature,
    ScriptFeature,
    ServerFeature,
    ServerIdFeature,
    ServerSecurityFeature,
    ShardingFeature,
    SharedPRNGFeature,
    ShellColorsFeature,
    ShutdownFeature,
    SoftShutdownFeature,
    SslFeature,
    StatisticsFeature,
    StorageEngineFeature,
    SystemDatabaseFeature,
    TempFeature,
    TtlFeature,
    UpgradeFeature,
    V8DealerFeature,
    V8PlatformFeature,
    V8SecurityFeature,
    transaction::ManagerFeature,
    ViewTypesFeature,
    sql::sql_function_feature,
    sql::OptimizerRulesFeature,
    pregel::PregelFeature,
    RocksDBOptionFeature,
    RocksDBRecoveryManager,
#ifdef _WIN32
    WindowsServiceFeature,
#endif
#ifdef TRI_HAVE_GETRLIMIT
    FileDescriptorsFeature,
#endif
#ifdef DBMS_HAVE_FORK
    DaemonFeature,
    SupervisorFeature,
#endif
#ifdef USE_ENTERPRISE
    AuditFeature,
    LdapFeature,
    LicenseFeature,
    RCloneFeature,
    HotBackupFeature,
    EncryptionFeature,
#endif
    SslServerFeature,
    iresearch::IResearchAnalyzerFeature,
    iresearch::IResearchFeature,
    ClusterEngine,
    RocksDBEngine,
    cluster::FailureOracleFeature,
    replication_sdk::replicated_state::replicated_state_app_feature,
    replication_sdk::replicated_state::black_hole::BlackHoleStateMachineFeature,
    replication_sdk::replicated_state::prototype::prototype_state_machine_feature,
    replication_sdk::replicated_state::document::DocumentStateMachineFeature,
    Bitcoin
>;    // clang-format on

    using DbmsdServer = application_features::ApplicationServerT<DbmsdFeatures>;
    using DbmsdFeature = application_features::ApplicationFeatureT<DbmsdServer>;

}    // namespace nil::dbms
