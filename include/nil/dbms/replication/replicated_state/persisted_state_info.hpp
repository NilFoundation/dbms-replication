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
#include <nil/dbms/replication/replicated_state/agency_specification.hpp>
#include <nil/dbms/replication/replicated_state/state_common.hpp>

namespace nil::dbms::replication::replicated_state {

struct persisted_state_info {
  LogId stateId;
  snapshot_info snapshot;
  state_generation generation;
  agency::implementation_spec specification;
};

template<class Inspector>
auto inspect(Inspector& f, persisted_state_info& x) {
  return f.object(x).fields(f.field("stateId", x.stateId),
                            f.field("snapshot", x.snapshot),
                            f.field("generation", x.generation),
                            f.field("specification", x.specification));
}

struct state_persistor_interface {
  virtual ~state_persistor_interface() = default;
  virtual void update_state_information(persisted_state_info const&) noexcept = 0;
  virtual void delete_state_information(LogId stateId) noexcept = 0;
};

}  // namespace arangodb::replication::replicated_state
