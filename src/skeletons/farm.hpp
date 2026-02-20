#pragma once
#include "../transport/serialization.hpp"

namespace hell::skeletons {
struct FarmPolicy {
	size_t num_workers = 0;

	enum class Distribution {
		LOCAL,
		DISTRIBUTED,
		HYBRID,
		AUTO
	};

	Distribution distribution = Distribution::AUTO;

	size_t queue_capacity = 1024;
};

template <hell::transport::Serializable In, hell::transport::Serializable Out>
class Farm {
      public:
	using InputType  = In;
	using OutputType = Out;

	Farm(std::function<Out(In)> work_function, size_t num_workers = 0) : work_(std::move(work_function)), policy_{.num_workers = num_workers} {
	}

	Farm(std::function<Out(In)> work_function, FarmPolicy policy) : work_(std::move(work_function)), policy_(std::move(policy)) {
	}

	const auto& work_function() {
		return work_;
	}

	const auto& policy() {
		return policy_;
	}

      private:
	std::function<Out(In)> work_;
	FarmPolicy             policy_;
};
} // namespace hell::skeletons
