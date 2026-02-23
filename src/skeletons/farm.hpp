#pragma once
#include "../transport/serialization.hpp"

namespace hell::skeletons {

/**
 * \brief Policy options for a logical farm stage.
 */
struct FarmPolicy {
	/** \brief Number of local workers (0 means auto). */
	size_t num_workers = 0;

	/** \brief Preferred execution placement for the farm. */
	enum class Distribution {
		LOCAL,
		DISTRIBUTED,
		HYBRID,
		AUTO
	};

	/** \brief Requested distribution mode. */
	Distribution distribution = Distribution::AUTO;

	/** \brief Suggested queue capacity for farm internals. */
	size_t queue_capacity = 1024;
};

/**
 * \brief Logical farm skeleton that applies the same work function in parallel.
 * \tparam In Input item type.
 * \tparam Out Output item type.
 */
template <hell::transport::Serializable In, hell::transport::Serializable Out>
class Farm {
      public:
	using InputType  = In;
	using OutputType = Out;

	/**
	 * \brief Creates a farm with a worker function and worker hint.
	 * \param work_function Worker callable for each input item.
	 * \param num_workers Number of local workers (0 means auto).
	 */
	Farm(std::function<Out(In)> work_function, size_t num_workers = 0) : work_(std::move(work_function)), policy_{.num_workers = num_workers} {
	}

	/**
	 * \brief Creates a farm with an explicit policy.
	 * \param work_function Worker callable for each input item.
	 * \param policy Placement and sizing policy.
	 */
	Farm(std::function<Out(In)> work_function, FarmPolicy policy) : work_(std::move(work_function)), policy_(std::move(policy)) {
	}

	/**
	 * \brief Returns the worker callable.
	 * \return Reference to stored worker function.
	 */
	const auto& work_function() const {
		return work_;
	}

	/**
	 * \brief Returns the farm policy.
	 * \return Reference to current policy.
	 */
	const auto& policy() const {
		return policy_;
	}

      private:
	std::function<Out(In)> work_;
	FarmPolicy             policy_;
};
} // namespace hell::skeletons
