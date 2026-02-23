#pragma once
#include "../transport/serialization.hpp"

namespace hell::skeletons {

/**
 * \brief Logical transformation skeleton from `In` to `Out`.
 * \tparam In Input item type.
 * \tparam Out Output item type.
 */
template <hell::transport::Serializable In, hell::transport::Serializable Out>
class Stage {
      public:
	using InputType  = In;
	using OutputType = Out;

	/**
	 * \brief Creates a stage from a worker function.
	 * \param fn Transformation callable.
	 * \param ordered Reserved flag for ordered processing.
	 */
	explicit Stage(std::function<Out(In)> fn, bool ordered = false) : work_(std::move(fn)), ordered_(ordered) {
	}

	/**
	 * \brief Returns the worker callable.
	 * \return Reference to stored transformation function.
	 */
	const auto& work_function() const {
		return work_;
	}

      private:
	std::function<Out(In)> work_;
	bool                   ordered_ = false;
};
} // namespace hell::skeletons
