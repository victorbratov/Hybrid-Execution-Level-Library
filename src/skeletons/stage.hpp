#pragma once
#include "../transport/serialization.hpp"

namespace hell::skeletons {
template <hell::transport::Serializable In, hell::transport::Serializable Out>
class Stage {
      public:
	using InputType  = In;
	using OutputType = Out;

	explicit Stage(std::function<Out(In)> fn, bool ordered = false) : work_(std::move(fn)), ordered_(ordered) {
	}

	const auto& work_function() {
		return work_;
	}

      private:
	std::function<Out(In)> work_;
	bool                   ordered_ = false;
};
} // namespace hell::skeletons
