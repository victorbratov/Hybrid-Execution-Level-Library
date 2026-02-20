#pragma once
#include "../transport/serialization.hpp"

namespace hell::skeletons {
template <hell::transport::Serializable Out>
class Source {
      public:
	using InputType  = void;
	using OutputType = Out;

	explicit Source(std::function<std::optional<Out>()> generator) : generator_(std::move(generator)) {
	}

	const auto& generator_function() {
		return generator_;
	}

      private:
	std::function<std::optional<Out>()> generator_;
};
} // namespace hell::skeletons
