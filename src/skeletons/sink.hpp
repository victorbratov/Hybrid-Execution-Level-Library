#pragma once
#include "../transport/serialization.hpp"

namespace hell::skeletons {
template <hell::transport::Serializable In>
class Sink {
      public:
	using InputType  = In;
	using OutputType = void;

	explicit Sink(std::function<void(In)> consumer) : consumer_(std::move(consumer)) {
	}

	const auto& consumer_function() {
		return consumer_;
	}

      private:
	std::function<void(In)> consumer_;
};
} // namespace hell::skeletons
