#pragma once
#include "../transport/serialization.hpp"

namespace hell::skeletons {

/**
 * \brief Logical terminal skeleton that consumes items.
 * \tparam In Consumed item type.
 */
template <hell::transport::Serializable In>
class Sink {
      public:
	using InputType  = In;
	using OutputType = void;

	/**
	 * \brief Creates a sink from a consumer function.
	 * \param consumer Callable invoked for each incoming item.
	 */
	explicit Sink(std::function<void(In)> consumer) : consumer_(std::move(consumer)) {
	}

	/**
	 * \brief Returns the consumer callable.
	 * \return Reference to stored consumer function.
	 */
	const auto& consumer_function() const {
		return consumer_;
	}

      private:
	std::function<void(In)> consumer_;
};
} // namespace hell::skeletons
