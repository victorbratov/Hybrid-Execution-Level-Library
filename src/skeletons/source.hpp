#pragma once
#include "../transport/serialization.hpp"

namespace hell::skeletons {

/**
 * \brief Logical source skeleton that generates workflow items.
 * \tparam Out Generated item type.
 */
template <hell::transport::Serializable Out>
class Source {
      public:
	using InputType  = void;
	using OutputType = Out;

	/**
	 * \brief Creates a source from a generator function.
	 * \param generator Function returning next item or `std::nullopt` to end stream.
	 */
	explicit Source(std::function<std::optional<Out>()> generator) : generator_(std::move(generator)) {
	}

	/**
	 * \brief Returns the generator callable.
	 * \return Reference to stored generator function.
	 */
	const auto& generator_function() const {
		return generator_;
	}

      private:
	std::function<std::optional<Out>()> generator_;
};
} // namespace hell::skeletons
