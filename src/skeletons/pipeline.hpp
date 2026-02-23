#pragma once
#include <type_traits>
#include <tuple>

namespace hell::detail {

/**
 * \brief Compile-time predicate for stage composability (`A::OutputType == B::InputType`).
 */
template <typename A, typename B>
concept Chainable = std::is_same_v<typename A::OutputType, typename B::InputType>;

/**
 * \brief Validates a sequence of stages is chainable at compile time.
 * \return True when all neighboring stages are chainable.
 */
template <typename... Stages>
constexpr bool validate_chain() {
	if constexpr (sizeof...(Stages) <= 1) {
		return true;
	} else {
		return []<typename First, typename Second, typename... Rest>(
		               std::type_identity<First>,
		               std::type_identity<Second>,
		               std::type_identity<Rest>...) {
			static_assert(Chainable<First, Second>, "Stages must be chainable");
			if constexpr (sizeof...(Rest) == 0) {
				return Chainable<First, Second>;
			} else {
				return Chainable<First, Second> && validate_chain<Second, Rest...>();
			}
		}(std::type_identity<Stages>{}...);
	}
}

} // namespace hell::detail

namespace hell::skeletons {

/**
 * \brief Typed immutable container of logically chained skeleton stages.
 * \tparam Stages Stage types in execution order.
 */
template <typename... Stages>
class Pipeline {
      public:
	static_assert(detail::validate_chain<Stages...>(), "Stages must be chainable");

	using InputType  = typename std::tuple_element_t<0, std::tuple<Stages...>>::InputType;
	using OutputType = typename std::tuple_element_t<sizeof...(Stages) - 1, std::tuple<Stages...>>::OutputType;

	/**
	 * \brief Constructs a pipeline from stage instances.
	 * \param stages Stage values in workflow order.
	 */
	explicit Pipeline(Stages... stages) : stages_(std::move(stages)...) {
	}

	/**
	 * \brief Returns the internal tuple of stages.
	 * \return Const reference to stored stages.
	 */
	[[nodiscard]] const auto& stages() const {
		return stages_;
	}

	/**
	 * \brief Number of stages in this pipeline type.
	 * \return Compile-time pipeline size.
	 */
	static constexpr size_t pipeline_size() {
		return sizeof...(Stages);
	}

      private:
	std::tuple<Stages...> stages_;
};

} // namespace hell::skeletons
