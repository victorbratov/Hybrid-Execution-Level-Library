/**
 * @file main.hpp
 * @brief Main entry point header for the Hell library.
 *
 * Includes all necessary components for building and running
 * a workflow pipeline on an MPI cluster.
 */
#pragma once
#if !__has_include(<mpi.h>)
#error "Hell requires MPI to be available"
#endif

#include <cstdint>
#include <vector>

/**
 * @enum StageType
 * @brief Identifies the functional role of a pipeline stage.
 */
enum class StageType : uint8_t {
	SOURCE,
	SINK,
	FILTER,
	FARM
};

/**
 * @struct StageDescriptor
 * @brief Metadata describing how a stage fits into the mapped execution plan.
 *
 * Each stage instance gets one descriptor. If a logical stage is distributed
 * across N nodes, N descriptors share the same `id` but differ in
 * `assigned_node` and `input_tag`.
 */
struct StageDescriptor {
	uint32_t  id;
	StageType type;
	uint32_t  concurrency;

	uint32_t assigned_node;
	uint16_t assigned_threads;

	uint32_t previous_stage_id;
	uint32_t next_stage_id;

	uint32_t              input_tag;
	std::vector<uint32_t> output_tags;
	std::vector<uint32_t> output_ranks;
	uint32_t              expected_eos_count = 0;
};

/**
 * @struct WorkflowPlan
 * @brief The complete mapped execution plan for all stages.
 */
struct WorkflowPlan {
	std::vector<StageDescriptor> stages;
	uint32_t                     num_stages;
};

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <concepts>
#include <functional>
#include <iterator>
#include <optional>
#include <type_traits>
#include <typeindex>
#include <utility>

#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <cstring>
#include <stdexcept>
#include <typeinfo>
#include <unordered_map>
#include <vector>

/**
 * @brief Computes a compile-time hash for type identification.
 */
inline constexpr uint64_t fnv1a(const char* string) {
	uint64_t hash = 14695981039346656037ULL;
	while (*string) {
		hash ^= static_cast<unsigned char>(*string++);
		hash *= 1099511628211ULL;
	}
	return hash;
}

/**
 * @brief Gets a unique ID for a type.
 */
template <typename T>
inline constexpr uint64_t type_id() {
#if defined(__clang__) || defined(__GNUC__)
	return fnv1a(__PRETTY_FUNCTION__);
#elif defined(_MSC_VER)
	return fnv1a(__FUNCSIG__);
#endif
}

/**
 * @concept Serializable
 * @brief Concept defining types that implement custom serialization.
 */
template <typename T>
concept Serializable = requires(
                               T                     t,
                               std::vector<uint8_t>& buf,
                               const uint8_t*        data,
                               size_t                size) {
	{ t.serialize(buf) } -> std::same_as<void>;
	{ T::deserialize(data, size) } -> std::convertible_to<T>;
} && std::copy_constructible<T>;

/**
 * @concept TriviallySerializable
 * @brief Concept defining types that can be bitwise copied.
 */
template <typename T>
concept TriviallySerializable = std::is_trivially_copyable_v<T> && !Serializable<T>;

/**
 * @concept PayloadCompatible
 * @brief Concept defining all types supported by the Payload system.
 */
template <typename T>
concept PayloadCompatible = Serializable<T> || TriviallySerializable<T>;

/**
 * @struct IHolder
 * @brief Interface for type-erased payload storage.
 */
struct IHolder {
	virtual ~IHolder()                                                = default;
	virtual uint64_t                 id() const                       = 0;
	virtual void                     write(std::vector<uint8_t>& buf) = 0;
	virtual std::unique_ptr<IHolder> clone() const                    = 0;
	virtual void*                    ptr()                            = 0;
	virtual const void*              ptr() const                      = 0;
};

/**
 * @class Holder
 * @brief Concrete type-erased storage for a specific payload type.
 */
template <PayloadCompatible T>
class Holder final : public IHolder {
	T value;

      public:
	explicit Holder(T value) :
	        value(std::move(value)) {
	}

	uint64_t id() const override {
		return type_id<T>();
	}

	void write(std::vector<uint8_t>& buf) override {
		if constexpr (TriviallySerializable<T>) {
			auto* p = reinterpret_cast<const uint8_t*>(&value);
			buf.insert(buf.end(), p, p + sizeof(value));
		} else {
			value.serialize(buf);
		}
	}

	std::unique_ptr<IHolder> clone() const override {
		return std::make_unique<Holder>(value);
	}

	void* ptr() override {
		return &value;
	}
	void const* ptr() const override {
		return &value;
	}
};

/**
 * @class PayloadRegistry
 * @brief Global registry for deserializing payload types.
 */
class PayloadRegistry {
      private:
	static auto& map() {
		static std::unordered_map<uint64_t, Factory> m;
		return m;
	}

      public:
	using Factory = std::function<std::unique_ptr<IHolder>(const uint8_t*, size_t)>;

	template <PayloadCompatible T>
	static void register_type() {
		auto  id = type_id<T>();
		auto& m  = map();
		if (m.contains(id))
			return;

		if constexpr (TriviallySerializable<T>) {
			m[id] = [](const uint8_t* data, size_t /*size*/) {
				T val;
				std::memcpy(&val, data, sizeof(T));
				return std::make_unique<Holder<T>>(std::move(val));
			};
		} else {
			m[id] = [](const uint8_t* data, size_t size) {
				return std::make_unique<Holder<T>>(T::deserialize(data, size));
			};
		}
	}

	template <PayloadCompatible... T>
	static void register_all() {
		(register_type<T>(), ...);
	}

	static std::unique_ptr<IHolder> make(uint64_t type_id, const uint8_t* data, size_t size) {
		auto& m  = map();
		auto  it = m.find(type_id);
		if (it == m.end()) {
			throw std::runtime_error("Unknown payload type");
		}
		return it->second(data, size);
	}
};

/**
 * @class Payload
 * @brief Type-erased container for passing arbitrary data between pipeline stages.
 */
class Payload {
	std::unique_ptr<IHolder> holder_;

      public:
	Payload() = default;

	template <PayloadCompatible T>
	Payload(T value) :
	        holder_(std::make_unique<Holder<T>>(std::move(value))) {
		PayloadRegistry::register_type<T>();
	}

	Payload(const Payload& other) :
	        holder_(other.holder_ ? other.holder_->clone() : nullptr) {
	}
	Payload operator=(const Payload& other) {
		holder_ = other.holder_ ? other.holder_->clone() : nullptr;
		return *this;
	}
	Payload(Payload&& other) noexcept            = default;
	Payload& operator=(Payload&& other) noexcept = default;

	[[nodiscard]] bool empty() const {
		return !holder_;
	}
	[[nodiscard]] uint64_t type_id() const {
		return holder_ ? holder_->id() : 0;
	}

	template <typename T>
	[[nodiscard]] bool holds() const {
		return holder_ && holder_->id() == ::type_id<T>();
	}

	template <typename T>
	T& get() {
		assert(holds<T>());
		return *static_cast<T*>(holder_->ptr());
	}

	template <typename T>
	const T& get() const {
		assert(holds<T>());
		return *static_cast<const T*>(holder_->ptr());
	}

	template <typename T>
	T& get_or_throw() {
		if (!holds<T>())
			throw std::bad_cast();
		return get<T>();
	}

	[[nodiscard]] std::vector<uint8_t> serialize() const {
		assert(holder_);
		std::vector<uint8_t> data;
		holder_->write(data);

		std::vector<uint8_t> buf;
		buf.reserve(data.size() + sizeof(uint64_t) * 2);

		uint64_t type_id = holder_->id();
		uint64_t size    = data.size();

		auto append = [&](const auto& value) {
			auto* p = reinterpret_cast<const uint8_t*>(&value);
			buf.insert(buf.end(), p, p + sizeof(value));
		};
		append(type_id);
		append(size);
		buf.insert(buf.end(), data.begin(), data.end());
		return buf;
	}

	static Payload deserialize(const uint8_t* raw, size_t size) {
		assert(size >= sizeof(uint64_t) * 2);
		uint64_t type_id, data_size;
		std::memcpy(&type_id, raw, sizeof(type_id));
		std::memcpy(&data_size, raw + sizeof(type_id), sizeof(data_size));

		Payload p;
		p.holder_ = PayloadRegistry::make(type_id, raw + sizeof(uint64_t) * 2, static_cast<size_t>(data_size));
		return p;
	}

	static Payload deserialize(const std::vector<uint8_t>& buf) {
		return deserialize(buf.data(), buf.size());
	}
};

#include <coroutine>
#include <exception>
#include <iterator>
#include <optional>
#include <utility>
#include <cassert>

/**
 * @struct stop_iteration
 * @brief Exception thrown to signal the end of generator iteration.
 */
struct stop_iteration {};

/**
 * @class generator
 * @brief A coroutine-based generator yielding values of type T.
 *
 * This generator uses C++20 coroutines to lazily evaluate and yield values.
 *
 * @tparam T The type of the value yielded by the generator.
 */
template <typename T>
class generator {
      public:
	/**
	 * @struct promise_type
	 * @brief The coroutine promise type for the generator.
	 */
	struct promise_type {
		std::optional<T>   current_value = std::nullopt;
		std::exception_ptr exception_    = nullptr;

		generator get_return_object() {
			return generator{std::coroutine_handle<promise_type>::from_promise(*this)};
		}

		std::suspend_always initial_suspend() noexcept {
			return {};
		}
		std::suspend_always final_suspend() noexcept {
			return {};
		}

		std::suspend_always yield_value(T& value) noexcept {
			current_value = value;
			return {};
		}

		std::suspend_always yield_value(T&& value) noexcept {
			current_value = std::move(value);
			return {};
		}

		void return_void() noexcept {
		}

		void unhandled_exception() {
			exception_ = std::current_exception();
		}
	};

	generator() noexcept = default;

	explicit generator(std::coroutine_handle<promise_type> handle) noexcept
	        :
	        handle_(handle) {
	}

	~generator() {
		if (handle_)
			handle_.destroy();
	}

	generator(const generator&)            = delete;
	generator& operator=(const generator&) = delete;

	generator(generator&& other) noexcept
	        :
	        handle_(std::exchange(other.handle_, nullptr)) {
	}

	generator& operator=(generator&& other) noexcept {
		if (this != &other) {
			if (handle_)
				handle_.destroy();
			handle_ = std::exchange(other.handle_, nullptr);
		}
		return *this;
	}

	/**
	 * @brief Resumes the coroutine to get the next value.
	 * @return An optional containing the next value, or std::nullopt if the generator is done.
	 */
	std::optional<T> next() {
		if (!handle_ || handle_.done())
			return std::nullopt;

		handle_.resume();

		if (handle_.done()) {
			if (handle_.promise().exception_)
				std::rethrow_exception(handle_.promise().exception_);
			return std::nullopt;
		}

		return handle_.promise().current_value;
	}

	/**
	 * @brief Checks if the generator has more values to yield.
	 * @return True if another value can be obtained, false otherwise.
	 */
	bool has_next() const noexcept {
		return handle_ && !handle_.done();
	}

	/**
	 * @brief Checks if the generator has finished execution.
	 * @return True if the generator is done, false otherwise.
	 */
	bool done() const noexcept {
		return !handle_ || handle_.done();
	}

	/**
	 * @class iterator
	 * @brief An input iterator for the generator.
	 */
	class iterator {
	      public:
		using iterator_category = std::input_iterator_tag;
		using difference_type   = std::ptrdiff_t;
		using value_type        = T;
		using reference         = T&;
		using pointer           = T*;

		iterator() noexcept = default;
		explicit iterator(std::coroutine_handle<promise_type> h) noexcept :
		        handle_(h) {
		}

		iterator& operator++() {
			handle_.resume();
			if (handle_.done())
				handle_ = nullptr;
			return *this;
		}

		void operator++(int) {
			++(*this);
		}

		reference operator*() const noexcept {
			return *handle_.promise().current_value;
		}
		pointer operator->() const noexcept {
			return std::addressof(*handle_.promise().current_value);
		}

		friend bool operator==(const iterator& a, const iterator& b) noexcept {
			return a.handle_ == b.handle_;
		}
		friend bool operator!=(const iterator& a, const iterator& b) noexcept {
			return !(a == b);
		}

	      private:
		std::coroutine_handle<promise_type> handle_ = nullptr;
	};

	/**
	 * @brief Gets an iterator to the beginning of the generated sequence.
	 * @return An iterator pointing to the first generated element.
	 */
	iterator begin() {
		if (handle_) {
			handle_.resume();
			if (handle_.done())
				return {};
		}
		return iterator{handle_};
	}

	/**
	 * @brief Gets a sentinel representing the end of the generated sequence.
	 * @return An empty iterator representing the end.
	 */
	iterator end() noexcept {
		return {};
	}

      private:
	std::coroutine_handle<promise_type> handle_ = nullptr;
};

/**
 * @class StageBase
 * @brief Abstract base class for all pipeline stages.
 */
class StageBase {
      public:
	using CacheInvalidationFn = std::function<bool(
	        std::chrono::steady_clock::time_point,
	        std::chrono::steady_clock::time_point,
	        std::chrono::steady_clock::time_point,
	        uint64_t,
	        size_t)>;

	uint32_t  id = 0;
	StageType type_;
	uint32_t  requested_concurrency = 1;
	std::optional<uint32_t> cache_size_kb;
	std::chrono::milliseconds cache_ttl = std::chrono::minutes(5);
	std::optional<CacheInvalidationFn> cache_invalidation_fn;

	using InputType  = void;
	using OutputType = void;

	virtual ~StageBase() = default;

	virtual Payload execute(const Payload&) = 0;

	virtual std::optional<Payload> generate() {
		return std::nullopt;
	};

	virtual void consume(const Payload&) {};
};

template <typename T>
concept StageCompatible = std::is_base_of_v<StageBase, T>;

/**
 * @class SourceStage
 * @brief A pipeline stage that acts as a data generator (no inputs, only outputs).
 * @tparam Output The payload type generated by this stage.
 */
template <PayloadCompatible Output>
class SourceStage : public StageBase {
	generator<Output> generator_;

      public:
	using OutputType = Output;

	explicit SourceStage(generator<Output> generator) :
	        generator_(std::move(generator)) {
		type_ = StageType::SOURCE;
		PayloadRegistry::register_type<Output>();
	};

	Payload execute(const Payload&) override {
		return {};
	}

	std::optional<Payload> generate() override {
		auto item = generator_.next();
		if (!item.has_value()) {
			return std::nullopt;
		}
		return Payload(std::move(*item));
	}
};

/**
 * @class SinkStage
 * @brief A pipeline stage that acts as a data consumer (inputs, no outputs).
 * @tparam Input The payload type consumed by this stage.
 */
template <PayloadCompatible Input>
class SinkStage : public StageBase {
	std::function<void(const Input&)> consumer_fn_;

      public:
	using InputType = Input;

	explicit SinkStage(std::function<void(const Input&)> consumer_fn) :
	        consumer_fn_(consumer_fn) {
		type_ = StageType::SINK;
		PayloadRegistry::register_type<Input>();
	};

	Payload execute(const Payload&) override {
		return {};
	}

	void consume(const Payload& input_item) override {
		consumer_fn_(input_item.get<Input>());
	};
};

/**
 * @class FilterStage
 * @brief A standard 1-to-1 processing stage.
 * @tparam Input The payload type consumed by this stage.
 * @tparam Output The payload type produced by this stage.
 */
template <PayloadCompatible Input, PayloadCompatible Output>
class FilterStage : public StageBase {
	std::function<Output(const Input&)> processor_fn_;
	static constexpr uint32_t            default_cache_size_kb = 1024;
 	static constexpr std::chrono::milliseconds default_cache_ttl =
	        std::chrono::minutes(5);

      public:
	using InputType  = Input;
	using OutputType = Output;

	explicit FilterStage(std::function<Output(const Input&)> processor_fn) :
	        processor_fn_(processor_fn) {
		type_ = StageType::FILTER;
		PayloadRegistry::register_all<Input, Output>();
	};

	FilterStage& set_caching(
	        uint32_t size_kb = default_cache_size_kb,
	        std::chrono::milliseconds ttl = default_cache_ttl) {
		cache_size_kb = size_kb;
		cache_ttl     = ttl;
		return *this;
	}

	FilterStage& set_cache_invalidation(CacheInvalidationFn invalidation_fn) {
		cache_invalidation_fn = std::move(invalidation_fn);
		return *this;
	}

	Payload execute(const Payload& input_item) override {
		return processor_fn_(input_item.get<Input>());
	};
};

/**
 * @class FarmStage
 * @brief A concurrent processing stage that instances multiple workers.
 * @tparam Input The payload type consumed by this stage.
 * @tparam Output The payload type produced by this stage.
 */
template <PayloadCompatible Input, PayloadCompatible Output>
class FarmStage : public StageBase {
	std::function<Output(const Input&)> processor_fn_;
	static constexpr uint32_t            default_cache_size_kb = 1024;
	static constexpr std::chrono::milliseconds default_cache_ttl =
	        std::chrono::minutes(5);

      public:
	using InputType  = Input;
	using OutputType = Output;

	explicit FarmStage(std::function<Output(const Input&)> processor_fn) :
	        processor_fn_(processor_fn) {
		type_                 = StageType::FARM;
		requested_concurrency = 0;
		PayloadRegistry::register_all<Input, Output>();
	};

	FarmStage& concurrency(uint32_t concurrency) {
		requested_concurrency = concurrency;
		return *this;
	};

	FarmStage& set_caching(
	        uint32_t size_kb = default_cache_size_kb,
	        std::chrono::milliseconds ttl = default_cache_ttl) {
		cache_size_kb = size_kb;
		cache_ttl     = ttl;
		return *this;
	}

	FarmStage& set_cache_invalidation(CacheInvalidationFn invalidation_fn) {
		cache_invalidation_fn = std::move(invalidation_fn);
		return *this;
	}

	Payload execute(const Payload& input_item) override {
		return processor_fn_(input_item.get<Input>());
	};
};

template <typename T>
struct is_sink_stage : std::false_type {};

template <PayloadCompatible T>
struct is_sink_stage<SinkStage<T>> : std::true_type {};

template <typename T>
constexpr bool is_sink_stage_v = is_sink_stage<T>::value;

template <typename T>
struct is_source_stage : std::false_type {};

template <PayloadCompatible T>
struct is_source_stage<SourceStage<T>> : std::true_type {};

template <typename T>
constexpr bool is_source_stage_v = is_source_stage<T>::value;

template <typename T>
struct is_filter_stage : std::false_type {};

template <PayloadCompatible Input, PayloadCompatible Output>
struct is_filter_stage<FilterStage<Input, Output>> : std::true_type {};

template <typename T>
constexpr bool is_filter_stage_v = is_filter_stage<T>::value;

template <typename T>
struct is_farm_stage : std::false_type {};

template <PayloadCompatible Input, PayloadCompatible Output>
struct is_farm_stage<FarmStage<Input, Output>> : std::true_type {};

template <typename T>
constexpr bool is_farm_stage_v = is_farm_stage<T>::value;

/**
 * @file serialization.hpp
 * @brief Convenience functions for payload serialization and deserialization.
 */

#include <vector>

/**
 * @brief Helper to deserialize a Payload from a byte buffer.
 * @param buf The byte buffer to deserialize.
 * @return The deserialized Payload.
 */
inline Payload deserialize_payload(const std::vector<uint8_t>& buf) {
	return Payload::deserialize(buf);
}

/**
 * @brief Helper to serialize a Payload into a byte buffer.
 * @param payload The payload to serialize.
 * @return The serialized payload as a byte buffer.
 */
inline std::vector<uint8_t> serialize_payload(const Payload& payload) {
	return payload.serialize();
}

/**
 * @brief Serializes a batch of Payloads into a single byte buffer.
 *
 * Wire format: [uint32_t count] [uint32_t len_0][payload_0 bytes] [uint32_t len_1][payload_1 bytes] ...
 *
 * @param batch The vector of payloads to serialize.
 * @return The serialized batch as a byte buffer.
 */
inline std::vector<uint8_t> serialize_batch(const std::vector<Payload>& batch) {
	std::vector<uint8_t> buf;

	auto append = [&](const auto& value) {
		auto* p = reinterpret_cast<const uint8_t*>(&value);
		buf.insert(buf.end(), p, p + sizeof(value));
	};

	uint32_t count = static_cast<uint32_t>(batch.size());
	append(count);

	for (auto& payload : batch) {
		auto item_buf = payload.serialize();
		uint32_t item_len = static_cast<uint32_t>(item_buf.size());
		append(item_len);
		buf.insert(buf.end(), item_buf.begin(), item_buf.end());
	}

	return buf;
}

/**
 * @brief Deserializes a batch of Payloads from a byte buffer.
 * @param buf The byte buffer produced by serialize_batch.
 * @return A vector of deserialized Payloads.
 */
inline std::vector<Payload> deserialize_batch(const std::vector<uint8_t>& buf) {
	const uint8_t* ptr = buf.data();

	auto read = [&]<typename T>(T& out) {
		std::memcpy(&out, ptr, sizeof(T));
		ptr += sizeof(T);
	};

	uint32_t count;
	read(count);

	std::vector<Payload> result;
	result.reserve(count);

	for (uint32_t i = 0; i < count; ++i) {
		uint32_t item_len;
		read(item_len);
		result.push_back(Payload::deserialize(ptr, item_len));
		ptr += item_len;
	}

	return result;
}

#include <cassert>
#include <memory>
#include <type_traits>
#include <vector>

/**
 * @class Pipeline
 * @brief Represents a sequence of connected processing stages.
 */
template <typename Input, typename Output>
class Pipeline {
	using InputType  = Input;
	using OutputType = Output;

      public:
	std::vector<std::shared_ptr<StageBase>> stages_;

	Pipeline() = default;

	explicit Pipeline(std::shared_ptr<StageBase> stage) {
		stages_.push_back(stage);
	};

	template <typename OtherInput, typename OtherOutput>
	Pipeline(Pipeline<OtherInput, OtherOutput>&& other) : stages_(std::move(other.stages_)) {
	}

	Pipeline&& operator|(Pipeline&& rhs) {
		for (auto& stage : rhs.stages_) {
			stages_.push_back(stage);
		}
		return std::move(*this);
	};
};

template <StageCompatible L, StageCompatible R>
Pipeline<typename std::decay_t<L>::InputType, typename std::decay_t<R>::OutputType> operator|(L&& lhs, R&& rhs) {
	static_assert(
	        std::is_same_v<typename std::decay_t<L>::OutputType, typename std::decay_t<R>::InputType>,
	        "Stage Output type and next Stage Input type must match");
	static_assert(!is_sink_stage_v<L>, "LHS stage cannot be a SinkStage");
	static_assert(!is_source_stage_v<R>, "RHS stage cannot be a SourceStage");
	Pipeline<typename std::decay_t<L>::InputType, typename std::decay_t<R>::OutputType> pipeline;
	pipeline.stages_.push_back(std::make_shared<std::decay_t<L>>(std::forward<L>(lhs)));
	pipeline.stages_.push_back(std::make_shared<std::decay_t<R>>(std::forward<R>(rhs)));
	return pipeline;
};

template <typename PipelineInput, typename PipelineOutput, StageCompatible R>
Pipeline<PipelineInput, typename std::decay_t<R>::OutputType> operator|(Pipeline<PipelineInput, PipelineOutput>&& lhs, R&& rhs) {
	static_assert(!is_source_stage_v<R>, "RHS stage cannot be a SourceStage");
	static_assert(std::is_same_v<typename std::decay_t<R>::InputType, PipelineOutput>, "RHS stage Input type must match Pipeline Output type");
	lhs.stages_.push_back(std::make_shared<std::decay_t<R>>(std::forward<R>(rhs)));
	return std::move(lhs);
};

#include <mpi.h>
#include <algorithm>
#include <unordered_map>

/**
 * @class Planner
 * @brief Responsible for creating an execution plan for a pipeline across a cluster.
 */
class Planner {
      public:
	static WorkflowPlan plan(const Pipeline<void, void>& pipeline, uint16_t world_size, const std::vector<int> cores_per_node) {
		WorkflowPlan   wp;
		const uint32_t num_logical_stages = pipeline.stages_.size();

		for (uint32_t i = 0; i < num_logical_stages; ++i) {
			pipeline.stages_[i]->id = i;
		}

		std::vector<int> remaining_cores = cores_per_node;
		uint32_t         next_tag        = 1;

		uint32_t guaranteed_cores = 0;
		uint32_t auto_farm_count  = 0;

		for (uint32_t i = 0; i < num_logical_stages; ++i) {
			auto& stage = pipeline.stages_[i];
			if (stage->type_ == StageType::FARM && stage->requested_concurrency == 0) {
				++auto_farm_count;
			} else {
				guaranteed_cores += stage->requested_concurrency;
			}
		}

		uint32_t total_cores = 0;
		for (uint16_t n = 0; n < world_size; ++n) {
			total_cores += cores_per_node[n];
		}

		uint32_t available_for_auto = (total_cores > guaranteed_cores) ? (total_cores - guaranteed_cores) : 0;
		uint32_t per_auto_farm      = (auto_farm_count > 0) ? std::max(1u, available_for_auto / auto_farm_count) : 0;

		std::vector<uint32_t> resolved_concurrency(num_logical_stages);
		for (uint32_t i = 0; i < num_logical_stages; ++i) {
			auto& stage = pipeline.stages_[i];
			if (stage->type_ == StageType::FARM && stage->requested_concurrency == 0) {
				resolved_concurrency[i] = per_auto_farm;
			} else {
				resolved_concurrency[i] = stage->requested_concurrency;
			}
		}

		for (uint32_t i = 0; i < num_logical_stages; ++i) {
			auto& stage = pipeline.stages_[i];

			if (stage->type_ == StageType::FARM) {
				uint32_t effective_concurrency = resolved_concurrency[i];
				uint32_t threads_remaining     = effective_concurrency;

				std::vector<uint16_t> candidates;
				for (uint16_t n = 0; n < world_size; ++n) {
					if (remaining_cores[n] > 0)
						candidates.push_back(n);
				}
				std::sort(candidates.begin(), candidates.end(), [&](uint16_t a, uint16_t b) {
					return remaining_cores[a] > remaining_cores[b];
				});

				uint16_t prev_node = (!wp.stages.empty()) ? wp.stages.back().assigned_node : 0;
				bool     collocate = remaining_cores[prev_node] >= (int)threads_remaining;

				if (collocate) {
					StageDescriptor sd;
					sd.id                = stage->id;
					sd.type              = stage->type_;
					sd.concurrency       = effective_concurrency;
					sd.assigned_node     = prev_node;
					sd.assigned_threads  = threads_remaining;
					sd.previous_stage_id = (i > 0) ? i - 1 : UINT32_MAX;
					sd.next_stage_id     = (i < num_logical_stages - 1) ? i + 1 : UINT32_MAX;
					sd.input_tag         = next_tag++;
					remaining_cores[prev_node] -= sd.assigned_threads;
					wp.stages.push_back(sd);
				} else {
					for (uint16_t node : candidates) {
						if (threads_remaining == 0)
							break;

						uint32_t alloc = std::min((uint32_t)remaining_cores[node], threads_remaining);
						if (alloc == 0)
							continue;

						StageDescriptor sd;
						sd.id                = stage->id;
						sd.type              = stage->type_;
						sd.concurrency       = effective_concurrency;
						sd.assigned_node     = node;
						sd.assigned_threads  = alloc;
						sd.previous_stage_id = (i > 0) ? i - 1 : UINT32_MAX;
						sd.next_stage_id     = (i < num_logical_stages - 1) ? i + 1 : UINT32_MAX;
						sd.input_tag         = next_tag++;
						remaining_cores[node] -= alloc;
						threads_remaining -= alloc;
						wp.stages.push_back(sd);
					}

					if (threads_remaining == effective_concurrency) {
						StageDescriptor sd;
						sd.id                = stage->id;
						sd.type              = stage->type_;
						sd.concurrency       = effective_concurrency;
						sd.assigned_node     = candidates.empty() ? 0 : candidates[0];
						sd.assigned_threads  = 1;
						sd.previous_stage_id = (i > 0) ? i - 1 : UINT32_MAX;
						sd.next_stage_id     = (i < num_logical_stages - 1) ? i + 1 : UINT32_MAX;
						sd.input_tag         = next_tag++;
						wp.stages.push_back(sd);
					}
				}
			} else {
				StageDescriptor sd;
				sd.id          = stage->id;
				sd.type        = stage->type_;
				sd.concurrency = stage->requested_concurrency;

				uint16_t best_node = 0;
				if (stage->type_ == StageType::SOURCE) {
					best_node = 0;
				} else {
					uint16_t prev_node = (!wp.stages.empty()) ? wp.stages.back().assigned_node : 0;

					if (remaining_cores[prev_node] >= (int)sd.concurrency) {
						best_node = prev_node;
					} else {
						best_node = 0;
						for (uint16_t node = 1; node < world_size; ++node) {
							if (remaining_cores[node] > remaining_cores[best_node]) {
								best_node = node;
							}
						}
					}
				}

				sd.assigned_node = best_node;

				if (remaining_cores[best_node] < (int)sd.concurrency) {
					sd.assigned_threads = std::max(1, remaining_cores[best_node]);
				} else {
					sd.assigned_threads = sd.concurrency;
				}

				remaining_cores[best_node] -= sd.assigned_threads;

				sd.previous_stage_id = (i > 0) ? i - 1 : UINT32_MAX;
				sd.next_stage_id     = (i < num_logical_stages - 1) ? i + 1 : UINT32_MAX;
				sd.input_tag         = (stage->type_ == StageType::SOURCE) ? UINT32_MAX : next_tag++;

				wp.stages.push_back(sd);
			}
		}

		std::unordered_map<uint32_t, std::vector<size_t>> stage_groups;
		for (size_t idx = 0; idx < wp.stages.size(); ++idx) {
			stage_groups[wp.stages[idx].id].push_back(idx);
		}

		for (size_t idx = 0; idx < wp.stages.size(); ++idx) {
			auto& sd = wp.stages[idx];

			uint32_t next_logical = sd.next_stage_id;
			if (next_logical != UINT32_MAX) {
				auto& successors = stage_groups[next_logical];
				for (size_t succ_idx : successors) {
					sd.output_tags.push_back(wp.stages[succ_idx].input_tag);
					sd.output_ranks.push_back(wp.stages[succ_idx].assigned_node);
				}
			}

			uint32_t prev_logical = sd.previous_stage_id;
			if (prev_logical != UINT32_MAX) {
				sd.expected_eos_count = stage_groups[prev_logical].size();
			} else {
				sd.expected_eos_count = 0;
			}
		}

		wp.num_stages = wp.stages.size();

		return wp;
	}
};

/**
 * @class PlanSerializer
 * @brief Handles serialization, deserialization, and broadcasting of the WorkflowPlan.
 */
class PlanSerializer {
      public:
	static std::vector<uint8_t> serialize(const WorkflowPlan& wp) {
		std::vector<uint8_t> buf;

		auto push = [&](const void* data, size_t size) {
			const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
			buf.insert(buf.end(), ptr, ptr + size);
		};

		push(&wp.num_stages, sizeof(wp.num_stages));
		for (auto& stage : wp.stages) {
			push(&stage.id, sizeof(stage.id));
			push(&stage.type, sizeof(stage.type));
			push(&stage.concurrency, sizeof(stage.concurrency));
			push(&stage.assigned_node, sizeof(stage.assigned_node));
			push(&stage.assigned_threads, sizeof(stage.assigned_threads));
			push(&stage.previous_stage_id, sizeof(stage.previous_stage_id));
			push(&stage.next_stage_id, sizeof(stage.next_stage_id));
			push(&stage.input_tag, sizeof(stage.input_tag));
			push(&stage.expected_eos_count, sizeof(stage.expected_eos_count));

			uint32_t n_out_tags = stage.output_tags.size();
			push(&n_out_tags, sizeof(n_out_tags));
			for (auto& tag : stage.output_tags)
				push(&tag, sizeof(tag));

			uint32_t n_out_ranks = stage.output_ranks.size();
			push(&n_out_ranks, sizeof(n_out_ranks));
			for (auto& rank : stage.output_ranks)
				push(&rank, sizeof(rank));
		}

		return buf;
	}

	static WorkflowPlan deserialize(const std::vector<uint8_t>& buf) {
		WorkflowPlan wp;
		size_t       offset = 0;
		auto         pop    = [&](void* data, size_t size) {
			if (offset + size > buf.size()) {
				throw std::runtime_error("Plan deserialization failed: buffer underflow");
			}
			std::memcpy(data, buf.data() + offset, size);
			offset += size;
		};

		pop(&wp.num_stages, sizeof(wp.num_stages));
		for (uint32_t i = 0; i < wp.num_stages; ++i) {
			StageDescriptor sd;
			pop(&sd.id, sizeof(sd.id));
			pop(&sd.type, sizeof(sd.type));
			pop(&sd.concurrency, sizeof(sd.concurrency));
			pop(&sd.assigned_node, sizeof(sd.assigned_node));
			pop(&sd.assigned_threads, sizeof(sd.assigned_threads));
			pop(&sd.previous_stage_id, sizeof(sd.previous_stage_id));
			pop(&sd.next_stage_id, sizeof(sd.next_stage_id));
			pop(&sd.input_tag, sizeof(sd.input_tag));
			pop(&sd.expected_eos_count, sizeof(sd.expected_eos_count));

			uint32_t n_out_tags;
			pop(&n_out_tags, sizeof(n_out_tags));
			sd.output_tags.resize(n_out_tags);
			for (auto& tag : sd.output_tags)
				pop(&tag, sizeof(tag));

			uint32_t n_out_ranks;
			pop(&n_out_ranks, sizeof(n_out_ranks));
			sd.output_ranks.resize(n_out_ranks);
			for (auto& rank : sd.output_ranks)
				pop(&rank, sizeof(rank));

			wp.stages.push_back(sd);
		}

		return wp;
	}

	static void broadcast_plan(WorkflowPlan& wp, int root, MPI_Comm comm) {
		int rank;
		MPI_Comm_rank(comm, &rank);

		std::vector<uint8_t> buf;
		uint32_t             buf_size = 0;

		if (rank == root) {
			buf      = serialize(wp);
			buf_size = buf.size();
		}

		MPI_Bcast(&buf_size, 1, MPI_UINT32_T, root, comm);
		if (rank != root) {
			buf.resize(buf_size);
		}
		MPI_Bcast(buf.data(), buf_size, MPI_BYTE, root, comm);

		if (rank != root) {
			wp = deserialize(buf);
		}
	}
};

#include <queue>
#include <condition_variable>

template <typename T>
class ConcurrentQueue {
	std::queue<T>           queue_;
	mutable std::mutex      mutex_;
	std::condition_variable cv_;
	std::atomic<bool>       closed_{false};

      public:
	void push(T item) {
		{
			std::lock_guard lock(mutex_);
			queue_.push(std::move(item));
		}
		cv_.notify_one();
	}

	bool pop(T& item) {
		std::unique_lock lock(mutex_);
		cv_.wait(lock, [&] { return !queue_.empty() || closed_; });
		if (queue_.empty())
			return false;
		item = std::move(queue_.front());
		queue_.pop();
		return true;
	}

	void close() {
		{
			std::lock_guard lock(mutex_);
			closed_ = true;
		}
		cv_.notify_all();
	}

	bool closed() const {
		return closed_.load(std::memory_order_acquire);
	}

	size_t size() const {
		std::lock_guard lock(mutex_);
		return queue_.size();
	}
};

struct Message {
	Payload payload;
	bool    eos = false;
};

#include <atomic>
#include <cstdint>
#include <vector>
#include <fstream>
#include <chrono>

#ifdef __APPLE__
#include <mach/mach_host.h>
#include <mach/processor_info.h>
#include <mach/mach.h>
#endif

/**
 * @brief Global toggle for telemetry timers.
 */
inline std::atomic<bool>& telemetry_timers_enabled() {
	static std::atomic<bool> enabled{false};
	return enabled;
}

/**
 * @struct StageMetrics
 * @brief Performance and telemetry metrics for a single pipeline stage.
 */
struct StageMetrics {
	uint32_t              stage_id = 0;
	std::atomic<uint64_t> items_processed{0};
	std::atomic<uint64_t> items_received{0};
	std::atomic<uint64_t> items_sent{0};
	std::atomic<uint64_t> bytes_received{0};
	std::atomic<uint64_t> bytes_sent{0};
	std::atomic<uint64_t> processing_time_us{0};
	std::atomic<uint64_t> idle_time_us{0};
	std::atomic<uint64_t> mpi_send_time_us{0};
	std::atomic<uint64_t> mpi_recv_time_us{0};
	std::atomic<uint32_t> active_workers{0};
	std::atomic<uint32_t> queue_depth{0};

	struct Snapshot {
		uint32_t stage_id;
		uint64_t items_processed;
		uint64_t items_received;
		uint64_t items_sent;
		uint64_t bytes_received;
		uint64_t bytes_sent;
		uint64_t processing_time_us;
		uint64_t idle_time_us;
		uint64_t mpi_send_time_us;
		uint64_t mpi_recv_time_us;
		uint32_t active_workers;
		uint32_t queue_depth;
	};

	Snapshot snapshot() const {
		return {
		        stage_id,
		        items_processed.load(std::memory_order_relaxed),
		        items_received.load(std::memory_order_relaxed),
		        items_sent.load(std::memory_order_relaxed),
		        bytes_received.load(std::memory_order_relaxed),
		        bytes_sent.load(std::memory_order_relaxed),
		        processing_time_us.load(std::memory_order_relaxed),
		        idle_time_us.load(std::memory_order_relaxed),
		        mpi_send_time_us.load(std::memory_order_relaxed),
		        mpi_recv_time_us.load(std::memory_order_relaxed),
		        active_workers.load(std::memory_order_relaxed),
		        queue_depth.load(std::memory_order_relaxed),
		};
	}
};

/**
 * @struct NodeMetrics
 */
struct NodeMetrics {
	int      rank       = 0;
	uint32_t hw_threads = 0;
	double   cpu_load   = 0.0;
	uint64_t rss_bytes  = 0;

	std::vector<StageMetrics::Snapshot> stages;
	std::vector<double>                 core_loads;
};

/**
 * @class ScopedTimer
 */
template <typename Precision = std::chrono::microseconds>
class ScopedTimer {
	std::atomic<uint64_t>&                target_;
	std::chrono::steady_clock::time_point start_;
	bool                                  active_;

      public:
	explicit ScopedTimer(std::atomic<uint64_t>& target) :
	        target_(target), active_(telemetry_timers_enabled().load(std::memory_order_relaxed)) {
		if (active_) {
			start_ = std::chrono::steady_clock::now();
		}
	}

	~ScopedTimer() {
		if (active_) {
			auto end     = std::chrono::steady_clock::now();
			auto elapsed = std::chrono::duration_cast<Precision>(end - start_).count();
			target_.fetch_add(elapsed, std::memory_order_relaxed);
		}
	}

	ScopedTimer(const ScopedTimer&)            = delete;
	ScopedTimer& operator=(const ScopedTimer&) = delete;
};

inline double get_cpu_load() {
	static std::atomic<uint64_t> prev_total{0}, prev_idle{0};
	uint64_t                     total = 0, idle_all = 0;

#ifdef __APPLE__
	host_cpu_load_info_data_t cpu_info;
	mach_msg_type_number_t    count = HOST_CPU_LOAD_INFO_COUNT;

	if (host_statistics64(mach_host_self(), HOST_CPU_LOAD_INFO, (host_info64_t)&cpu_info, &count) == KERN_SUCCESS) {
		total    = cpu_info.cpu_ticks[CPU_STATE_USER] + cpu_info.cpu_ticks[CPU_STATE_NICE] + cpu_info.cpu_ticks[CPU_STATE_SYSTEM] + cpu_info.cpu_ticks[CPU_STATE_IDLE];
		idle_all = cpu_info.cpu_ticks[CPU_STATE_IDLE];
	}
#elif defined(__linux__)
	std::ifstream file("/proc/stat");
	std::string   label;
	uint64_t      user, nice, system, idle, iowait, irq, softirq, steal;

	if (file >> label >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal) {
		total    = user + nice + system + idle + iowait + irq + softirq + steal;
		idle_all = idle + iowait;
	}
#endif

	uint64_t last_t = prev_total.exchange(total);
	uint64_t last_i = prev_idle.exchange(idle_all);

	if (last_t == 0)
		return 0.0;

	uint64_t dt = total - last_t;
	uint64_t di = idle_all - last_i;

	return (dt > 0) ? (1.0 - static_cast<double>(di) / dt) : 0.0;
}

inline std::vector<double> get_core_loads() {
#ifdef __APPLE__
	static std::vector<uint32_t> prev_ticks;
	natural_t                    processor_count = 0;
	processor_info_array_t       cpu_info;
	mach_msg_type_number_t       count;

	if (host_processor_info(mach_host_self(), PROCESSOR_CPU_LOAD_INFO, &processor_count, &cpu_info, &count) != KERN_SUCCESS) {
		return {};
	}

	std::vector<double> loads;
	loads.reserve(processor_count);

	if (prev_ticks.size() != processor_count * CPU_STATE_MAX) {
		prev_ticks.resize(processor_count * CPU_STATE_MAX, 0);
	}

	for (unsigned i = 0; i < processor_count; ++i) {
		processor_cpu_load_info_t core_info = (processor_cpu_load_info_t)&cpu_info[i * CPU_STATE_MAX];

		uint64_t total = core_info->cpu_ticks[CPU_STATE_USER] + core_info->cpu_ticks[CPU_STATE_NICE] + core_info->cpu_ticks[CPU_STATE_SYSTEM] + core_info->cpu_ticks[CPU_STATE_IDLE];
		uint64_t idle  = core_info->cpu_ticks[CPU_STATE_IDLE];

		uint64_t p_total = prev_ticks[i * CPU_STATE_MAX + CPU_STATE_USER] + prev_ticks[i * CPU_STATE_MAX + CPU_STATE_NICE] + prev_ticks[i * CPU_STATE_MAX + CPU_STATE_SYSTEM] + prev_ticks[i * CPU_STATE_MAX + CPU_STATE_IDLE];
		uint64_t p_idle  = prev_ticks[i * CPU_STATE_MAX + CPU_STATE_IDLE];

		uint64_t dt = total - p_total;
		uint64_t di = idle - p_idle;

		loads.push_back((dt > 0) ? (1.0 - static_cast<double>(di) / dt) : 0.0);

		for (int state = 0; state < CPU_STATE_MAX; ++state) {
			prev_ticks[i * CPU_STATE_MAX + state] = core_info->cpu_ticks[state];
		}
	}

	vm_deallocate(mach_task_self(), (vm_address_t)cpu_info, count * sizeof(integer_t));
	return loads;

#elif defined(__linux__)
	static std::vector<std::pair<uint64_t, uint64_t>> prev_state;
	std::ifstream                                     file("/proc/stat");
	std::string                                       line;
	std::vector<double>                               loads;

	int core_idx = 0;
	while (std::getline(file, line)) {
		if (line.compare(0, 3, "cpu") == 0 && line.size() > 3 && std::isdigit(line[3])) {
			std::string label;
			uint64_t    user, nice, system, idle, iowait, irq, softirq, steal;

			size_t space_pos = line.find(' ');
			if (space_pos != std::string::npos) {
				std::string        values = line.substr(space_pos);
				std::istringstream iss(values);
				if (iss >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal) {
					uint64_t total    = user + nice + system + idle + iowait + irq + softirq + steal;
					uint64_t idle_all = idle + iowait;

					if (static_cast<size_t>(core_idx) >= prev_state.size()) {
						prev_state.push_back({0, 0});
					}

					uint64_t p_total = prev_state[core_idx].first;
					uint64_t p_idle  = prev_state[core_idx].second;

					uint64_t dt = total - p_total;
					uint64_t di = idle_all - p_idle;

					loads.push_back((dt > 0) ? (1.0 - static_cast<double>(di) / dt) : 0.0);

					prev_state[core_idx] = {total, idle_all};
					core_idx++;
				}
			}
		}
	}
	return loads;
#else
	return {};
#endif
}

inline uint64_t rss_bytes() {
#ifdef __APPLE__
	struct mach_task_basic_info info;
	mach_msg_type_number_t      infoCount = MACH_TASK_BASIC_INFO_COUNT;

	if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &infoCount) == KERN_SUCCESS) {
		return (uint64_t)info.resident_size;
	}
	return 0;
#elif defined(__linux__)
	std::ifstream f("/proc/self/statm");
	uint64_t      pages;

	if (f >> pages >> pages) {
		static long page_size = sysconf(_SC_PAGESIZE);
		return pages * static_cast<uint64_t>(page_size);
	}
	return 0;
#else
	return 0;
#endif
}

#include <chrono>
#include <cstdlib>
#include <format>
#include <string>

/**
 * @brief Retrieves an environment variable or returns a default value.
 */
inline std::string get_env(const std::string& key, const std::string& default_value) {
	const char* val = std::getenv(key.c_str());
	return val ? std::string(val) : default_value;
}

/**
 * @brief Retrieves a boolean environment variable or returns a default value.
 */
inline bool get_env_bool(const std::string& key, bool default_value) {
	const char* val = std::getenv(key.c_str());
	if (!val)
		return default_value;
	std::string s(val);
	for (auto& c : s)
		c = static_cast<char>(std::tolower(c));
	return s == "true" || s == "1" || s == "yes" || s == "on";
}

/**
 * @brief Gets a filesystem-safe datetime string (e.g., "20260324_120000").
 */
inline std::string get_current_datetime_str() {
	auto    now  = std::chrono::system_clock::now();
	auto    time = std::chrono::system_clock::to_time_t(now);
	std::tm tm;
	localtime_r(&time, &tm);
	return std::format("{:04d}{:02d}{:02d}_{:02d}{:02d}{:02d}",
	                   tm.tm_year + 1900,
	                   tm.tm_mon + 1,
	                   tm.tm_mday,
	                   tm.tm_hour,
	                   tm.tm_min,
	                   tm.tm_sec);
}

/**
 * @brief Gets a human-readable timestamp for logging.
 */
inline std::string get_timestamp_str() {
	auto    now  = std::chrono::system_clock::now();
	auto    ms   = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
	auto    time = std::chrono::system_clock::to_time_t(now);
	std::tm tm;
	localtime_r(&time, &tm);
	return std::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}",
	                   tm.tm_year + 1900,
	                   tm.tm_mon + 1,
	                   tm.tm_mday,
	                   tm.tm_hour,
	                   tm.tm_min,
	                   tm.tm_sec,
	                   ms.count());
}

#include <cstdlib>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string_view>
#include <unistd.h>

/**
 * @enum LogLevel
 * @brief Represents the severity level of a log message.
 */
enum class LogLevel : uint8_t {
	DEBUG,
	INFO,
	WARNING,
	ERROR,
	FATAL,
};

/**
 * @brief Converts a LogLevel to its corresponding string representation.
 * @param level The LogLevel.
 * @return A string view of the log level name.
 */
constexpr std::string_view log_level_to_string(LogLevel level) {
	switch (level) {
		case LogLevel::DEBUG:
			return "DEBUG";
		case LogLevel::INFO:
			return "INFO";
		case LogLevel::WARNING:
			return "WARNING";
		case LogLevel::ERROR:
			return "ERROR";
		case LogLevel::FATAL:
			return "FATAL";
	}
	return "UNKNOWN";
}

/**
 * @class Logger
 * @brief Thread-safe, node-aware logging utility.
 *
 * Supports writing logs to both the console and a specific file per node.
 */
class Logger {
      public:
	/**
	 * @brief Gets the singleton Logger instance.
	 * @return Reference to the Logger singleton.
	 */
	static Logger& instance() {
		static Logger instance;
		return instance;
	}

	/**
	 * @brief Initializes the Logger for this specific node.
	 * @param rank The MPI rank of the node.
	 * @param logs_dir_path The directory to store log files. If empty, uses HELL_LOGS_DIR env or "logs/<datetime>".
	 */
	void init(int rank, std::filesystem::path logs_dir_path = "") {
		{
			std::lock_guard lock(mtx_);
			rank_ = rank;

			if (logs_dir_path.empty()) {
				logs_dir_path = get_env("HELL_LOGS_DIR", "logs/" + get_current_datetime_str());
			}

			std::filesystem::create_directories(logs_dir_path);
			auto log_file_path = logs_dir_path / std::format("node_{:03d}.log", rank_);
			log_file_.open(log_file_path, std::ios::out | std::ios::trunc);

			if (!log_file_.is_open()) {
				std::cerr << "Failed to open log file: " << log_file_path << std::endl;
			}
		}
		log_internal(LogLevel::INFO, std::format("logger initialized on node {} with PID - {}", rank_, ::getpid()));
	}

	/**
	 * @brief Sets the minimum severity level to be logged.
	 * @param level The minimum LogLevel.
	 */
	void set_log_level(LogLevel level) {
		log_level_ = level;
	}

	/**
	 * @brief Toggles whether to duplicate logs to standard output.
	 * @param value True to duplicate to stdout, false otherwise.
	 */
	void set_log_to_console(bool value) {
		log_to_console_ = value;
	}

	/**
	 * @brief Logs a formatted message with a specific severity.
	 * @tparam Args The types of the formatting arguments.
	 * @param level The severity level.
	 * @param fmt_string The format string.
	 * @param args The format arguments.
	 */
	template <typename... Args>
	void log(LogLevel level, const std::format_string<Args...> fmt_string, Args&&... args) {
		if (level < log_level_)
			return;
		auto message = std::format(fmt_string, std::forward<Args>(args)...);
		log_internal(level, message);
	}

	/**
	 * @brief Logs a DEBUG level message.
	 */
	template <typename... Args>
	void debug(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::DEBUG, fmt_string, std::forward<Args>(args)...);
	}

	template <typename... Args>
	void info(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::INFO, fmt_string, std::forward<Args>(args)...);
	}

	template <typename... Args>
	void warning(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::WARNING, fmt_string, std::forward<Args>(args)...);
	}

	template <typename... Args>
	void error(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::ERROR, fmt_string, std::forward<Args>(args)...);
	}

	/**
	 * @brief Logs a FATAL level message.
	 */
	template <typename... Args>
	void fatal(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::FATAL, fmt_string, std::forward<Args>(args)...);
	}

	/**
	 * @brief Writes a distinct textual block into the log file.
	 * @param header The title string of the block.
	 * @param body The contents of the block.
	 */
	void write_block(std::string_view header, std::string_view body) {
		std::lock_guard lock(mtx_);
		auto            ts    = timestamp();
		auto            block = std::format(
                        "[{}] [N{:03d}]\n╔══ {} ══\n{}\n╚══ end ══\n",
                        ts,
                        rank_,
                        header,
                        body);
		if (log_file_.is_open()) {
			log_file_ << block;
			log_file_.flush();
		}
	}

      private:
	Logger() = default;

	std::string timestamp() const {
		return get_timestamp_str();
	}

	void log_internal(LogLevel level, const std::string& message) {
		std::lock_guard lock(mtx_);
		auto            line = std::format("[{}] [N{:03d}] [{}] {}\n", timestamp(), rank_, log_level_to_string(level), message);
		if (log_file_.is_open()) {
			log_file_ << line;
			log_file_.flush();
		}
		if (log_to_console_) {
			std::cout << line;
		}
	}

	std::mutex    mtx_;
	std::ofstream log_file_;
	int           rank_           = -1;
	LogLevel      log_level_      = LogLevel::DEBUG;
	bool          log_to_console_ = true;
};

/**
 * @brief Global convenience method for accessing the logger instance.
 * @return Reference to the Logger singleton.
 */
inline Logger& logger() {
	return Logger::instance();
}

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

/**
 * @class StageExecutor
 * @brief Pure compute wrapper for a pipeline stage.
 *
 * Reads data from its input queue, processes it via the stage function,
 * and calls the provided send callback to emit results. All I/O
 * (MPI sends, local queue pushes) is handled externally by NodeExecutor.
 */
class StageExecutor {
      public:
	using SendFn = std::function<void(std::vector<Payload>&&)>;
	using EosFn  = std::function<void()>;

	StageDescriptor            sd_;
	std::shared_ptr<StageBase> stage_;
	ConcurrentQueue<Message>*  input_queue_;
	StageMetrics               metrics;

	SendFn send_fn_;
	EosFn  eos_fn_;

	uint32_t batch_size_;

	StageExecutor(StageDescriptor sd, std::shared_ptr<StageBase> stage, ConcurrentQueue<Message>* input_queue, SendFn send_fn, EosFn eos_fn, uint32_t batch_size = 64) :
	        sd_(std::move(sd)), stage_(std::move(stage)),
	        input_queue_(input_queue),
	        send_fn_(std::move(send_fn)), eos_fn_(std::move(eos_fn)),
	        batch_size_(batch_size) {
		metrics.stage_id = sd_.id;
		send_buffer_.reserve(batch_size_);
		if ((sd_.type == StageType::FILTER || sd_.type == StageType::FARM)
		    && stage_->cache_size_kb.has_value()) {
			cache_capacity_bytes_ = static_cast<size_t>(*stage_->cache_size_kb)
			                       * 1024ULL;
		}
	}

	void run_stage() {
		switch (sd_.type) {
			case StageType::SOURCE:
				run_source();
				break;
			case StageType::FILTER:
				run_filter();
				break;
			case StageType::FARM:
				run_farm();
				break;
			case StageType::SINK:
				run_sink();
				break;
		}

		logger().debug("Stage {} fully complete", sd_.id);
	}

      private:
	struct CacheNode {
		uint64_t                              key;
		Payload                               value;
		size_t                                bytes;
		std::chrono::steady_clock::time_point created_at;
		std::chrono::steady_clock::time_point last_used_at;
		uint64_t                              hits;
	};

	std::vector<Payload> send_buffer_;
	std::list<CacheNode> cache_lru_;
	std::unordered_map<uint64_t, std::list<CacheNode>::iterator> cache_index_;
	std::mutex                                              cache_mutex_;
	size_t                                                  cache_capacity_bytes_ = 0;
	size_t                                                  cache_current_bytes_  = 0;

	[[nodiscard]] bool is_cache_enabled() const {
		return cache_capacity_bytes_ > 0;
	}

	bool should_invalidate_locked(
	        const CacheNode& node,
	        std::chrono::steady_clock::time_point now) const {
		if (stage_->cache_invalidation_fn.has_value()) {
			return (*stage_->cache_invalidation_fn)(
			        now,
			        node.created_at,
			        node.last_used_at,
			        node.hits,
			        node.bytes);
		}

		return now - node.last_used_at >= stage_->cache_ttl;
	}

	static uint64_t hash_payload_bytes(const std::vector<uint8_t>& data) {
		uint64_t hash = 14695981039346656037ULL;
		for (uint8_t byte : data) {
			hash ^= static_cast<uint64_t>(byte);
			hash *= 1099511628211ULL;
		}
		return hash;
	}

	std::optional<Payload> try_get_cached(uint64_t key) {
		std::scoped_lock lock(cache_mutex_);
		auto             it = cache_index_.find(key);
		if (it == cache_index_.end()) {
			return std::nullopt;
		}

		auto now = std::chrono::steady_clock::now();
		if (should_invalidate_locked(*it->second, now)) {
			cache_current_bytes_ -= it->second->bytes;
			cache_lru_.erase(it->second);
			cache_index_.erase(it);
			return std::nullopt;
		}

		it->second->last_used_at = now;
		it->second->hits += 1;
		cache_lru_.splice(cache_lru_.begin(), cache_lru_, it->second);
		return it->second->value;
	}

	void insert_cached(uint64_t key, Payload value, size_t bytes) {
		if (bytes > cache_capacity_bytes_) {
			return;
		}

		std::scoped_lock lock(cache_mutex_);
		auto             existing = cache_index_.find(key);
		if (existing != cache_index_.end()) {
			cache_current_bytes_ -= existing->second->bytes;
			cache_lru_.erase(existing->second);
			cache_index_.erase(existing);
		}

		while (!cache_lru_.empty() && cache_current_bytes_ + bytes > cache_capacity_bytes_) {
			auto& tail = cache_lru_.back();
			cache_current_bytes_ -= tail.bytes;
			cache_index_.erase(tail.key);
			cache_lru_.pop_back();
		}

		auto now = std::chrono::steady_clock::now();

		cache_lru_.push_front(CacheNode{.key = key,
		                               .value = std::move(value),
		                               .bytes = bytes,
		                               .created_at = now,
		                               .last_used_at = now,
		                               .hits = 1});
		cache_index_[key] = cache_lru_.begin();
		cache_current_bytes_ += bytes;
	}

	Payload execute_with_optional_cache(const Payload& input_item) {
		if (!is_cache_enabled()) {
			return stage_->execute(input_item);
		}

		std::vector<uint8_t> input_bytes = input_item.serialize();
		uint64_t             key         = hash_payload_bytes(input_bytes);

		auto cached = try_get_cached(key);
		if (cached.has_value()) {
			return std::move(*cached);
		}

		Payload output = stage_->execute(input_item);
		size_t  bytes  = input_bytes.size() + output.serialize().size();
		insert_cached(key, output, bytes);
		return output;
	}

	void flush_send_buffer() {
		if (send_buffer_.empty())
			return;
		send_fn_(std::move(send_buffer_));
		send_buffer_.clear();
		send_buffer_.reserve(batch_size_);
	}

	void send_to_next(Payload res) {
		metrics.items_sent.fetch_add(1, std::memory_order_relaxed);
		send_buffer_.push_back(std::move(res));
		if (send_buffer_.size() >= batch_size_) {
			flush_send_buffer();
		}
	}

	void send_eos_to_next() {
		flush_send_buffer();
		eos_fn_();
	}

	bool pop_data_or_drain_eos(Message& msg, uint32_t& eos_seen) {
		while (true) {
			{
				ScopedTimer timer(metrics.idle_time_us);
				if (!input_queue_->pop(msg))
					return false;
			}
			metrics.queue_depth.store(static_cast<uint32_t>(input_queue_->size()),
			                         std::memory_order_relaxed);
			if (!msg.eos) {
				metrics.items_received.fetch_add(1, std::memory_order_relaxed);
				return true;
			}
			++eos_seen;
			logger().debug("Stage {} EOS {}/{}", sd_.id, eos_seen, sd_.expected_eos_count);
			if (eos_seen >= sd_.expected_eos_count) {
				return false;
			}
		}
	}

	void run_source() {
		logger().debug("Stage {} SOURCE starting", sd_.id);
		uint64_t count = 0;
		while (true) {
			std::optional<Payload> item;
			{
				ScopedTimer timer(metrics.processing_time_us);
				item = stage_->generate();
			}
			if (!item.has_value())
				break;
			metrics.items_processed.fetch_add(1, std::memory_order_relaxed);
			send_to_next(std::move(*item));
			++count;
		}
		send_eos_to_next();
		logger().debug("Stage {} SOURCE done — {} items", sd_.id, count);
	}

	void run_filter() {
		logger().debug("Stage {} FILTER starting (expecting {} EOS)",
		               sd_.id,
		               sd_.expected_eos_count);
		Message msg;
		uint32_t eos_seen = 0;
		while (pop_data_or_drain_eos(msg, eos_seen)) {
			Payload item;
			{
				ScopedTimer timer(metrics.processing_time_us);
				item = execute_with_optional_cache(msg.payload);
			}
			metrics.items_processed.fetch_add(1, std::memory_order_relaxed);
			send_to_next(std::move(item));
		}
		send_eos_to_next();
		logger().debug("Stage {} FILTER done", sd_.id);
	}

	void run_farm() {
		logger().debug("Stage {} FARM starting with {} workers, expecting {} EOS",
		               sd_.id,
		               sd_.assigned_threads,
		               sd_.expected_eos_count);

		ConcurrentQueue<Message> results;
		std::atomic<uint32_t>    active_workers{sd_.assigned_threads};
		std::atomic<uint32_t>    total_eos_seen{0};
		metrics.active_workers.store(sd_.assigned_threads,
		                             std::memory_order_relaxed);

		std::vector<std::jthread> workers;
		for (uint32_t i = 0; i < sd_.assigned_threads; ++i) {
			workers.emplace_back([&, worker_id = i](std::stop_token) {
				logger().debug("Stage {} worker {} started", sd_.id, worker_id);
				Message msg;
				while (true) {
					bool got;
					{
						ScopedTimer timer(metrics.idle_time_us);
						got = input_queue_->pop(msg);
					}
					if (!got)
						break;
					metrics.queue_depth.store(static_cast<uint32_t>(input_queue_->size()),
					                         std::memory_order_relaxed);

					if (msg.eos) {
						uint32_t eos_count = total_eos_seen.fetch_add(
						                             1,
						                             std::memory_order_acq_rel)
						                     + 1;
						logger().debug("Stage {} worker {} saw EOS ({}/{})",
						               sd_.id,
						               worker_id,
						               eos_count,
						               sd_.expected_eos_count);
						if (eos_count < sd_.expected_eos_count) {
							continue;
						}
						for (uint32_t k = 1; k < sd_.assigned_threads; ++k) {
							input_queue_->push(Message{.payload = {}, .eos = true});
						}
						break;
					}
					metrics.items_received.fetch_add(1, std::memory_order_relaxed);

					Payload res;
					{
						ScopedTimer timer(metrics.processing_time_us);
						res = execute_with_optional_cache(msg.payload);
					}
					metrics.items_processed.fetch_add(1, std::memory_order_relaxed);
					results.push(Message{.payload = std::move(res)});
				}

				uint32_t remaining = active_workers.fetch_sub(
				                             1,
				                             std::memory_order_acq_rel)
				                     - 1;
				metrics.active_workers.store(remaining, std::memory_order_relaxed);
				logger().debug("Stage {} worker {} done ({} remaining)",
				               sd_.id,
				               worker_id,
				               remaining);

				if (remaining == 0) {
					results.push(Message{.payload = {}, .eos = true});
				}
			});
		}

		std::jthread sender([&](std::stop_token) {
			Message msg;
			while (results.pop(msg)) {
				if (msg.eos)
					break;
				send_to_next(std::move(msg.payload));
			}
			send_eos_to_next();
		});

		for (auto& w : workers)
			w.join();
		sender.join();
		logger().debug("Stage {} FARM done", sd_.id);
	}

	void run_sink() {
		logger().debug("Stage {} SINK starting (expecting {} EOS)",
		               sd_.id,
		               sd_.expected_eos_count);
		Message msg;
		uint32_t eos_seen = 0;
		while (pop_data_or_drain_eos(msg, eos_seen)) {
			{
				ScopedTimer timer(metrics.processing_time_us);
				stage_->consume(msg.payload);
			}
			metrics.items_processed.fetch_add(1, std::memory_order_relaxed);
		}
		logger().debug("Stage {} SINK done", sd_.id);
	}
};

#include <mpi.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

/**
 * @class NodeExecutor
 * @brief Orchestrates all stages assigned to a single MPI node.
 *
 * Owns all ConcurrentQueues, runs a single MPI receiver loop to demux
 * incoming messages by tag, and provides send/EOS methods that
 * transparently handle local-vs-remote routing.
 */
class NodeExecutor {
	int                                                                     rank_;
	uint32_t                                                                batch_size_;
	std::vector<StageDescriptor>                                            stage_descriptors_;
	std::vector<std::shared_ptr<StageBase>>                                 stage_ptrs_;
	std::unordered_map<uint32_t, std::unique_ptr<ConcurrentQueue<Message>>> queues_;
	std::vector<int>                                                        listen_tags_;
	std::vector<std::shared_ptr<StageExecutor>>                             executors_;
	std::atomic<bool>                                                       receiver_should_stop_{false};
	std::atomic<uint32_t>                                                   stages_finished_{0};
	std::unordered_map<uint32_t, StageMetrics*>                             tag_to_stage_metrics_;
	std::unordered_map<uint32_t, StageMetrics*>                             stage_id_to_metrics_;

      public:
	NodeExecutor(int rank, uint32_t batch_size = 64) :
	        rank_(rank), batch_size_(batch_size) {
	}

	/**
	 * @brief Add a stage to this node.
	 */
	void add_stage(const StageDescriptor& sd, std::shared_ptr<StageBase> stage_ptr) {
		stage_descriptors_.push_back(sd);
		stage_ptrs_.push_back(std::move(stage_ptr));
	}

	/**
	 * @brief Get all stage executors (for telemetry tracking).
	 */
	const std::vector<std::shared_ptr<StageExecutor>>& get_executors() const {
		return executors_;
	}

	/**
	 * @brief Main entry point: create queues, wire up executors, run everything.
	 */
	void run() {
		setup_queues();
		create_executors();
		run_all();
	}

      private:
	void setup_queues() {
		for (auto& sd : stage_descriptors_) {
			if (sd.input_tag != UINT32_MAX) {
				queues_[sd.input_tag] = std::make_unique<ConcurrentQueue<Message>>();

				uint32_t local_pred_count = 0;
				for (auto& other_sd : stage_descriptors_) {
					if (other_sd.id == sd.previous_stage_id) {
						++local_pred_count;
					}
				}

				const bool expects_remote_pred =
				        (sd.type != StageType::SOURCE) && (sd.expected_eos_count > local_pred_count);

				if (expects_remote_pred) {
					listen_tags_.push_back(static_cast<int>(sd.input_tag));
				}
			}
		}
	}

	void create_executors() {
		for (size_t i = 0; i < stage_descriptors_.size(); ++i) {
			auto& sd    = stage_descriptors_[i];
			auto& stage = stage_ptrs_[i];

			ConcurrentQueue<Message>* input_q = nullptr;
			if (sd.input_tag != UINT32_MAX) {
				input_q = queues_[sd.input_tag].get();
			}

			auto rr_counter = std::make_shared<std::atomic<uint32_t>>(0);

			auto send_fn = [this, sd, rr_counter](std::vector<Payload>&& batch) {
				send_batch(sd, *rr_counter, std::move(batch));
			};

			auto eos_fn = [this, sd]() {
				send_eos(sd);
				stages_finished_.fetch_add(1, std::memory_order_release);
			};

			auto executor = std::make_shared<StageExecutor>(
			        sd,
			        stage,
			        input_q,
			        std::move(send_fn),
			        std::move(eos_fn),
			        batch_size_);

			stage_id_to_metrics_[sd.id] = &executor->metrics;
			if (sd.input_tag != UINT32_MAX) {
				tag_to_stage_metrics_[sd.input_tag] = &executor->metrics;
			}
			executors_.push_back(std::move(executor));
		}
	}

	void run_all() {
		std::optional<std::jthread> receiver_thread;
		if (!listen_tags_.empty()) {
			receiver_thread.emplace([this](std::stop_token) {
				this->receiver_loop();
			});
		}

		std::vector<std::jthread> stage_threads;
		for (auto& exec : executors_) {
			stage_threads.emplace_back([exec](std::stop_token) {
				exec->run_stage();
			});
		}

		for (auto& t : stage_threads)
			t.join();

		logger().debug("All stages done on node {}", rank_);

		receiver_should_stop_.store(true, std::memory_order_release);
		if (receiver_thread) {
			receiver_thread->join();
		}

		logger().debug("NodeExecutor fully complete on node {}", rank_);
	}

	/**
	 * @brief Single MPI receiver loop: probes for any of our listen_tags,
	 * receives the message, and pushes it into the correct queue by tag.
	 */
	void receiver_loop() {
		logger().debug("Node {} MPI receiver starting, listening on {} tags",
		               rank_,
		               listen_tags_.size());

		while (!receiver_should_stop_.load(std::memory_order_acquire)) {
			bool received_any = false;

			for (int tag : listen_tags_) {
				StageMetrics* recv_metrics = nullptr;
				auto          metrics_it   = tag_to_stage_metrics_.find(static_cast<uint32_t>(tag));
				if (metrics_it != tag_to_stage_metrics_.end()) {
					recv_metrics = metrics_it->second;
				}

				int        flag = 0;
				MPI_Status status;
				MPI_Iprobe(MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &flag, &status);

				if (!flag)
					continue;

				received_any = true;
				int message_size;
				MPI_Get_count(&status, MPI_BYTE, &message_size);

				std::vector<uint8_t> buffer(message_size);
				auto                 recv_start = std::chrono::steady_clock::now();
				MPI_Recv(buffer.data(), message_size, MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				if (recv_metrics && telemetry_timers_enabled().load(std::memory_order_relaxed)) {
					auto recv_end = std::chrono::steady_clock::now();
					auto elapsed  = std::chrono::duration_cast<std::chrono::microseconds>(recv_end - recv_start).count();
					recv_metrics->mpi_recv_time_us.fetch_add(static_cast<uint64_t>(elapsed), std::memory_order_relaxed);
				}

				process_received_message(buffer, message_size, tag);
			}

			if (!received_any) {
				std::this_thread::yield();
			}
		}

		logger().debug("Node {} MPI receiver exiting", rank_);
	}

	void process_received_message(const std::vector<uint8_t>& buffer, int buffer_size, int tag) {
		auto it = queues_.find(static_cast<uint32_t>(tag));
		if (it == queues_.end()) {
			logger().error("Node {} received message for unknown tag {}", rank_, tag);
			return;
		}

		auto* queue = it->second.get();
		auto  metrics_it = tag_to_stage_metrics_.find(static_cast<uint32_t>(tag));
		StageMetrics* stage_metrics = metrics_it != tag_to_stage_metrics_.end() ? metrics_it->second : nullptr;

		if (stage_metrics && buffer_size > 0) {
			stage_metrics->bytes_received.fetch_add(static_cast<uint64_t>(buffer_size), std::memory_order_relaxed);
		}

		if (buffer_size == 0) {
			queue->push(Message{.payload = {}, .eos = true});
			if (stage_metrics) {
				stage_metrics->queue_depth.store(static_cast<uint32_t>(queue->size()),
				                               std::memory_order_relaxed);
			}
			logger().debug("Node {} received REMOTE EOS on tag {}", rank_, tag);
		} else {
			std::vector<Payload> payloads = deserialize_batch(buffer);
			for (auto& pl : payloads) {
				queue->push(Message{.payload = std::move(pl)});
			}
			if (stage_metrics) {
				stage_metrics->queue_depth.store(static_cast<uint32_t>(queue->size()),
				                               std::memory_order_relaxed);
			}
		}
	}

	/**
	 * @brief Send a batch of payloads to the next stage(s).
	 *
	 * Uses round-robin across output_tags when there are multiple downstream instances.
	 */
	void send_batch(const StageDescriptor& sd, std::atomic<uint32_t>& rr_counter, std::vector<Payload>&& batch) {
		if (sd.output_tags.empty() || batch.empty())
			return;

		if (sd.output_tags.size() == 1) {
			send_to_target(sd, sd.output_tags[0], sd.output_ranks[0], batch);
		} else {
			std::vector<std::vector<Payload>> per_target(sd.output_tags.size());
			for (auto& payload : batch) {
				uint32_t idx = rr_counter.fetch_add(1, std::memory_order_relaxed)
			             % sd.output_tags.size();
				per_target[idx].push_back(std::move(payload));
			}

			for (size_t i = 0; i < per_target.size(); ++i) {
				if (!per_target[i].empty()) {
					send_to_target(sd, sd.output_tags[i], sd.output_ranks[i], per_target[i]);
				}
			}
		}
	}

	/**
	 * @brief Send data to a specific target tag/rank. If the target is local, push
	 * directly into the queue. Otherwise, serialize and MPI_Send.
	 */
	void send_to_target(const StageDescriptor& sd, uint32_t tag, uint32_t dest_rank, const std::vector<Payload>& payloads) {
		StageMetrics* sender_metrics = nullptr;
		auto          sender_it      = stage_id_to_metrics_.find(sd.id);
		if (sender_it != stage_id_to_metrics_.end()) {
			sender_metrics = sender_it->second;
		}

		// Check if the destination is local (we own the queue for that tag)
		auto it = queues_.find(tag);
		if (it != queues_.end()) {
			for (auto& pl : payloads) {
				it->second->push(Message{.payload = pl});
			}
			auto target_it = tag_to_stage_metrics_.find(tag);
			if (target_it != tag_to_stage_metrics_.end()) {
				target_it->second->queue_depth.store(static_cast<uint32_t>(it->second->size()),
				                                    std::memory_order_relaxed);
			}
		} else {
			std::vector<uint8_t> buffer = serialize_batch(payloads);
			if (sender_metrics) {
				sender_metrics->bytes_sent.fetch_add(static_cast<uint64_t>(buffer.size()), std::memory_order_relaxed);
			}
			auto send_start = std::chrono::steady_clock::now();
			MPI_Send(buffer.data(), static_cast<int>(buffer.size()), MPI_BYTE, static_cast<int>(dest_rank), static_cast<int>(tag), MPI_COMM_WORLD);
			if (sender_metrics && telemetry_timers_enabled().load(std::memory_order_relaxed)) {
				auto send_end = std::chrono::steady_clock::now();
				auto elapsed  = std::chrono::duration_cast<std::chrono::microseconds>(send_end - send_start).count();
				sender_metrics->mpi_send_time_us.fetch_add(static_cast<uint64_t>(elapsed), std::memory_order_relaxed);
			}
		}
	}

	/**
	 * @brief Send EOS to all downstream instances of this stage.
	 */
	void send_eos(const StageDescriptor& sd) {
		for (size_t i = 0; i < sd.output_tags.size(); ++i) {
			uint32_t tag  = sd.output_tags[i];
			uint32_t rank = sd.output_ranks[i];

			auto it = queues_.find(tag);
			if (it != queues_.end()) {
				it->second->push(Message{.payload = {}, .eos = true});
				logger().debug("Stage {} sent LOCAL EOS on tag {}", sd.id, tag);
			} else {
				MPI_Send(nullptr, 0, MPI_BYTE, static_cast<int>(rank), static_cast<int>(tag), MPI_COMM_WORLD);
				logger().debug("Stage {} sent REMOTE EOS to rank {} on tag {}",
				               sd.id,
				               rank,
				               tag);
			}
		}
	}
};

#include <memory>
#include <mpi.h>
#include <unordered_map>

#include <format>

/**
 * @brief Returns the string representation of a StageType.
 */
inline std::string stage_type_name(StageType type) {
	switch (type) {
		case StageType::SOURCE:
			return "SOURCE";
		case StageType::FILTER:
			return "FILTER";
		case StageType::SINK:
			return "SINK";
		case StageType::FARM:
			return "FARM";
	}
	return "UNKNOWN";
}

/**
 * @brief Generates a formatted string representing the cluster configuration.
 */
inline std::string cluster_config_view(int world_size, const std::vector<int>& cores_per_node) {
	std::string out;
	out += std::format("Cluster: {} nodes\n", world_size);
	out += "┌──────┬───────────┐\n";
	out += "│ Node │ HW Cores  │\n";
	out += "├──────┼───────────┤\n";
	for (int i = 0; i < world_size; ++i) {
		out += std::format("│ {:>4} │ {:>9} │\n", i, cores_per_node[i]);
	}
	out += "└──────┴───────────┘\n";
	return out;
}

/**
 * @brief Generates a formatted string representing the execution plan.
 */
inline std::string plan_view(const WorkflowPlan& plan) {
	std::string out;
	out += std::format("Workflow: {} descriptors\n", plan.num_stages);
	out += "┌────────┬────────────┬──────┬─────────┬───────────┬──────────────────┬──────┐\n";
	out += "│ Stage  │ Type       │ Node │ Threads │ Input Tag │ Output Tags      │ EOS  │\n";
	out += "├────────┼────────────┼──────┼─────────┼───────────┼──────────────────┼──────┤\n";
	for (auto& stage : plan.stages) {
		auto out_tags_str = stage.output_tags.empty() ? std::string("-") : ([&] {
			std::string res;
			for (size_t i = 0; i < stage.output_tags.size(); ++i) {
				if (i > 0) res += ",";
				res += std::format("{}", stage.output_tags[i]);
			}
			return res;
		}());
		auto input_tag_str = (stage.input_tag == UINT32_MAX) ? std::string("-") : std::format("{}", stage.input_tag);
		out += std::format("│ {:>6} │ {:<10} │ {:>4} │ {:>7} │ {:>9} │ {:<16} │ {:>4} │\n",
		                   stage.id,
		                   stage_type_name(stage.type),
		                   stage.assigned_node,
		                   stage.assigned_threads,
		                   input_tag_str,
		                   out_tags_str,
		                   stage.expected_eos_count);
	}
	out += "└────────┴────────────┴──────┴─────────┴───────────┴──────────────────┴──────┘\n";
	return out;
}

/**
 * @brief Generates a formatted string representing NodeMetrics.
 */
inline std::string node_metrics_view(const NodeMetrics& nm) {
	std::string out;
	out += std::format("Node {} — CPU: {:.1f}% — RSS: {:.1f} MB — HW threads: {}\n",
	                   nm.rank,
	                   nm.cpu_load * 100.0,
	                   nm.rss_bytes / (1024.0 * 1024.0),
	                   nm.hw_threads);

	if (nm.stages.empty()) {
		out += "  (no stages assigned)\n";
		return out;
	}

	out += "  ┌───────┬────────────┬────────────┬────────────┬────────────┬──────────┬───────┐\n";
	out += "  │ Stage │  Processed │   Received │       Sent │ Compute μs │  Idle μs │ Queue │\n";
	out += "  ├───────┼────────────┼────────────┼────────────┼────────────┼──────────┼───────┤\n";
	for (auto& s : nm.stages) {
		out += std::format(
		        "  │ {:>5} │ {:>10} │ {:>10} │ {:>10} │ {:>10} │ {:>8} │ {:>5} │\n",
		        s.stage_id,
		        s.items_processed,
		        s.items_received,
		        s.items_sent,
		        s.processing_time_us,
		        s.idle_time_us,
		        s.queue_depth);
	}
	out += "  └───────┴────────────┴────────────┴────────────┴────────────┴──────────┴───────┘\n";
	return out;
}

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <format>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <mpi.h>

constexpr int MONITOR_TAG      = 9999;
constexpr int MONITOR_DONE_TAG = 9998;

/**
 * @class NodeReporter
 * @brief Periodically collects and reports node-local metrics to the monitor.
 */
class NodeReporter {
	int                        rank_;
	std::vector<StageMetrics*> tracked_stages_;
	std::atomic<bool>          running_{false};
	std::jthread               reporter_thread_;

      public:
	explicit NodeReporter(int rank) :
	        rank_(rank) {
	}

	void track_stage(StageMetrics* stage) {
		tracked_stages_.push_back(stage);
	}

	void start(std::chrono::milliseconds interval = std::chrono::milliseconds{500}) {
		running_         = true;
		reporter_thread_ = std::jthread([this, interval](std::stop_token token) {
			while (!token.stop_requested() && running_) {
				send_report();
				std::this_thread::sleep_for(interval);
			}
			send_report();
			send_done_signal();
		});
	}

	void stop() {
		running_ = false;
		if (reporter_thread_.joinable()) {
			reporter_thread_.request_stop();
			reporter_thread_.join();
		}
	}

      private:
	void send_report() {
		NodeMetrics nm;
		nm.rank       = rank_;
		nm.hw_threads = std::thread::hardware_concurrency();
		nm.cpu_load   = get_cpu_load();
		nm.rss_bytes  = rss_bytes();
		nm.core_loads = get_core_loads();
		for (auto* stage : tracked_stages_) {
			nm.stages.push_back(stage->snapshot());
		}

		auto buf = serialize_node_metrics(nm);
		MPI_Send(buf.data(), static_cast<int>(buf.size()), MPI_BYTE, 0, MONITOR_TAG, MPI_COMM_WORLD);
	}

	void send_done_signal() {
		MPI_Send(nullptr, 0, MPI_BYTE, 0, MONITOR_DONE_TAG, MPI_COMM_WORLD);
	}

	static std::vector<uint8_t> serialize_node_metrics(const NodeMetrics& nm) {
		std::vector<uint8_t> buf;
		auto                 push = [&](const auto& v) {
			auto p = reinterpret_cast<const uint8_t*>(&v);
			buf.insert(buf.end(), p, p + sizeof(v));
		};

		push(nm.rank);
		push(nm.hw_threads);
		push(nm.cpu_load);
		push(nm.rss_bytes);
		uint32_t stages_size = nm.stages.size();
		push(stages_size);
		for (auto& s : nm.stages) {
			auto p = reinterpret_cast<const uint8_t*>(&s);
			buf.insert(buf.end(), p, p + sizeof(s));
		}

		uint32_t core_loads_size = nm.core_loads.size();
		push(core_loads_size);
		for (auto& load : nm.core_loads) {
			push(load);
		}

		return buf;
	}

      public:
	static NodeMetrics deserialize_node_metrics(const uint8_t* buf) {
		NodeMetrics nm;
		size_t      offset = 0;
		auto        pull   = [&](auto& v) {
			std::memcpy(&v, buf + offset, sizeof(v));
			offset += sizeof(v);
		};

		pull(nm.rank);
		pull(nm.hw_threads);
		pull(nm.cpu_load);
		pull(nm.rss_bytes);
		uint32_t stages_size;
		pull(stages_size);
		nm.stages.resize(stages_size);
		for (auto& s : nm.stages) {
			std::memcpy(&s, buf + offset, sizeof(s));
			offset += sizeof(s);
		}

		uint32_t core_loads_size;
		pull(core_loads_size);
		nm.core_loads.resize(core_loads_size);
		for (auto& load : nm.core_loads) {
			pull(load);
		}

		return nm;
	}
};

inline int get_telemetry_port() {
	return std::stoi(get_env("HELL_TELEMETRY_PORT", "9100"));
}

const int TELEMETRY_DEFAULT_PORT = get_telemetry_port();

/**
 * @class MonitorCollector
 * @brief Aggregates telemetry data from all nodes and serves it via UDP.
 */
class MonitorCollector {
	int                   world_size_;
	std::atomic<bool>     running_{false};
	std::jthread          collector_thread_;
	std::filesystem::path output_dir_;

	std::vector<NodeMetrics> latest_;
	std::mutex               mtx_;

	int                udp_sock_ = -1;
	struct sockaddr_in dest_addr_;

      public:
	explicit MonitorCollector(int                   world_size,
	                          std::filesystem::path output_dir     = "",
	                          int                   telemetry_port = -1) :
	        world_size_(world_size), latest_(world_size) {
		if (output_dir.empty()) {
			output_dir_ = get_env("HELL_METRICS_DIR", "metrics/" + get_current_datetime_str());
		} else {
			output_dir_ = std::move(output_dir);
		}

		if (telemetry_port == -1) {
			telemetry_port = TELEMETRY_DEFAULT_PORT;
		}

		std::filesystem::create_directories(output_dir_);

		udp_sock_ = ::socket(AF_INET, SOCK_DGRAM, 0);
		if (udp_sock_ >= 0) {
			std::memset(&dest_addr_, 0, sizeof(dest_addr_));
			dest_addr_.sin_family = AF_INET;
			dest_addr_.sin_port   = htons(static_cast<uint16_t>(telemetry_port));
			inet_pton(AF_INET, "127.0.0.1", &dest_addr_.sin_addr);
		}
	}

	void start() {
		running_          = true;
		collector_thread_ = std::jthread([this](std::stop_token /*token*/) {
			int done_count = 0;

			while (done_count < world_size_) {
				receive_metric_if_available();

				int        flag = 0;
				MPI_Status status;
				MPI_Iprobe(MPI_ANY_SOURCE, MONITOR_DONE_TAG, MPI_COMM_WORLD, &flag, &status);
				if (flag) {
					MPI_Recv(nullptr, 0, MPI_BYTE, status.MPI_SOURCE, MONITOR_DONE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					++done_count;
					logger().debug("Monitor: reporter done from rank {} ({}/{})",
					               status.MPI_SOURCE,
					               done_count,
					               world_size_);
				}

				if (!flag) {
					std::this_thread::sleep_for(std::chrono::milliseconds{10});
				}
			}

			drain_remaining_metrics();

			logger().debug("Monitor collector: all {} reporters done", world_size_);
		});
	}

	void stop() {
		running_ = false;
		if (collector_thread_.joinable()) {
			collector_thread_.join();
		}
		write_final_summary();
		if (udp_sock_ >= 0) {
			::close(udp_sock_);
			udp_sock_ = -1;
		}
	}

	std::vector<NodeMetrics> current_state() {
		std::lock_guard lock(mtx_);
		return latest_;
	}

      private:
	void receive_metric_if_available() {
		int        flag = 0;
		MPI_Status status;

		MPI_Iprobe(MPI_ANY_SOURCE, MONITOR_TAG, MPI_COMM_WORLD, &flag, &status);
		if (!flag)
			return;

		int count;
		MPI_Get_count(&status, MPI_BYTE, &count);
		std::vector<uint8_t> buf(count);
		MPI_Recv(buf.data(), count, MPI_BYTE, status.MPI_SOURCE, MONITOR_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		auto nm = NodeReporter::deserialize_node_metrics(buf.data());

		{
			std::lock_guard lock(mtx_);
			if (nm.rank >= 0 && nm.rank < world_size_)
				latest_[nm.rank] = nm;
		}

		logger().debug("Monitor ← Node {}: CPU {:.1f}%",
		               nm.rank,
		               nm.cpu_load * 100.0);

		send_live_json();
	}

	void drain_remaining_metrics() {
		while (true) {
			int        flag = 0;
			MPI_Status status;
			MPI_Iprobe(MPI_ANY_SOURCE, MONITOR_TAG, MPI_COMM_WORLD, &flag, &status);
			if (!flag)
				break;

			int count;
			MPI_Get_count(&status, MPI_BYTE, &count);
			std::vector<uint8_t> buf(count);
			MPI_Recv(buf.data(), count, MPI_BYTE, status.MPI_SOURCE, MONITOR_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			auto nm = NodeReporter::deserialize_node_metrics(buf.data());
			{
				std::lock_guard lock(mtx_);
				if (nm.rank >= 0 && nm.rank < world_size_)
					latest_[nm.rank] = nm;
			}
		}
	}

	void send_live_json() {
		if (udp_sock_ < 0)
			return;

		std::lock_guard lock(mtx_);

		std::ostringstream f;

		f << "{\"timestamp\":\"" << get_timestamp_str() << "\",\"nodes\":[";

		for (int i = 0; i < world_size_; ++i) {
			auto& nm = latest_[i];

			f << "{"
			  << "\"rank\":" << i << ","
			  << "\"hw_threads\":" << nm.hw_threads << ","
			  << "\"cpu_load\":" << nm.cpu_load << ","
			  << "\"rss_bytes\":" << nm.rss_bytes << ","
			  << "\"stages\":[";

			for (size_t j = 0; j < nm.stages.size(); ++j) {
				auto& s = nm.stages[j];

				f << "{"
				  << "\"stage_id\":" << s.stage_id << ","
				  << "\"items_processed\":" << s.items_processed << ","
				  << "\"items_received\":" << s.items_received << ","
				  << "\"items_sent\":" << s.items_sent << ","
				  << "\"bytes_received\":" << s.bytes_received << ","
				  << "\"bytes_sent\":" << s.bytes_sent << ","
				  << "\"processing_time_us\":" << s.processing_time_us << ","
				  << "\"idle_time_us\":" << s.idle_time_us << ","
				  << "\"mpi_send_time_us\":" << s.mpi_send_time_us << ","
				  << "\"mpi_recv_time_us\":" << s.mpi_recv_time_us << ","
				  << "\"active_workers\":" << s.active_workers << ","
				  << "\"queue_depth\":" << s.queue_depth
				  << "}";

				if (j + 1 < nm.stages.size())
					f << ",";
			}

			f << "]";

			if (nm.core_loads.size() > 0) {
				f << ",\"core_loads\":[";
				for (size_t j = 0; j < nm.core_loads.size(); ++j) {
					f << nm.core_loads[j];
					if (j + 1 < nm.core_loads.size())
						f << ",";
				}
				f << "]";
			} else {
				f << ",\"core_loads\":[]";
			}

			f << "}";

			if (i + 1 < world_size_)
				f << ",";
		}

		f << "]}";

		auto msg = f.str();
		::sendto(udp_sock_, msg.data(), msg.size(), 0, reinterpret_cast<const struct sockaddr*>(&dest_addr_), sizeof(dest_addr_));
	}

	void write_final_summary() {
		std::lock_guard lock(mtx_);

		std::string summary;
		summary += "\n╔═══════════════════════════════════════╗\n";
		summary += "║       FINAL EXECUTION SUMMARY         ║\n";
		summary += "╚═══════════════════════════════════════╝\n\n";

		for (auto& nm : latest_) {
			summary += node_metrics_view(nm);
			summary += "\n";
		}

		uint64_t total_items = 0, total_compute = 0, total_idle = 0;
		uint64_t total_bytes_sent = 0, total_bytes_recv = 0;
		for (auto& nm : latest_) {
			for (auto& s : nm.stages) {
				total_items += s.items_processed;
				total_compute += s.processing_time_us;
				total_idle += s.idle_time_us;
				total_bytes_sent += s.bytes_sent;
				total_bytes_recv += s.bytes_received;
			}
		}

		double efficiency = (total_compute + total_idle) > 0 ? (double)total_compute / (double)(total_compute + total_idle) * 100.0 : 0.0;

		summary += std::format("Total items processed:  {}\n", total_items);
		summary += std::format("Total compute time:     {} μs\n", total_compute);
		summary += std::format("Total idle time:        {} μs\n", total_idle);
		summary += std::format("Compute efficiency:     {:.1f}%\n", efficiency);
		summary += std::format("Total MPI sent:         {:.2f} MB\n",
		                       total_bytes_sent / (1024.0 * 1024.0));
		summary += std::format("Total MPI received:     {:.2f} MB\n",
		                       total_bytes_recv / (1024.0 * 1024.0));

		logger().write_block("FINAL SUMMARY", summary);

		auto          path = output_dir_ / "summary.txt";
		std::ofstream f(path);
		f << summary;
	}
};

#include <thread>

/**
 * @class Engine
 * @brief Main execution engine for the workflow pipeline.
 *
 * The Engine class is responsible for taking a constructed Pipeline,
 * planning its execution across the available MPI cluster, and executing
 * the assigned stages on the local node via a NodeExecutor.
 */
class Engine {
	Pipeline<void, void> pipeline_;
	uint32_t             batch_size_ = 64;

      public:
	/**
	 * @brief Sets the workflow pipeline to be executed.
	 * @param pipeline The pipeline to execute.
	 */
	void set_workflow(Pipeline<void, void> pipeline) {
		pipeline_ = std::move(pipeline);
	}

	/**
	 * @brief Sets the batch size for MPI cross-node communication.
	 *
	 * Multiple payloads are accumulated and sent as a single MPI message
	 * to amortize per-message overhead. Default is 64.
	 *
	 * @param n Number of payloads per batch.
	 */
	void set_batch_size(uint32_t n) {
		batch_size_ = n;
	}

	/**
	 * @brief Executes the pipeline on the MPI cluster.
	 *
	 * Initializes the logger, validates MPI thread support, gathers cluster
	 * configuration, generates and broadcasts the execution plan, and creates
	 * a NodeExecutor for the local node to run the assigned stages.
	 */
	void execute() {
		int rank, world_size;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		MPI_Comm_size(MPI_COMM_WORLD, &world_size);

		logger().init(rank);
		logger().set_log_level(LogLevel::DEBUG);
		logger().set_log_to_console(true);
		logger().debug("Engine starting on node {}", rank);

		int provided;
		MPI_Query_thread(&provided);
		if (provided < MPI_THREAD_MULTIPLE) {
			logger().error("MPI_THREAD_MULTIPLE required");
			MPI_Abort(MPI_COMM_WORLD, 1);
		}

		int              core_num = std::thread::hardware_concurrency();
		std::vector<int> cores_per_node(world_size);
		MPI_Allgather(&core_num, 1, MPI_INT, cores_per_node.data(), 1, MPI_INT, MPI_COMM_WORLD);

		logger().write_block("CLUSTER CONFIG",
		                     cluster_config_view(world_size, cores_per_node));

		WorkflowPlan wp;
		if (rank == 0) {
			wp = Planner::plan(pipeline_, world_size, cores_per_node);
		}
		PlanSerializer::broadcast_plan(wp, 0, MPI_COMM_WORLD);

		logger().write_block("WORKFLOW PLAN", plan_view(wp));

		NodeExecutor node_executor(rank, batch_size_);

		for (auto& sd : wp.stages) {
			if (static_cast<int>(sd.assigned_node) != rank)
				continue;

			auto stage_ptr = pipeline_.stages_[sd.id];
			node_executor.add_stage(sd, stage_ptr);
		}

		bool telemetry_enabled = get_env_bool("HELL_TELEMETRY_ENABLED", false);
		telemetry_timers_enabled().store(telemetry_enabled, std::memory_order_relaxed);

		std::unique_ptr<MonitorCollector> collector;
		if (rank == 0 && telemetry_enabled) {
			collector = std::make_unique<MonitorCollector>(world_size);
			collector->start();
			logger().debug("Monitor collector started");
		}

		node_executor.run();

		NodeReporter reporter(rank);
		if (telemetry_enabled) {
			for (auto& exec : node_executor.get_executors()) {
				reporter.track_stage(&exec->metrics);
			}
			reporter.start();
			std::this_thread::sleep_for(std::chrono::milliseconds{100});
			reporter.stop();
			logger().debug("Reporter stopped on node {}", rank);
		}

		if (rank == 0 && collector) {
			collector->stop();
			logger().debug("Monitor collector stopped");
		}

		MPI_Barrier(MPI_COMM_WORLD);
		logger().debug("Engine finished on node {}", rank);
	}
};

