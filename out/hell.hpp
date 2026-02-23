#pragma once

/**
 * \file
 * \brief Umbrella include for the public H.E.L.L. API.
 */

#if !__has_include(<mpi.h>)
#error "H.E.L.L. requires MPI. Install an MPI implementation (OpenMPI/MPICH) and compile with an MPI toolchain (e.g. mpic++)."
#endif

#include <mpi.h>

#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <stop_token>

namespace hell::transport {

/**
 * \brief Bounded blocking queue with close semantics and stop-token aware waits.
 * \tparam T Stored item type.
 */
template <typename T>
class ConcurrentQueue {
      public:
	explicit ConcurrentQueue(size_t max_size = 1024) : max_size_(max_size) {
	}

	/**
	 * \brief Pushes an item into the queue.
	 *
	 * \param item The item to push.
	 * \param st A stop token that can be used to request a stop.
	 * \return True if the item was pushed, false if the queue is full or closed.
	 */
	bool push(T item, std::stop_token st = {}) {
		std::unique_lock lock(mutex_);
		not_full_.wait(lock, st, [&] {
			return queue_.size() < max_size_ || closed_;
		});
		if (st.stop_requested() || closed_)
			return false;
		queue_.push(std::move(item));
		not_empty_.notify_one();
		return true;
	}

	/**
	 * \brief Pops an item from the queue.
	 *
	 * \param st A stop token that can be used to request a stop.
	 * \return An optional containing the item if it was popped, or nullopt if the queue is empty or closed.
	 */
	std::optional<T> pop(std::stop_token st = {}) {
		std::unique_lock lock(mutex_);
		not_empty_.wait(lock, st, [&] {
			return !queue_.empty() || closed_;
		});
		if (queue_.empty())
			return std::nullopt;
		T item = std::move(queue_.front());
		queue_.pop();
		not_full_.notify_one();
		return item;
	}

	/**
	 * \brief Closes the queue.
	 */
	void close() {
		{
			std::lock_guard lock(mutex_);
			closed_ = true;
		}
		not_empty_.notify_all();
		not_full_.notify_all();
	}

	/**
	 * \brief Checks if the queue is closed.
	 *
	 * \return True if the queue is closed, false otherwise.
	 */
	bool is_closed() const {
		std::lock_guard lock(mutex_);
		return closed_;
	}

	/**
	 * \brief Checks if the queue is empty.
	 * \return True if the queue is empty, false otherwise.
	 */
	bool is_empty() const {
		std::lock_guard lock(mutex_);
		return queue_.empty();
	}

      private:
	mutable std::mutex          mutex_;
	std::condition_variable_any not_empty_;
	std::condition_variable_any not_full_;
	std::queue<T>               queue_;
	size_t                      max_size_;
	bool                        closed_ = false;
};

} // namespace hell::transport

#include <span>
#include <vector>
#include <cstring>
#include <string>
#include <concepts>
#include <type_traits>

namespace hell::transport {

/**
 * \brief Raw byte container used as serialized transport payload.
 */
using ByteBuffer = std::vector<std::byte>;

/**
 * \brief A concept for types that can be sent over the network as is.
 */
template <typename T>
concept TriviallySendable = std::is_trivially_copyable_v<T>;

/**
 * \brief this class is used to serialize data to be sent over the network. after reading from the archive call ok() to check if the archive has failed.
 */
class Archive {
      public:
	/**
	 * \brief Writes a value to the archive.
	 * \param val The value to write.
	 */
	template <TriviallySendable T>
	void write(const T& val) {
		auto ptr = reinterpret_cast<const std::byte*>(&val);
		buffer_.insert(buffer_.end(), ptr, ptr + sizeof(T));
	}

	/**
	 * \brief Writes a sequence of bytes to the archive.
	 * \param data The data to write.
	 * \param len The length of the data to write.
	 */
	void write_bytes(const void* data, size_t len) {
		auto ptr = static_cast<const std::byte*>(data);
		buffer_.insert(buffer_.end(), ptr, ptr + len);
	}

	/**
	 * \brief Reads a value from the archive.
	 * \return The value read from the archive.
	 */
	template <TriviallySendable T>
	T read() {
		if (failed_ || read_pos_ + sizeof(T) > buffer_.size()) {
			failed_ = true;
			return {};
		}
		T val;
		std::memcpy(&val, buffer_.data() + read_pos_, sizeof(T));
		read_pos_ += sizeof(T);
		return val;
	}

	/**
	 * \brief Reads a sequence of bytes from the archive.
	 * \param dest The destination buffer to write the bytes to.
	 * \param len The length of the bytes to read.
	 */
	void read_bytes(void* dest, size_t len) {
		if (failed_ || read_pos_ + len > buffer_.size()) {
			failed_ = true;
			return;
		}
		std::memcpy(dest, buffer_.data() + read_pos_, len);
		read_pos_ += len;
	}

	/**
	 * \brief Returns the buffer containing the serialized data.
	 * \return The buffer containing the serialized data.
	 */
	ByteBuffer& buffer() {
		return buffer_;
	}

	/**
	 * \brief Returns the buffer containing the serialized data.
	 * \return The buffer containing the serialized data.
	 */
	std::span<const std::byte> view() const {
		return std::span<const std::byte>{buffer_.data() + read_pos_, buffer_.size() - read_pos_};
	}

	/**
	 * \brief Returns the full buffer containing the serialized data.
	 * \return The full buffer containing the serialized data.
	 */
	std::span<const std::byte> view_full() const {
		return std::span<const std::byte>{buffer_.data(), buffer_.size()};
	}

	/**
	 * \brief Sets the buffer containing the serialized data.
	 * \param buf The buffer containing the serialized data.
	 */
	void set_buffer(ByteBuffer buf) {
		buffer_   = std::move(buf);
		read_pos_ = 0;
	}

	/**
	 * \brief Checks if the archive has failed.
	 * \return True if the archive has failed, false otherwise.
	 */
	bool ok() {
		return !failed_;
	}

	/**
	 * \brief Resets the archive.
	 */
	void reset_archive() {
		buffer_.clear();
		read_pos_ = 0;
		failed_   = false;
	}

      private:
	ByteBuffer buffer_;
	size_t     read_pos_ = 0;
	bool       failed_   = false;
};

/**
 * \brief A concept for types that can be serialized using an archive.
 */
template <typename T>
concept Serializable = requires(T val, Archive& ar) {
	{ val.serialize(ar) };
	{ T::deserialize(ar) } -> std::same_as<T>;
} || TriviallySendable<T>;

/**
 * \brief Serializes a value to a byte buffer.
 * \param val The value to serialize.
 * \return The serialized value.
 */
template <Serializable T>
ByteBuffer serialize(const T& val) {
	Archive ar;
	if constexpr (TriviallySendable<T>) {
		ar.write(val);
	} else {
		val.serialize(ar);
	}
	return std::move(ar.buffer());
}

/**
 * \brief Deserializes a value from a byte buffer.
 * \param buf The byte buffer to deserialize from.
 * \return The deserialized value.
 */
template <Serializable T>
T deserialize(ByteBuffer buf) {
	Archive ar;
	ar.set_buffer(std::move(buf));
	T value;
	if constexpr (TriviallySendable<T>) {
		value = ar.read<T>();
	} else {
		value = T::deserialize(ar);
	}
	if (!ar.ok()) {
		throw std::runtime_error("deserialization failed");
	}
	return value;
}

} // namespace hell::transport

#include <memory>
#include <stop_token>

namespace hell::transport {

/**
 * \brief Abstract writer endpoint of a typed channel.
 * \tparam T Item type sent through the channel.
 */
template <typename T>
class ChannelWriter {
      public:
	/** \brief Virtual destructor. */
	virtual ~ChannelWriter()                           = default;
	/**
	 * \brief Sends one item to the channel.
	 * \param item Item to send.
	 * \param st Stop token for cooperative cancellation.
	 * \return True on success, false if closed/cancelled.
	 */
	virtual bool send(T item, std::stop_token st = {}) = 0;
	/** \brief Closes the writer and signals end-of-stream. */
	virtual void close()                               = 0;
};

/**
 * \brief Abstract reader endpoint of a typed channel.
 * \tparam T Item type received from the channel.
 */
template <typename T>
class ChannelReader {
      public:
	/** \brief Virtual destructor. */
	virtual ~ChannelReader()                               = default;
	/**
	 * \brief Receives one item from the channel.
	 * \param st Stop token for cooperative cancellation.
	 * \return Received item or `std::nullopt` when stream ends/cancels.
	 */
	virtual std::optional<T> recv(std::stop_token st = {}) = 0;
};

/**
 * \brief A channel writer that writes items to a local concurrent queue.
 */
template <typename T>
class LocalChannelWriter : public ChannelWriter<T> {
      public:
	explicit LocalChannelWriter(std::shared_ptr<ConcurrentQueue<T>> queue) : queue_(std::move(queue)) {
	}

	bool send(T item, std::stop_token st = {}) override {
		return queue_->push(std::move(item), st);
	}

	void close() override {
		queue_->close();
	}

      private:
	std::shared_ptr<ConcurrentQueue<T>> queue_;
};

/**
 * \brief A channel reader that reads items from a local concurrent queue.
 */
template <typename T>
class LocalChannelReader : public ChannelReader<T> {
      public:
	explicit LocalChannelReader(std::shared_ptr<ConcurrentQueue<T>> queue) : queue_(std::move(queue)) {
	}

	std::optional<T> recv(std::stop_token st = {}) override {
		return queue_->pop(st);
	}

      private:
	std::shared_ptr<ConcurrentQueue<T>> queue_;
};

/**
 * \brief A pair of channel writer and reader.
 */
template <typename T>
struct ChannelPair {
	std::unique_ptr<ChannelWriter<T>> writer;
	std::unique_ptr<ChannelReader<T>> reader;
};

/**
 * \brief Creates a pair of channel writer and reader that write to and read from a local concurrent queue.
 * \param capacity The capacity of the local concurrent queue.
 * \return A pair of channel writer and reader.
 */
template <typename T>
ChannelPair<T> make_local_channel(size_t capacity = 1024) {
	auto queue = std::make_shared<ConcurrentQueue<T>>(capacity);
	return {
	        std::make_unique<LocalChannelWriter<T>>(queue),
	        std::make_unique<LocalChannelReader<T>>(queue)};
}

} // namespace hell::transport

#include <functional>
#include <concepts>

namespace hell::core {

/**
 * \brief A is a work running unit that recieves data items through a channel, processes them and sends
 * the result through another channel.
 *
 * The work is defined by a function that takes an input item and returns an output item.
 */
template <hell::transport::Serializable In, hell::transport::Serializable Out>
class StageExecutor {
      public:
	using InputType  = In;
	using OutputType = Out;

	/**
	 * \brief Creates a stage executor from a worker function.
	 * \param fn Callable applied to each input item.
	 */
	explicit StageExecutor(std::function<Out(In)> fn) : work_(std::move(fn)) {
	}

	/**
	 * \brief Runs the stage until input closes or cancellation is requested.
	 * \param input Input channel.
	 * \param output Output channel.
	 * \param st Stop token controlling cooperative cancellation.
	 */
	void run(transport::ChannelReader<In>&  input,
	         transport::ChannelWriter<Out>& output,
	         std::stop_token                st) {
		while (!st.stop_requested()) {
			auto item = input.recv(st);
			if (!item)
				break;

			auto result = work_(std::move(*item));
			if (!output.send(std::move(result), st))
				break;
		}
		output.close();
	}

      private:
	std::function<Out(In)> work_;
};

/**
 * \brief A source stage: no input channel
 */
template <hell::transport::Serializable Out>
class SourceExecutor {
      public:
	/**
	 * \brief Creates a source executor from a generator function.
	 * \param generator Callable returning next item or `std::nullopt` to stop.
	 */
	explicit SourceExecutor(std::function<std::optional<Out>()> generator) : generator_(std::move(generator)) {
	}

	/**
	 * \brief Runs the source until generator ends or cancellation is requested.
	 * \param output Output channel.
	 * \param st Stop token controlling cooperative cancellation.
	 */
	void run(transport::ChannelWriter<Out>& output, std::stop_token st) {
		while (!st.stop_requested()) {
			auto item = generator_();
			if (!item)
				break;
			if (!output.send(std::move(*item), st))
				break;
		}
		output.close();
	}

      private:
	std::function<std::optional<Out>()> generator_;
};

/**
 * \brief A sink stage: no output channel
 */
template <hell::transport::Serializable In>
class SinkExecutor {
      public:
	/**
	 * \brief Creates a sink executor from a consumer function.
	 * \param consumer Callable invoked for each input item.
	 */
	explicit SinkExecutor(std::function<void(In)> consumer) : consumer_(std::move(consumer)) {
	}

	/**
	 * \brief Runs the sink until input closes or cancellation is requested.
	 * \param input Input channel.
	 * \param st Stop token controlling cooperative cancellation.
	 */
	void run(transport::ChannelReader<In>& input, std::stop_token st) {
		while (!st.stop_requested()) {
			auto item = input.recv(st);
			if (!item)
				break;
			consumer_(std::move(*item));
		}
	}

      private:
	std::function<void(In)> consumer_;
};

} // namespace hell::core

#include <vector>
#include <thread>

namespace hell::core {

/**
 * \brief A work farm that reads items from a shared input channel, processes them and sends the result through a shared output channel.
 * the work is distributed among N workers.
 */
template <typename In, typename Out>
class FarmExecutor {
      public:
	/**
	 * \brief Creates a new FarmExecutor.
	 *  \param worker The work to be done by the workers.
	 *  \param num_workers The number of workers to use.
	 */
	FarmExecutor(std::function<Out(In)> worker, size_t num_workers) : worker_(std::move(worker)), num_workers_(num_workers) {
	}

	/**
	 * \brief Runs the FarmExecutor.
	 *  \param input The input channel.
	 *  \param output The output channel.
	 *  \param st Stop token controlling cooperative cancellation.
	 */
	void run(transport::ChannelReader<In>&  input,
	         transport::ChannelWriter<Out>& output,
	         std::stop_token                st) {
		std::vector<std::jthread> workers;
		workers.reserve(num_workers_);

		for (size_t i = 0; i < num_workers_; ++i) {
			workers.emplace_back([&, this]() {
				while (!st.stop_requested()) {
					auto item = input.recv(st);
					if (!item)
						break;

					auto result = worker_(std::move(*item));
					if (!output.send(std::move(result), st))
						break;
				}
			});
		}

		for (auto& w : workers) {
			if (w.joinable())
				w.join();
		}
		output.close();
	}

      private:
	std::function<Out(In)> worker_;
	size_t                 num_workers_;
};

} // namespace hell::core

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

#include <algorithm>
#include <concepts>
#include <memory>
#include <ostream>
#include <span>
#include <sstream>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <vector>

namespace hell::skeletons {

/** \brief Serialized payload type used by erased node executors. */
using ErasedMessage       = transport::ByteBuffer;
/** \brief Type-erased channel reader for serialized payloads. */
using ErasedChannelReader = transport::ChannelReader<ErasedMessage>;
/** \brief Type-erased channel writer for serialized payloads. */
using ErasedChannelWriter = transport::ChannelWriter<ErasedMessage>;

/**
 * \brief Compile-time mapping from skeleton type to `LogicalPlanNode::Kind`.
 */
template <typename T>
struct LogicalPlanNodeKindTag;

/** \brief Trait: true when `T` is a `Source` skeleton. */
template <typename T>
struct IsSourceSkeleton : std::false_type {
};

template <hell::transport::Serializable Out>
struct IsSourceSkeleton<Source<Out>> : std::true_type {
};

/** \brief Trait: true when `T` is a `Stage` skeleton. */
template <typename T>
struct IsStageSkeleton : std::false_type {
};

template <hell::transport::Serializable In, hell::transport::Serializable Out>
struct IsStageSkeleton<Stage<In, Out>> : std::true_type {
};

/** \brief Trait: true when `T` is a `Farm` skeleton. */
template <typename T>
struct IsFarmSkeleton : std::false_type {
};

template <hell::transport::Serializable In, hell::transport::Serializable Out>
struct IsFarmSkeleton<Farm<In, Out>> : std::true_type {
};

/** \brief Trait: true when `T` is a `Sink` skeleton. */
template <typename T>
struct IsSinkSkeleton : std::false_type {
};

template <hell::transport::Serializable In>
struct IsSinkSkeleton<Sink<In>> : std::true_type {
};

/**
 * \brief Type-erased logical skeleton node plus metadata and execution entrypoint.
 */
class LogicalPlanNode {
      public:
	/**
	 * \brief Logical node category.
	 */
	enum class Kind {
		SOURCE,
		STAGE,
		FARM,
		SINK,
		UNKNOWN
	};

	/**
	 * \brief Creates an erased node from a concrete skeleton.
	 * \tparam Skeleton Concrete skeleton type.
	 * \param skeleton Concrete skeleton instance.
	 * \return Type-erased logical plan node.
	 */
	template <typename Skeleton>
	static LogicalPlanNode from(Skeleton skeleton) {
		using NodeT = std::remove_cvref_t<Skeleton>;
		return LogicalPlanNode(std::move(skeleton), classify<NodeT>());
	}

	LogicalPlanNode(const LogicalPlanNode&)            = default;
	LogicalPlanNode(LogicalPlanNode&&) noexcept        = default;
	LogicalPlanNode& operator=(const LogicalPlanNode&) = default;
	LogicalPlanNode& operator=(LogicalPlanNode&&) noexcept = default;
	~LogicalPlanNode()                                     = default;

	/** \brief Returns node kind metadata. */
	[[nodiscard]] Kind kind() const {
		return kind_;
	}

	/** \brief Returns concrete C++ type metadata for this node. */
	[[nodiscard]] std::type_index concrete_type() const {
		return concrete_type_;
	}

	/** \brief Returns declared input type metadata for this node. */
	[[nodiscard]] std::type_index input_type() const {
		return input_type_;
	}

	/** \brief Returns declared output type metadata for this node. */
	[[nodiscard]] std::type_index output_type() const {
		return output_type_;
	}

	/** \brief Returns debug type name for the concrete skeleton. */
	[[nodiscard]] std::string_view debug_name() const {
		return debug_name_;
	}

	/**
	 * \brief Executes this node using erased channels.
	 * \param input Optional input channel depending on node kind.
	 * \param output Optional output channel depending on node kind.
	 * \param st Stop token controlling cooperative cancellation.
	 */
	void execute(ErasedChannelReader* input, ErasedChannelWriter* output, std::stop_token st = {}) const {
		if (model_) {
			model_->execute(input, output, st);
		}
	}

	/**
	 * \brief Checks whether this erased node stores the requested skeleton type.
	 * \tparam Skeleton Skeleton type to test.
	 * \return True if stored type matches `Skeleton`.
	 */
	template <typename Skeleton>
	[[nodiscard]] bool holds() const {
		return model_ && model_->concrete_type() == std::type_index(typeid(std::remove_cvref_t<Skeleton>));
	}

	/**
	 * \brief Returns pointer to stored skeleton when type matches.
	 * \tparam Skeleton Requested concrete skeleton type.
	 * \return Pointer to stored value or `nullptr` on mismatch.
	 */
	template <typename Skeleton>
	[[nodiscard]] const std::remove_cvref_t<Skeleton>* as() const {
		if (!holds<Skeleton>()) {
			return nullptr;
		}
		return &static_cast<const Model<std::remove_cvref_t<Skeleton>>&>(*model_).value;
	}

      private:
	struct Concept {
		virtual ~Concept() = default;

		[[nodiscard]] virtual std::type_index concrete_type() const                                       = 0;
		virtual void                            execute(ErasedChannelReader*, ErasedChannelWriter*, std::stop_token) const = 0;
	};

	template <typename Skeleton>
	struct Model final : Concept {
		explicit Model(Skeleton v) : value(std::move(v)) {
		}

		[[nodiscard]] std::type_index concrete_type() const override {
			return std::type_index(typeid(Skeleton));
		}

		void execute(ErasedChannelReader* input, ErasedChannelWriter* output, std::stop_token st) const override {
			execute_impl(value, input, output, st);
		}

		Skeleton value;
	};

	template <typename Skeleton>
	static void execute_impl(const Skeleton& skeleton, ErasedChannelReader* input, ErasedChannelWriter* output, std::stop_token st) {
		using NodeT = std::remove_cvref_t<Skeleton>;
		if constexpr (IsSourceSkeleton<NodeT>::value) {
			using Out = typename NodeT::OutputType;
			if (output == nullptr) {
				throw std::invalid_argument("LogicalPlanNode::execute source requires output channel");
			}
			auto generator = skeleton.generator_function();
			while (!st.stop_requested()) {
				auto item = generator();
				if (!item.has_value()) {
					break;
				}
				if (!output->send(transport::serialize<Out>(*item), st)) {
					break;
				}
			}
			output->close();
		} else if constexpr (IsStageSkeleton<NodeT>::value) {
			using In  = typename NodeT::InputType;
			using Out = typename NodeT::OutputType;
			if (input == nullptr || output == nullptr) {
				throw std::invalid_argument("LogicalPlanNode::execute stage requires input and output channels");
			}
			auto worker = skeleton.work_function();
			while (!st.stop_requested()) {
				auto item = input->recv(st);
				if (!item.has_value()) {
					break;
				}
				auto decoded = transport::deserialize<In>(std::move(*item));
				auto result  = worker(std::move(decoded));
				if (!output->send(transport::serialize<Out>(result), st)) {
					break;
				}
			}
			output->close();
		} else if constexpr (IsFarmSkeleton<NodeT>::value) {
			using In  = typename NodeT::InputType;
			using Out = typename NodeT::OutputType;
			if (input == nullptr || output == nullptr) {
				throw std::invalid_argument("LogicalPlanNode::execute farm requires input and output channels");
			}

			const size_t workers_hint = skeleton.policy().num_workers;
			const size_t workers      = workers_hint > 0 ? workers_hint : std::max<size_t>(1, std::thread::hardware_concurrency());

			core::FarmExecutor<ErasedMessage, ErasedMessage> executor([worker = skeleton.work_function()](ErasedMessage item) {
				auto decoded = transport::deserialize<In>(std::move(item));
				auto result  = worker(std::move(decoded));
				return transport::serialize<Out>(result);
			},
			                                                workers);
			executor.run(*input, *output, st);
		} else if constexpr (IsSinkSkeleton<NodeT>::value) {
			using In = typename NodeT::InputType;
			if (input == nullptr) {
				throw std::invalid_argument("LogicalPlanNode::execute sink requires input channel");
			}
			auto consumer = skeleton.consumer_function();
			while (!st.stop_requested()) {
				auto item = input->recv(st);
				if (!item.has_value()) {
					break;
				}
				auto decoded = transport::deserialize<In>(std::move(*item));
				consumer(std::move(decoded));
			}
		}
	}

	template <typename Skeleton>
	explicit LogicalPlanNode(Skeleton skeleton, Kind kind)
	    : kind_(kind),
	      concrete_type_(typeid(std::remove_cvref_t<Skeleton>)),
	      input_type_(typeid(typename std::remove_cvref_t<Skeleton>::InputType)),
	      output_type_(typeid(typename std::remove_cvref_t<Skeleton>::OutputType)),
	      debug_name_(typeid(std::remove_cvref_t<Skeleton>).name()),
	      model_(std::make_shared<Model<std::remove_cvref_t<Skeleton>>>(std::move(skeleton))) {
	}

	template <typename Skeleton>
	static consteval Kind classify() {
		return LogicalPlanNodeKindTag<std::remove_cvref_t<Skeleton>>::value;
	}

	Kind                     kind_          = Kind::UNKNOWN;
	std::type_index          concrete_type_ = typeid(void);
	std::type_index          input_type_    = typeid(void);
	std::type_index          output_type_   = typeid(void);
	std::string_view         debug_name_;
	std::shared_ptr<Concept> model_;
};

template <typename T>
struct LogicalPlanNodeKindTag {
	static constexpr LogicalPlanNode::Kind value = LogicalPlanNode::Kind::UNKNOWN;
};

template <hell::transport::Serializable Out>
struct LogicalPlanNodeKindTag<Source<Out>> {
	static constexpr LogicalPlanNode::Kind value = LogicalPlanNode::Kind::SOURCE;
};

template <hell::transport::Serializable In, hell::transport::Serializable Out>
struct LogicalPlanNodeKindTag<Stage<In, Out>> {
	static constexpr LogicalPlanNode::Kind value = LogicalPlanNode::Kind::STAGE;
};

template <hell::transport::Serializable In, hell::transport::Serializable Out>
struct LogicalPlanNodeKindTag<Farm<In, Out>> {
	static constexpr LogicalPlanNode::Kind value = LogicalPlanNode::Kind::FARM;
};

template <hell::transport::Serializable In>
struct LogicalPlanNodeKindTag<Sink<In>> {
	static constexpr LogicalPlanNode::Kind value = LogicalPlanNode::Kind::SINK;
};

/**
 * \brief Converts node-kind enum value to a stable string label.
 * \param kind Node kind.
 * \return String literal for the provided kind.
 */
[[nodiscard]] constexpr std::string_view logical_plan_node_kind_name(LogicalPlanNode::Kind kind) {
	switch (kind) {
		case LogicalPlanNode::Kind::SOURCE:
			return "SOURCE";
		case LogicalPlanNode::Kind::STAGE:
			return "STAGE";
		case LogicalPlanNode::Kind::FARM:
			return "FARM";
		case LogicalPlanNode::Kind::SINK:
			return "SINK";
		default:
			return "UNKNOWN";
	}
}

/**
 * \brief Builds a one-line human-readable description for a node.
 * \param node Node to describe.
 * \return Formatted description string.
 */
[[nodiscard]] inline std::string describe_node(const LogicalPlanNode& node) {
	std::ostringstream stream;
	stream << "kind=" << logical_plan_node_kind_name(node.kind()) << ", concrete=" << node.debug_name() << ", input="
	       << node.input_type().name() << ", output=" << node.output_type().name();
	return stream.str();
}

/**
 * \brief Prints a logical plan to an output stream.
 * \param out Target output stream.
 * \param plan Plan to print.
 */
inline void print_plan(std::ostream& out, std::span<const LogicalPlanNode> plan) {
	out << "Logical plan (" << plan.size() << " nodes)\n";
	for (size_t i = 0; i < plan.size(); ++i) {
		out << "[" << i << "] " << describe_node(plan[i]) << '\n';
	}
}

/**
 * \brief Prints a logical plan to an output stream.
 * \param out Target output stream.
 * \param plan Plan to print.
 */
inline void print_plan(std::ostream& out, const std::vector<LogicalPlanNode>& plan) {
	print_plan(out, std::span<const LogicalPlanNode>(plan.data(), plan.size()));
}

} // namespace hell::skeletons

#include <string_view>
#include <vector>

namespace hell::engine {

/**
 * \brief Engine alias for a type-erased skeleton node.
 */
using LogicalPlanNode = skeletons::LogicalPlanNode;

/**
 * \brief Ordered list of logical nodes composing a workflow.
 */
using LogicalPlan = std::vector<LogicalPlanNode>;

/**
 * \brief Copies a user-erased plan into the engine logical-plan container.
 * \param user_plan Logical nodes produced from user skeletons.
 * \return Engine-owned logical plan.
 */
[[nodiscard]] inline LogicalPlan build_logical_plan(const std::vector<skeletons::LogicalPlanNode>& user_plan) {
	return LogicalPlan(user_plan.begin(), user_plan.end());
}

/**
 * \brief Returns a readable string for a logical node kind.
 * \param kind Logical node kind enum value.
 * \return Human-readable kind name.
 */
[[nodiscard]] inline std::string_view kind_name(skeletons::LogicalPlanNode::Kind kind) {
	return skeletons::logical_plan_node_kind_name(kind);
}

} // namespace hell::engine

#include <vector>

namespace hell::engine {

/**
 * \brief One logical node plus the MPI rank selected to execute it.
 */
struct PhysicalPlanNode {
	/** \brief Logical node descriptor. */
	LogicalPlanNode logical;
	/** \brief MPI rank assigned by the mapper. */
	int             assigned_rank = 0;
};

/**
 * \brief Mapped workflow where each logical node has a target rank.
 */
using PhysicalPlan = std::vector<PhysicalPlanNode>;

/**
 * \brief Maps a logical plan to MPI ranks.
 *
 * Source and sink nodes are pinned to rank 0. Intermediate nodes are
 * round-robined across worker ranks (1..world_size-1) when possible.
 *
 * \param logical_plan Input logical workflow.
 * \param world_size Number of MPI ranks in the communicator.
 * \return Physical plan with rank assignments.
 */
[[nodiscard]] inline PhysicalPlan map_logical_plan_to_cluster(const LogicalPlan& logical_plan, int world_size) {
	PhysicalPlan plan;
	plan.reserve(logical_plan.size());
	if (world_size <= 1) {
		for (const auto& logical_node : logical_plan) {
			plan.push_back(PhysicalPlanNode{.logical = logical_node, .assigned_rank = 0});
		}
		return plan;
	}

	size_t worker_rr = 0;
	for (size_t i = 0; i < logical_plan.size(); ++i) {
		const auto kind = logical_plan[i].kind();
		if (kind == skeletons::LogicalPlanNode::Kind::SOURCE || kind == skeletons::LogicalPlanNode::Kind::SINK) {
			plan.push_back(PhysicalPlanNode{.logical = logical_plan[i], .assigned_rank = 0});
			continue;
		}

		const int worker_rank = 1 + static_cast<int>(worker_rr % static_cast<size_t>(world_size - 1));
		plan.push_back(PhysicalPlanNode{.logical = logical_plan[i], .assigned_rank = worker_rank});
		++worker_rr;
	}
	return plan;
}

} // namespace hell::engine

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <cstddef>

namespace hell::engine {

/**
 * \brief Summary of one `Engine::execute()` invocation for the local MPI rank.
 */
struct ExecutionReport {
	/** \brief Local MPI rank for this report. */
	int    rank                 = 0;
	/** \brief MPI world size observed by this rank. */
	int    world_size           = 1;
	/** \brief True when this rank is the coordinator (rank 0). */
	bool   coordinator_did_work = false;
	/** \brief True when at least one physical-plan node is assigned to this rank. */
	bool   rank_has_assigned_work = false;
	/** \brief Number of nodes in the user-provided logical pipeline. */
	size_t user_plan_nodes      = 0;
	/** \brief Number of nodes in the engine logical plan. */
	size_t logical_plan_nodes   = 0;
	/** \brief Number of nodes in the mapped physical plan. */
	size_t physical_plan_nodes  = 0;
	/** \brief Number of physical-plan nodes assigned to this rank. */
	size_t assigned_plan_nodes  = 0;
};

/**
 * \brief MPI rank discovery result used by the engine.
 */
struct RankInfo {
	/** \brief MPI rank ID in `MPI_COMM_WORLD`. */
	int rank       = 0;
	/** \brief Number of ranks in `MPI_COMM_WORLD`. */
	int world_size = 1;
};

} // namespace hell::engine

#include <algorithm>
#include <vector>

namespace hell::engine {

/**
 * \brief Builds an execution report for coordinator rank behavior.
 * \param user_plan_nodes Number of user nodes in the original workflow.
 * \param rank Local MPI rank.
 * \param world_size MPI world size.
 * \param logical_plan Logical plan visible to this rank.
 * \param physical_plan Physical plan visible to this rank.
 * \return Execution report for rank 0 orchestration.
 */
[[nodiscard]] inline ExecutionReport execute_orchestrator(
    size_t                                         user_plan_nodes,
    int                                            rank,
    int                                            world_size,
    const LogicalPlan&                             logical_plan,
    const PhysicalPlan&                            physical_plan) {
	const size_t assigned_nodes =
	    static_cast<size_t>(std::count_if(physical_plan.begin(), physical_plan.end(), [rank](const PhysicalPlanNode& node) {
		    return node.assigned_rank == rank;
	    }));

	ExecutionReport report;
	report.rank                   = rank;
	report.world_size             = world_size;
	report.coordinator_did_work   = true;
	report.rank_has_assigned_work = assigned_nodes > 0;
	report.user_plan_nodes        = user_plan_nodes;
	report.logical_plan_nodes     = logical_plan.size();
	report.physical_plan_nodes    = physical_plan.size();
	report.assigned_plan_nodes    = assigned_nodes;
	return report;
}

} // namespace hell::engine

#include <algorithm>
#include <cstddef>

namespace hell::engine {

/**
 * \brief Builds an execution report for non-coordinator rank behavior.
 * \param rank Local MPI rank.
 * \param world_size MPI world size.
 * \param user_plan_nodes Number of user nodes in the original workflow.
 * \param logical_plan Logical plan visible to this rank.
 * \param physical_plan Physical plan visible to this rank.
 * \return Execution report for worker-side execution state.
 */
[[nodiscard]] inline ExecutionReport execute_worker(int rank, int world_size, size_t user_plan_nodes, const LogicalPlan& logical_plan, const PhysicalPlan& physical_plan) {
	const size_t assigned_nodes =
	    static_cast<size_t>(std::count_if(physical_plan.begin(), physical_plan.end(), [rank](const PhysicalPlanNode& node) {
		    return node.assigned_rank == rank;
	    }));

	ExecutionReport report;
	report.rank                   = rank;
	report.world_size             = world_size;
	report.coordinator_did_work   = false;
	report.rank_has_assigned_work = assigned_nodes > 0;
	report.user_plan_nodes        = user_plan_nodes;
	report.logical_plan_nodes     = logical_plan.size();
	report.physical_plan_nodes    = physical_plan.size();
	report.assigned_plan_nodes    = assigned_nodes;
	return report;
}

} // namespace hell::engine

#include <mpi.h>

namespace hell::engine {

/**
 * \brief Coordinator/worker engine that stores a workflow and maps it to ranks.
 */
class Engine {
      public:
	/** \brief Constructs an empty engine instance. */
	Engine() = default;

	/**
	 * \brief Sets the user pipeline that will be planned on execute.
	 * \param pipeline Typed user pipeline.
	 * \return Reference to this engine.
	 */
	template <typename... Stages>
	Engine& set_pipeline(skeletons::Pipeline<Stages...> pipeline) {
		using PipelineT = skeletons::Pipeline<Stages...>;
		user_pipeline_  = std::make_unique<UserPipelineModel<PipelineT>>(std::move(pipeline));
		return *this;
	}

	/**
	 * \brief Clears the pipeline and cached plans.
	 */
	void clear() {
		user_pipeline_.reset();
		logical_plan_.clear();
		physical_plan_.clear();
	}

	/**
	 * \brief Executes one planning step for the current rank.
	 * \return Rank-local execution report.
	 */
	[[nodiscard]] ExecutionReport execute() {
		const RankInfo rank_info = discover_rank_info();

		if (!user_pipeline_) {
			logical_plan_.clear();
			physical_plan_.clear();
			return execute_worker(rank_info.rank, rank_info.world_size, 0, logical_plan_, physical_plan_);
		}

		const auto erased_user_plan = user_pipeline_->erase();
		logical_plan_               = build_logical_plan(erased_user_plan);
		physical_plan_              = map_logical_plan_to_cluster(logical_plan_, rank_info.world_size);

		const size_t user_plan_nodes = user_pipeline_->size();

		if (rank_info.rank != 0) {
			return execute_worker(rank_info.rank, rank_info.world_size, user_plan_nodes, logical_plan_, physical_plan_);
		}

		return execute_orchestrator(user_plan_nodes, rank_info.rank, rank_info.world_size, logical_plan_, physical_plan_);
	}

	/**
	 * \brief Returns the last built logical plan.
	 * \return Reference to cached logical plan.
	 */
	[[nodiscard]] const LogicalPlan& logical_plan() const {
		return logical_plan_;
	}

	/**
	 * \brief Returns the last built physical plan.
	 * \return Reference to cached physical plan.
	 */
	[[nodiscard]] const PhysicalPlan& physical_plan() const {
		return physical_plan_;
	}

      private:
	struct UserPipelineConcept {
		virtual ~UserPipelineConcept() = default;

		[[nodiscard]] virtual std::vector<skeletons::LogicalPlanNode> erase() const = 0;
		[[nodiscard]] virtual size_t                                  size() const  = 0;
	};

	template <typename Pipeline>
	struct UserPipelineModel final : UserPipelineConcept {
		explicit UserPipelineModel(Pipeline value) : value_(std::move(value)) {
		}

		[[nodiscard]] std::vector<skeletons::LogicalPlanNode> erase() const override {
			return erase_pipeline(value_);
		}

		[[nodiscard]] size_t size() const override {
			return Pipeline::pipeline_size();
		}

		Pipeline value_;
	};

	template <typename Pipeline>
	[[nodiscard]] static std::vector<skeletons::LogicalPlanNode> erase_pipeline(const Pipeline& pipeline) {
		std::vector<skeletons::LogicalPlanNode> nodes;
		nodes.reserve(Pipeline::pipeline_size());
		std::apply([&nodes](const auto&... stages) {
			(nodes.push_back(skeletons::LogicalPlanNode::from(stages)), ...);
		},
		           pipeline.stages());
		return nodes;
	}

	[[nodiscard]] static RankInfo discover_rank_info() {
		RankInfo info{};
		int      initialized = 0;
		MPI_Initialized(&initialized);
		if (initialized) {
			MPI_Comm_rank(MPI_COMM_WORLD, &info.rank);
			MPI_Comm_size(MPI_COMM_WORLD, &info.world_size);
		}
		return info;
	}

	std::unique_ptr<UserPipelineConcept> user_pipeline_;
	LogicalPlan                          logical_plan_;
	PhysicalPlan                         physical_plan_;
};

} // namespace hell::engine

