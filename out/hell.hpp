#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <stop_token>

namespace hell::transport {

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

template <typename T>
class ChannelWriter {
      public:
	virtual ~ChannelWriter()                           = default;
	virtual bool send(T item, std::stop_token st = {}) = 0;
	virtual void close()                               = 0;
};

template <typename T>
class ChannelReader {
      public:
	virtual ~ChannelReader()                               = default;
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

	explicit StageExecutor(std::function<Out(In)> fn) : work_(std::move(fn)) {
	}

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
	explicit SourceExecutor(std::function<std::optional<Out>()> generator) : generator_(std::move(generator)) {
	}

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
	explicit SinkExecutor(std::function<void(In)> consumer) : consumer_(std::move(consumer)) {
	}

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
	 *  \return A new FarmExecutor.
	 */
	FarmExecutor(std::function<Out(In)> worker, size_t num_workers) : worker_(std::move(worker)), num_workers_(num_workers) {
	}

	/**
	 * \brief Runs the FarmExecutor.
	 *  \param input The input channel.
	 *  \param output The output channel.
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

namespace hell::skeletons {
struct FarmPolicy {
	size_t num_workers = 0;

	enum class Distribution {
		LOCAL,
		DISTRIBUTED,
		HYBRID,
		AUTO
	};

	Distribution distribution = Distribution::AUTO;

	size_t queue_capacity = 1024;
};

template <hell::transport::Serializable In, hell::transport::Serializable Out>
class Farm {
      public:
	using InputType  = In;
	using OutputType = Out;

	Farm(std::function<Out(In)> work_function, size_t num_workers = 0) : work_(std::move(work_function)), policy_{.num_workers = num_workers} {
	}

	Farm(std::function<Out(In)> work_function, FarmPolicy policy) : work_(std::move(work_function)), policy_(std::move(policy)) {
	}

	const auto& work_function() {
		return work_;
	}

	const auto& policy() {
		return policy_;
	}

      private:
	std::function<Out(In)> work_;
	FarmPolicy             policy_;
};
} // namespace hell::skeletons

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

#include <memory>
#include <concepts>
#include <ostream>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <vector>

namespace hell::skeletons {

template <typename T>
struct LogicalPlanNodeKindTag;

class LogicalPlanNode {
      public:
	enum class Kind {
		SOURCE,
		STAGE,
		FARM,
		SINK,
		UNKNOWN
	};

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

	[[nodiscard]] Kind kind() const {
		return kind_;
	}

	[[nodiscard]] std::type_index concrete_type() const {
		return concrete_type_;
	}

	[[nodiscard]] std::type_index input_type() const {
		return input_type_;
	}

	[[nodiscard]] std::type_index output_type() const {
		return output_type_;
	}

	[[nodiscard]] std::string_view debug_name() const {
		return debug_name_;
	}

	template <typename Skeleton>
	[[nodiscard]] bool holds() const {
		return model_ && model_->concrete_type() == std::type_index(typeid(std::remove_cvref_t<Skeleton>));
	}

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

		[[nodiscard]] virtual std::type_index concrete_type() const = 0;
	};

	template <typename Skeleton>
	struct Model final : Concept {
		explicit Model(Skeleton v) : value(std::move(v)) {
		}

		[[nodiscard]] std::type_index concrete_type() const override {
			return std::type_index(typeid(Skeleton));
		}

		Skeleton value;
	};

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

	Kind                    kind_          = Kind::UNKNOWN;
	std::type_index         concrete_type_ = typeid(void);
	std::type_index         input_type_    = typeid(void);
	std::type_index         output_type_   = typeid(void);
	std::string_view        debug_name_;
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

[[nodiscard]] inline std::string describe_node(const LogicalPlanNode& node) {
	std::ostringstream stream;
	stream << "kind=" << logical_plan_node_kind_name(node.kind()) << ", concrete=" << node.debug_name() << ", input="
	       << node.input_type().name() << ", output=" << node.output_type().name();
	return stream.str();
}

inline void print_plan(std::ostream& out, std::span<const LogicalPlanNode> plan) {
	out << "Logical plan (" << plan.size() << " nodes)\n";
	for (size_t i = 0; i < plan.size(); ++i) {
		out << "[" << i << "] " << describe_node(plan[i]) << '\n';
	}
}

inline void print_plan(std::ostream& out, const std::vector<LogicalPlanNode>& plan) {
	print_plan(out, std::span<const LogicalPlanNode>(plan.data(), plan.size()));
}

} // namespace hell::skeletons

