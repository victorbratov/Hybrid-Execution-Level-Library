#pragma once
#if !__has_include(<mpi.h>)
#error "Hell requires MPI to be available"
#endif

#include <cstdint>
#include <vector>

enum class StageType : uint8_t {
	SOURCE,
	SINK,
	FILTER,
	FARM
};

struct StageDescriptor {
	uint32_t  id;
	StageType type;
	uint32_t  concurrency;

	uint32_t assigned_node;
	uint16_t assigned_threads;

	std::vector<uint32_t> previous_stage_ranks;
	std::vector<uint32_t> next_stage_ranks;
	uint32_t              previous_stage_id;
	uint32_t              next_stage_id;

	uint32_t input_tag;
	uint32_t output_tag;
};

struct WorkflowPlan {
	std::vector<StageDescriptor> stages;
	uint32_t                     num_stages;
};

#include <cstdint>
#include <concepts>
#include <functional>
#include <iterator>
#include <optional>
#include <type_traits>
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

inline constexpr uint64_t fnv1a(const char* string) {
	uint64_t hash = 14695981039346656037ULL;
	while (*string) {
		hash ^= static_cast<unsigned char>(*string++);
		hash *= 1099511628211ULL;
	}
	return hash;
}

template <typename T>
inline constexpr uint64_t type_id() {
#if defined(__clang__) || defined(__GNUC__)
	return fnv1a(__PRETTY_FUNCTION__);
#elif defined(_MSC_VER)
	return fnv1a(__FUNCSIG__);
#endif
}

template <typename T>
concept Serializable = requires(
                               T                     t,
                               std::vector<uint8_t>& buf,
                               const uint8_t*        data,
                               size_t                size) {
	{ t.serialize(buf) } -> std::same_as<void>;
	{ T::deserialize(data, size) } -> std::convertible_to<T>;
} && std::copy_constructible<T>;

template <typename T>
concept TriviallySerializable = std::is_trivially_copyable_v<T> && !Serializable<T>;

template <typename T>
concept PayloadCompatible = Serializable<T> || TriviallySerializable<T>;

struct IHolder {
	virtual ~IHolder()                                                = default;
	virtual uint64_t                 id() const                       = 0;
	virtual void                     write(std::vector<uint8_t>& buf) = 0;
	virtual std::unique_ptr<IHolder> clone() const                    = 0;
	virtual void*                    ptr()                            = 0;
	virtual const void*              ptr() const                      = 0;
};

template <PayloadCompatible T>
class Holder final : public IHolder {
	T value;

      public:
	explicit Holder(T value) : value(std::move(value)) {
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

class Payload {
	std::unique_ptr<IHolder> holder_;

      public:
	Payload() = default;

	template <PayloadCompatible T>
	Payload(T value) : holder_(std::make_unique<Holder<T>>(std::move(value))) {
		PayloadRegistry::register_type<T>();
	}

	Payload(const Payload& other) : holder_(other.holder_ ? other.holder_->clone() : nullptr) {
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

struct stop_iteration {};

template <typename T>
class generator {
      public:
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
	        : handle_(handle) {
	}

	~generator() {
		if (handle_)
			handle_.destroy();
	}

	generator(const generator&)            = delete;
	generator& operator=(const generator&) = delete;

	generator(generator&& other) noexcept
	        : handle_(std::exchange(other.handle_, nullptr)) {
	}

	generator& operator=(generator&& other) noexcept {
		if (this != &other) {
			if (handle_)
				handle_.destroy();
			handle_ = std::exchange(other.handle_, nullptr);
		}
		return *this;
	}

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

	bool has_next() const noexcept {
		return handle_ && !handle_.done();
	}

	bool done() const noexcept {
		return !handle_ || handle_.done();
	}

	class iterator {
	      public:
		using iterator_category = std::input_iterator_tag;
		using difference_type   = std::ptrdiff_t;
		using value_type        = T;
		using reference         = T&;
		using pointer           = T*;

		iterator() noexcept = default;
		explicit iterator(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {
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

	iterator begin() {
		if (handle_) {
			handle_.resume();
			if (handle_.done())
				return {};
		}
		return iterator{handle_};
	}

	iterator end() noexcept {
		return {};
	}

      private:
	std::coroutine_handle<promise_type> handle_ = nullptr;
};

class StageBase {
      public:
	uint32_t  id = 0;
	StageType type_;
	uint32_t  requested_concurrency = 1;

	virtual ~StageBase() = default;

	virtual Payload execute(const Payload&) = 0;

	virtual std::optional<Payload> generate() {
		return std::nullopt;
	};

	virtual void consume(const Payload&) {};
};

template <PayloadCompatible Output>
class SourceStage : public StageBase {
	generator<Output> generator_;

      public:
	explicit SourceStage(generator<Output> generator) : generator_(std::move(generator)) {
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

template <PayloadCompatible Input>
class SinkStage : public StageBase {
	std::function<void(const Input&)> consumer_fn_;

      public:
	explicit SinkStage(std::function<void(const Input&)> consumer_fn) : consumer_fn_(consumer_fn) {
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

template <PayloadCompatible Input, PayloadCompatible Output>
class FilterStage : public StageBase {
	std::function<Output(const Input&)> processor_fn_;

      public:
	explicit FilterStage(std::function<Output(const Input&)> processor_fn) : processor_fn_(processor_fn) {
		type_ = StageType::FILTER;
		PayloadRegistry::register_all<Input, Output>();
	};

	Payload execute(const Payload& input_item) override {
		return processor_fn_(input_item.get<Input>());
	};
};

template <PayloadCompatible Input, PayloadCompatible Output>
class FarmStage : public StageBase {
	std::function<Output(const Input&)> processor_fn_;

      public:
	explicit FarmStage(std::function<Output(const Input&)> processor_fn) : processor_fn_(processor_fn) {
		type_ = StageType::FARM;
		PayloadRegistry::register_all<Input, Output>();
	};

	FarmStage& concurrency(uint32_t concurrency) {
		requested_concurrency = concurrency;
		return *this;
	};

	Payload execute(const Payload& input_item) override {
		return processor_fn_(input_item.get<Input>());
	};
};

#include <vector>

inline Payload deserialize_payload(const std::vector<uint8_t>& buf) {
	return Payload::deserialize(buf);
}

inline std::vector<uint8_t> serialize_payload(const Payload& payload) {
	return payload.serialize();
}

#include <memory>
#include <vector>

class Pipeline {
      public:
	std::vector<std::shared_ptr<StageBase>> stages_;

	Pipeline() = default;

	explicit Pipeline(std::shared_ptr<StageBase> stage) {
		stages_.push_back(stage);
	};

	Pipeline operator|(Pipeline&& rhs) {
		for (auto& stage : rhs.stages_) {
			stages_.push_back(stage);
		}
		return std::move(*this);
	};
};

template <typename L, typename R, typename = std::enable_if_t<std::is_base_of_v<StageBase, L> && std::is_base_of_v<StageBase, R>>>
Pipeline operator|(L&& lhs, R&& rhs) {
	Pipeline pipeline;
	pipeline.stages_.push_back(std::make_shared<std::decay_t<L>>(std::forward<L>(lhs)));
	pipeline.stages_.push_back(std::make_shared<std::decay_t<R>>(std::forward<R>(rhs)));
	return pipeline;
};

template <typename R, typename = std::enable_if_t<std::is_base_of_v<StageBase, R>>>
Pipeline operator|(Pipeline&& lhs, R&& rhs) {
	lhs.stages_.push_back(std::make_shared<std::decay_t<R>>(std::forward<R>(rhs)));
	return std::move(lhs);
};

#include <mpi.h>

class Planner {
      public:
	static WorkflowPlan plan(const Pipeline& pipeline, uint16_t world_size, const std::vector<int> cores_per_node) {
		WorkflowPlan wp;
		wp.num_stages = pipeline.stages_.size();

		for (uint32_t i = 0; i < wp.num_stages; ++i) {
			pipeline.stages_[i]->id = i;
		}

		std::vector<int> remaining_cores = cores_per_node;

		for (uint32_t i = 0; i < wp.num_stages; ++i) {
			auto&           stage = pipeline.stages_[i];
			StageDescriptor sd;
			sd.id          = stage->id;
			sd.type        = stage->type_;
			sd.concurrency = stage->requested_concurrency;

			uint16_t best_node = 0;
			for (uint16_t node = 0; node < world_size; ++node) {
				if (remaining_cores[node] >= (int)stage->requested_concurrency) {
					best_node = node;
					break;
				}

				if (remaining_cores[node] > remaining_cores[best_node]) {
					best_node = node;
				}
			}

			sd.assigned_node    = best_node;
			sd.assigned_threads = std::min((int)sd.concurrency, std::max(1, (int)remaining_cores[best_node]));

			remaining_cores[best_node] -= sd.assigned_threads;

			sd.previous_stage_id = (i > 0) ? i - 1 : UINT32_MAX;
			sd.next_stage_id     = (i < wp.num_stages - 1) ? i + 1 : UINT32_MAX;

			sd.input_tag  = (i > 0) ? (int)(i - 1) * 100 : -1;
			sd.output_tag = (i < wp.num_stages - 1) ? (int)i * 100 : -1;

			wp.stages.push_back(sd);
		}

		for (uint32_t i = 0; i < wp.num_stages; ++i) {
			if (i > 0) {
				wp.stages[i].previous_stage_ranks.push_back(wp.stages[i - 1].assigned_node);
			}
			if (i < wp.num_stages - 1) {
				wp.stages[i].next_stage_ranks.push_back(wp.stages[i + 1].assigned_node);
			}
		}

		return wp;
	}
};

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
			push(&stage.output_tag, sizeof(stage.output_tag));

			uint32_t nprev = stage.previous_stage_ranks.size();
			push(&nprev, sizeof(nprev));
			for (auto& prev : stage.previous_stage_ranks)
				push(&prev, sizeof(prev));

			uint32_t nnext = stage.next_stage_ranks.size();
			push(&nnext, sizeof(nnext));
			for (auto& next : stage.next_stage_ranks)
				push(&next, sizeof(next));
		}

		return buf;
	}

	static WorkflowPlan deserialize(const std::vector<uint8_t>& buf) {
		WorkflowPlan wp;
		size_t       offset = 0;
		auto         pop    = [&](void* data, size_t size) {
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
			pop(&sd.output_tag, sizeof(sd.output_tag));

			uint32_t nprev;
			pop(&nprev, sizeof(nprev));
			sd.previous_stage_ranks.resize(nprev);
			for (auto& n : sd.previous_stage_ranks)
				pop(&n, sizeof(n));

			uint32_t nnext;
			pop(&nnext, sizeof(nnext));
			sd.next_stage_ranks.resize(nnext);
			for (auto& n : sd.next_stage_ranks)
				pop(&n, sizeof(n));

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

#include <mpi.h>

#include <memory>
#include <mutex>
#include <queue>
#include <stop_token>
#include <thread>

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
		closed_ = true;
		cv_.notify_all();
	}

	bool closed() const {
		return closed_;
	}
};

struct Message {
	Payload payload;
	bool    eos = false;
};

class StageExecutor {
      public:
	StageDescriptor            sd_;
	std::shared_ptr<StageBase> stage_;
	ConcurrentQueue<Message>   queue_;
	int                        rank_;

	StageExecutor(StageDescriptor sd, std::shared_ptr<StageBase> stage, int rank) : sd_(sd), stage_(stage), rank_(rank) {
	}

	void mpi_reciever_loop() {
		if (sd_.type == StageType::SOURCE)
			return;

		const uint32_t expected_eos =
		        sd_.previous_stage_ranks.empty() ? 1U : static_cast<uint32_t>(sd_.previous_stage_ranks.size());
		uint32_t eos_received = 0;

		while (true) {
			MPI_Status status;
			int        count;

			MPI_Probe(MPI_ANY_SOURCE, sd_.input_tag, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_BYTE, &count);

			std::vector<uint8_t> buf(count);
			MPI_Recv(buf.data(), count, MPI_BYTE, status.MPI_SOURCE, sd_.input_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			if (count == 0) {
				++eos_received;
				if (eos_received >= expected_eos) {
					queue_.push(Message{.eos = true});
					break;
				}
				continue;
			}

			Message msg;
			msg.payload = deserialize_payload(buf);
			queue_.push(std::move(msg));
		};
	}

	void send_to_next(const Payload& res) {
		if (sd_.next_stage_ranks.empty())
			return;

		auto buf = serialize_payload(res);
		for (auto& dest : sd_.next_stage_ranks) {
			MPI_Send(buf.data(), buf.size(), MPI_BYTE, dest, sd_.output_tag, MPI_COMM_WORLD);
		}
	}

	void send_eos_to_next() {
		for (auto& dest : sd_.next_stage_ranks) {
			MPI_Send(nullptr, 0, MPI_BYTE, dest, sd_.output_tag, MPI_COMM_WORLD);
		};
	}

	void run_source() {
		while (true) {
			auto item = stage_->generate();
			if (!item.has_value())
				break;
			send_to_next(*item);
		}
		send_eos_to_next();
	}

	void run_filter() {
		Message msg;
		while (queue_.pop(msg)) {
			if (msg.eos)
				break;
			auto item = stage_->execute(msg.payload);
			send_to_next(item);
		};
		send_eos_to_next();
	}

	void run_farm() {
		ConcurrentQueue<Message> results;
		std::atomic<int>         active_workers{(int)sd_.assigned_threads};

		std::vector<std::jthread> workers;
		for (uint32_t i = 0; i < sd_.assigned_threads; ++i) {
			workers.emplace_back([&](std::stop_token) {
				Message msg;
				while (queue_.pop(msg)) {
					if (msg.eos) {
						queue_.push(msg);
						break;
					}
					auto res = stage_->execute(msg.payload);
					results.push(Message{.payload = res});
				}
				if (--active_workers == 0) {
					results.push(Message{.eos = true});
				}
			});
		}

		std::jthread sender([&](std::stop_token) {
			Message msg;
			while (results.pop(msg)) {
				if (msg.eos)
					break;
				send_to_next(msg.payload);
			}
			send_eos_to_next();
		});

		for (auto& worker : workers)
			worker.join();
		sender.join();
	}

	void run_sink() {
		Message msg;
		while (queue_.pop(msg)) {
			if (msg.eos)
				break;
			stage_->consume(msg.payload);
		}
	}

	void run_stage() {
		std::optional<std::jthread> reciever;
		if (sd_.type != StageType::SOURCE) {
			reciever.emplace([this](std::stop_token) {
				this->mpi_reciever_loop();
			});
		}

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

		if (reciever)
			reciever->join();
	}
};

#include <mpi.h>

#include <thread>
class Engine {
	Pipeline pipeline_;

      public:
	void set_workflow(Pipeline pipeline) {
		pipeline_ = std::move(pipeline);
	}

	void execute() {
		int rank, world_size;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		MPI_Comm_size(MPI_COMM_WORLD, &world_size);

		int              core_num = std::thread::hardware_concurrency();
		std::vector<int> cores_per_node(world_size);
		MPI_Allgather(&core_num, 1, MPI_INT, cores_per_node.data(), 1, MPI_INT, MPI_COMM_WORLD);

		WorkflowPlan wp;
		if (rank == 0) {
			wp = Planner::plan(pipeline_, world_size, cores_per_node);
		}

		PlanSerializer::broadcast_plan(wp, 0, MPI_COMM_WORLD);

		std::vector<std::jthread> stage_threads;

		for (auto& sd : wp.stages) {
			if ((int)sd.assigned_node != rank)
				continue;

			auto stage_ptr = pipeline_.stages_[sd.id];
			auto executor  = std::make_shared<StageExecutor>(sd, stage_ptr, rank);

			stage_threads.emplace_back([executor](std::stop_token) {
				executor->run_stage();
			});
		}

		for (auto& stage_thread : stage_threads) {
			stage_thread.join();
		}

		MPI_Barrier(MPI_COMM_WORLD);
	}
};

