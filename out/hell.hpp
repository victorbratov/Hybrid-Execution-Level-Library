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
			if (stage->type_ == StageType::SOURCE || stage->type_ == StageType::SINK) {
				best_node = 0;
			} else {
				if (world_size > 1) {
					// Prefer collocating with the previous stage to avoid MPI overhead
					uint16_t prev_node = (i > 0) ? wp.stages[i - 1].assigned_node : 0;
					if (remaining_cores[prev_node] > 0) {
						best_node = prev_node;
					} else {
						// Fall back: pick the node with the most remaining cores
						best_node = 1;
						for (uint16_t node = 1; node < world_size; ++node) {
							if (remaining_cores[node] > remaining_cores[best_node]) {
								best_node = node;
							}
						}
					}
				} else {
					best_node = 0;
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

#include <atomic>
#include <cstdint>
#include <vector>
#include <fstream>

#ifdef __APPLE__
#include <mach/mach_host.h>
#include <mach/processor_info.h>
#include <mach/mach.h>
#endif

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

struct NodeMetrics {
	int      rank       = 0;
	uint32_t hw_threads = 0;
	double   cpu_load   = 0.0;
	uint64_t rss_bytes  = 0;

	std::vector<StageMetrics::Snapshot> stages;
	std::vector<double>                 core_loads;
};

template <typename Precision = std::chrono::microseconds>
class ScopedTimer {
	std::atomic<uint64_t>&                target_;
	std::chrono::steady_clock::time_point start_;

      public:
	explicit ScopedTimer(std::atomic<uint64_t>& target) :
	        target_(target), start_(std::chrono::steady_clock::now()) {
	}

	~ScopedTimer() {
		auto end     = std::chrono::steady_clock::now();
		auto elapsed = std::chrono::duration_cast<Precision>(end - start_).count();
		target_.fetch_add(elapsed, std::memory_order_relaxed);
	}

	ScopedTimer(const ScopedTimer&)            = delete;
	ScopedTimer& operator=(const ScopedTimer&) = delete;
};

inline double get_cpu_load() {
	static std::atomic<uint64_t> prev_total{0}, prev_idle{0};
	uint64_t                     total = 0, idle_all = 0;

#ifdef __APPLE__
	// macOS Implementation (Mach Kernel)
	host_cpu_load_info_data_t cpu_info;
	mach_msg_type_number_t    count = HOST_CPU_LOAD_INFO_COUNT;

	if (host_statistics64(mach_host_self(), HOST_CPU_LOAD_INFO, (host_info64_t)&cpu_info, &count) == KERN_SUCCESS) {
		total    = cpu_info.cpu_ticks[CPU_STATE_USER] + cpu_info.cpu_ticks[CPU_STATE_NICE] + cpu_info.cpu_ticks[CPU_STATE_SYSTEM] + cpu_info.cpu_ticks[CPU_STATE_IDLE];
		idle_all = cpu_info.cpu_ticks[CPU_STATE_IDLE];
	}

#elif defined(__linux__)
	// Linux Implementation (Procfs)
	std::ifstream file("/proc/stat");
	std::string   label;
	uint64_t      user, nice, system, idle, iowait, irq, softirq, steal;

	if (file >> label >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal) {
		total    = user + nice + system + idle + iowait + irq + softirq + steal;
		idle_all = idle + iowait;
	}
#else
#error "Platform not supported"
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
	static std::vector<std::pair<uint64_t, uint64_t>> prev_state; // {total, idle}
	std::ifstream                                     file("/proc/stat");
	std::string                                       line;
	std::vector<double>                               loads;

	int core_idx = 0;
	while (std::getline(file, line)) {
		if (line.compare(0, 3, "cpu") == 0 && line.size() > 3 && std::isdigit(line[3])) {
			std::string label;
			uint64_t    user, nice, system, idle, iowait, irq, softirq, steal;

			// We can use a simple parser: stringstream is easy, but manual parsing is fine too.
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
	// macOS Implementation
	struct mach_task_basic_info info;
	mach_msg_type_number_t      infoCount = MACH_TASK_BASIC_INFO_COUNT;

	if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &infoCount) == KERN_SUCCESS) {
		return (uint64_t)info.resident_size;
	}
	return 0;

#elif defined(__linux__)
	// Linux Implementation
	std::ifstream f("/proc/self/statm");
	uint64_t      pages;

	if (f >> pages >> pages) {
		static long page_size = sysconf(_SC_PAGESIZE);
		return pages * static_cast<uint64_t>(page_size);
	}
	return 0;
#else
#error "Platform not supported"
	return 0;
#endif
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

enum class LogLevel : uint8_t {
	DEBUG,
	INFO,
	WARNING,
	ERROR,
	FATAL,
};

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

class Logger {
      public:
	static Logger& instance() {
		static Logger instance;
		return instance;
	}

	void init(int rank, const std::filesystem::path& logs_dir_path = "logs") {
		{
			std::lock_guard lock(mtx_);
			rank_ = rank;

			std::filesystem::create_directories(logs_dir_path);
			auto log_file_path = logs_dir_path / std::format("node_{:03d}.log", rank_);
			log_file_.open(log_file_path, std::ios::out | std::ios::trunc);

			if (!log_file_.is_open()) {
				std::cerr << "Failed to open log file: " << log_file_path << std::endl;
			}
		}
		log_internal(LogLevel::INFO, std::format("logger initialized on node {} with PID - {}", rank_, ::getpid()));
	}

	void set_log_level(LogLevel level) {
		log_level_ = level;
	}

	void set_log_to_console(bool value) {
		log_to_console_ = value;
	}

	template <typename... Args>
	void log(LogLevel level, const std::format_string<Args...> fmt_string, Args&&... args) {
		if (level < log_level_)
			return;
		auto message = std::format(fmt_string, std::forward<Args>(args)...);
		log_internal(level, message);
	}

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

	template <typename... Args>
	void fatal(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::FATAL, fmt_string, std::forward<Args>(args)...);
	}

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
		auto    now  = std::chrono::system_clock::now();
		auto    ms   = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
		auto    time = std::chrono::system_clock::to_time_t(now);
		std::tm tm;
		localtime_r(&time, &tm);
		return std::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, ms.count());
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

inline Logger& logger() {
	return Logger::instance();
}

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <stop_token>
#include <thread>
#include <optional>
#include <unordered_map>

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

class StageExecutor {
      public:
	StageDescriptor            sd_;
	std::shared_ptr<StageBase> stage_;
	ConcurrentQueue<Message>   queue_;
	int                        rank_;
	StageMetrics               metrics;

	std::vector<ConcurrentQueue<Message>*> local_next_queues_;
	std::vector<int>                       remote_next_ranks_;

	uint32_t expected_eos_count_ = 0;

	bool     has_remote_pred_         = false;
	uint32_t num_local_predecessors_  = 0;
	uint32_t num_remote_predecessors_ = 0;

	std::atomic<bool> mpi_receiver_should_stop_{false};

	StageExecutor(StageDescriptor sd, std::shared_ptr<StageBase> stage, int rank) :
	        sd_(std::move(sd)), stage_(std::move(stage)), rank_(rank) {
		metrics.stage_id = sd_.id;
	}

	void resolve_connections(
	        const std::unordered_map<uint32_t, StageExecutor*>& local_executors) {
		bool found_local_next = false;
		for (int dest_rank : sd_.next_stage_ranks) {
			if (dest_rank == rank_ && !found_local_next) {
				auto it = local_executors.find(sd_.next_stage_id);
				if (it != local_executors.end()) {
					local_next_queues_.push_back(&it->second->queue_);
					found_local_next = true;
					logger().debug("Stage {} → Stage {}: LOCAL bypass",
					               sd_.id,
					               sd_.next_stage_id);
					continue;
				}
			}
			if (dest_rank != rank_) {
				remote_next_ranks_.push_back(dest_rank);
				logger().debug("Stage {} → rank {}: REMOTE MPI",
				               sd_.id,
				               dest_rank);
			}
		}

		num_local_predecessors_  = 0;
		num_remote_predecessors_ = 0;
		for (int pred_rank : sd_.previous_stage_ranks) {
			if (pred_rank == rank_)
				++num_local_predecessors_;
			else
				++num_remote_predecessors_;
		}
		has_remote_pred_ = (num_remote_predecessors_ > 0);

		expected_eos_count_ = num_local_predecessors_
		                      + (has_remote_pred_ ? 1 : 0);

		logger().debug(
		        "Stage {} connections: {} local pred, {} remote pred, "
		        "expecting {} EOS, {} local next, {} remote next",
		        sd_.id,
		        num_local_predecessors_,
		        num_remote_predecessors_,
		        expected_eos_count_,
		        local_next_queues_.size(),
		        remote_next_ranks_.size());
	}

	void mpi_receiver_loop() {
		if (num_remote_predecessors_ == 0)
			return;

		uint32_t eos_received = 0;

		logger().debug("Stage {} MPI receiver: expecting {} remote EOS on tag {}",
		               sd_.id,
		               num_remote_predecessors_,
		               sd_.input_tag);

		while (!mpi_receiver_should_stop_.load(std::memory_order_relaxed)) {
			MPI_Status status;
			int        flag = 0;

			MPI_Iprobe(MPI_ANY_SOURCE, sd_.input_tag, MPI_COMM_WORLD, &flag, &status);

			if (!flag) {
				std::this_thread::sleep_for(std::chrono::microseconds(50));
				continue;
			}

			int count;
			MPI_Get_count(&status, MPI_BYTE, &count);

			std::vector<uint8_t> buf(count);
			{
				ScopedTimer timer(metrics.mpi_recv_time_us);
				MPI_Recv(buf.data(), count, MPI_BYTE, status.MPI_SOURCE, sd_.input_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			}

			if (count == 0) {
				++eos_received;
				logger().debug("Stage {} remote EOS {}/{} from rank {}",
				               sd_.id,
				               eos_received,
				               num_remote_predecessors_,
				               status.MPI_SOURCE);
				if (eos_received >= num_remote_predecessors_) {
					queue_.push(Message{.eos = true});
					logger().debug("Stage {} MPI receiver injecting EOS into queue",
					               sd_.id);
					break;
				}
				continue;
			}

			metrics.items_received.fetch_add(1, std::memory_order_relaxed);
			metrics.bytes_received.fetch_add(
			        static_cast<uint64_t>(count),
			        std::memory_order_relaxed);

			Message msg;
			msg.payload = deserialize_payload(buf);
			queue_.push(std::move(msg));
		}

		logger().debug("Stage {} MPI receiver exiting", sd_.id);
	}

	void send_to_next(Payload res) {
		if (!remote_next_ranks_.empty()) {
			auto buf = serialize_payload(res);
			for (int dest : remote_next_ranks_) {
				ScopedTimer timer(metrics.mpi_send_time_us);
				MPI_Send(buf.data(), static_cast<int>(buf.size()), MPI_BYTE, dest, sd_.output_tag, MPI_COMM_WORLD);
				metrics.items_sent.fetch_add(1, std::memory_order_relaxed);
				metrics.bytes_sent.fetch_add(
				        static_cast<uint64_t>(buf.size()),
				        std::memory_order_relaxed);
			}
		}

		for (size_t i = 0; i < local_next_queues_.size(); ++i) {
			if (i + 1 < local_next_queues_.size()) {
				local_next_queues_[i]->push(Message{.payload = res});
			} else {
				local_next_queues_[i]->push(Message{.payload = std::move(res)});
			}
			metrics.items_sent.fetch_add(1, std::memory_order_relaxed);
		}
	}

	void send_eos_to_next() {
		for (auto* q : local_next_queues_) {
			q->push(Message{.eos = true});
			logger().debug("Stage {} sent LOCAL EOS to next", sd_.id);
		}
		for (int dest : remote_next_ranks_) {
			MPI_Send(nullptr, 0, MPI_BYTE, dest, sd_.output_tag, MPI_COMM_WORLD);
			logger().debug("Stage {} sent REMOTE EOS to rank {}", sd_.id, dest);
		}
	}

	bool pop_data_or_drain_eos(Message& msg) {
		uint32_t eos_seen = 0;
		while (true) {
			{
				ScopedTimer timer(metrics.idle_time_us);
				if (!queue_.pop(msg))
					return false;
			}
			if (!msg.eos) {
				return true;
			}
			++eos_seen;
			logger().debug("Stage {} EOS {}/{}", sd_.id, eos_seen, expected_eos_count_);
			if (eos_seen >= expected_eos_count_) {
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
		               expected_eos_count_);
		Message msg;
		while (pop_data_or_drain_eos(msg)) {
			Payload item;
			{
				ScopedTimer timer(metrics.processing_time_us);
				item = stage_->execute(msg.payload);
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
		               expected_eos_count_);

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
						got = queue_.pop(msg);
					}
					if (!got)
						break;

					if (msg.eos) {
						uint32_t eos_count = total_eos_seen.fetch_add(
						                             1,
						                             std::memory_order_acq_rel)
						                     + 1;
						logger().debug("Stage {} worker {} saw EOS ({}/{})",
						               sd_.id,
						               worker_id,
						               eos_count,
						               expected_eos_count_);
						if (eos_count < expected_eos_count_) {
							continue;
						}
						for (uint32_t k = 1; k < sd_.assigned_threads; ++k) {
							queue_.push(Message{.eos = true});
						}
						break;
					}

					Payload res;
					{
						ScopedTimer timer(metrics.processing_time_us);
						res = stage_->execute(std::move(msg.payload));
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
					results.push(Message{.eos = true});
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
		               expected_eos_count_);
		Message msg;
		while (pop_data_or_drain_eos(msg)) {
			{
				ScopedTimer timer(metrics.processing_time_us);
				stage_->consume(msg.payload);
			}
			metrics.items_processed.fetch_add(1, std::memory_order_relaxed);
		}
		logger().debug("Stage {} SINK done", sd_.id);
	}

	void run_stage() {
		std::optional<std::jthread> receiver;
		if (sd_.type != StageType::SOURCE && has_remote_pred_) {
			receiver.emplace([this](std::stop_token) {
				this->mpi_receiver_loop();
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

		mpi_receiver_should_stop_.store(true, std::memory_order_release);
		if (receiver)
			receiver->join();

		logger().debug("Stage {} fully complete", sd_.id);
	}
};

#include <memory>
#include <mpi.h>
#include <unordered_map>

#include <format>

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

inline std::string plan_view(const WorkflowPlan& plan) {
	std::string out;
	out += std::format("Workflow: {} stages\n", plan.num_stages);
	out += "┌────────┬────────────┬──────┬─────────┬─────────────────┬──────────────────┐\n";
	out += "│ Stage  │ Type       │ Node │ Threads │ Prev (node)     │ Next (node)      │\n";
	out += "├────────┼────────────┼──────┼─────────┼─────────────────┼──────────────────┤\n";
	for (auto& stage : plan.stages) {
		auto prev_str = stage.previous_stage_ranks.empty() ? "-" : ([&] {
			std::string res;
			for (auto& prev : stage.previous_stage_ranks) {
				res += std::format("{:>4} ", prev);
			}
			return res;
		}());
		auto next_str = stage.next_stage_ranks.empty() ? "-" : ([&] {
			std::string res;
			for (auto& next : stage.next_stage_ranks) {
				res += std::format("{:>4} ", next);
			}
			return res;
		}());
		out += std::format("│ {:>6} │ {:<10} │ {:>4} │ {:>7} │ {:<15} │ {:<16} │\n",
		                   stage.id,
		                   stage_type_name(stage.type),
		                   stage.assigned_node,
		                   stage.assigned_threads,
		                   prev_str,
		                   next_str);
	}
	out += "└────────┴────────────┴──────┴─────────┴─────────────────┴──────────────────┘\n";
	return out;
}

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

constexpr int TELEMETRY_DEFAULT_PORT = 9100;

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
	                          std::filesystem::path output_dir     = "metrics",
	                          int                   telemetry_port = TELEMETRY_DEFAULT_PORT) :
	        world_size_(world_size), output_dir_(std::move(output_dir)), latest_(world_size) {
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

		f << "{\"timestamp\":\"" << current_time_str() << "\",\"nodes\":[";

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

	static std::string current_time_str() {
		auto    now  = std::chrono::system_clock::now();
		auto    time = std::chrono::system_clock::to_time_t(now);
		std::tm tm;
		localtime_r(&time, &tm);
		return std::format("{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}",
		                   tm.tm_year + 1900,
		                   tm.tm_mon + 1,
		                   tm.tm_mday,
		                   tm.tm_hour,
		                   tm.tm_min,
		                   tm.tm_sec);
	}
};

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

		std::vector<std::shared_ptr<StageExecutor>>  executors;
		std::unordered_map<uint32_t, StageExecutor*> local_map;

		for (auto& sd : wp.stages) {
			if (static_cast<int>(sd.assigned_node) != rank)
				continue;

			auto stage_ptr = pipeline_.stages_[sd.id];
			auto exec      = std::make_shared<StageExecutor>(sd, stage_ptr, rank);
			executors.push_back(exec);
			local_map[sd.id] = exec.get();
		}

		for (auto& exec : executors) {
			exec->resolve_connections(local_map);
		}

		std::unique_ptr<MonitorCollector> collector;
		if (rank == 0) {
			collector = std::make_unique<MonitorCollector>(world_size);
			collector->start();
			logger().debug("Monitor collector started");
		}

		NodeReporter reporter(rank);
		for (auto& exec : executors) {
			reporter.track_stage(&exec->metrics);
		}

		std::vector<std::jthread> stage_threads;
		for (auto& exec : executors) {
			stage_threads.emplace_back([exec](std::stop_token) {
				exec->run_stage();
			});
		}

		reporter.start();

		for (auto& t : stage_threads)
			t.join();

		logger().debug("All stages done on node {}", rank);

		reporter.stop();
		logger().debug("Reporter stopped on node {}", rank);

		if (rank == 0 && collector) {
			collector->stop();
			logger().debug("Monitor collector stopped");
		}

		MPI_Barrier(MPI_COMM_WORLD);
		logger().debug("Engine finished on node {}", rank);
	}
};

