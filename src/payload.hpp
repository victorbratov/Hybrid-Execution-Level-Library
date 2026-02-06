#pragma once
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
