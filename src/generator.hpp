#pragma once

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
