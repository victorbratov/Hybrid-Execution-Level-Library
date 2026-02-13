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
