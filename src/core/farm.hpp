#pragma once
#include "stage.hpp"
#include "../transport/channel.hpp"
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
