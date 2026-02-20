#pragma once
#include "../transport/serialization.hpp"
#include "../transport/channel.hpp"
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
