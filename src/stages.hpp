#pragma once
#include <cstdint>
#include <concepts>
#include <functional>
#include <iterator>
#include <optional>
#include <type_traits>
#include <utility>
#include "./stage_descriptor.hpp"
#include "./payload.hpp"
#include "./generator.hpp"

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
