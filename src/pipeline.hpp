#pragma once
#include <cassert>
#include <memory>
#include <type_traits>
#include <vector>
#include "./stages.hpp"

/**
 * @class Pipeline
 * @brief Represents a sequence of connected processing stages.
 */
template <typename Input, typename Output>
class Pipeline {
	using InputType  = Input;
	using OutputType = Output;

      public:
	std::vector<std::shared_ptr<StageBase>> stages_;

	Pipeline() = default;

	explicit Pipeline(std::shared_ptr<StageBase> stage) {
		stages_.push_back(stage);
	};

	template <typename OtherInput, typename OtherOutput>
	Pipeline(Pipeline<OtherInput, OtherOutput>&& other) : stages_(std::move(other.stages_)) {
	}

	Pipeline operator|(Pipeline&& rhs) {
		for (auto& stage : rhs.stages_) {
			stages_.push_back(stage);
		}
		return std::move(*this);
	};
};

template <StageCompatible L, StageCompatible R>
Pipeline<typename std::decay_t<L>::InputType, typename std::decay_t<R>::OutputType> operator|(L&& lhs, R&& rhs) {
	static_assert(
	        std::is_same_v<typename std::decay_t<L>::OutputType, typename std::decay_t<R>::InputType>,
	        "Stage Output type and next Stage Input type must match");
	static_assert(!is_sink_stage_v<L>, "LHS stage cannot be a SinkStage");
	static_assert(!is_source_stage_v<R>, "RHS stage cannot be a SourceStage");
	Pipeline<typename std::decay_t<L>::InputType, typename std::decay_t<R>::OutputType> pipeline;
	pipeline.stages_.push_back(std::make_shared<std::decay_t<L>>(std::forward<L>(lhs)));
	pipeline.stages_.push_back(std::make_shared<std::decay_t<R>>(std::forward<R>(rhs)));
	return pipeline;
};

template <typename PipelineInput, typename PipelineOutput, StageCompatible R>
Pipeline<PipelineInput, typename std::decay_t<R>::OutputType> operator|(Pipeline<PipelineInput, PipelineOutput>&& lhs, R&& rhs) {
	static_assert(!is_source_stage_v<R>, "RHS stage cannot be a SourceStage");
	static_assert(std::is_same_v<typename std::decay_t<R>::InputType, PipelineOutput>, "RHS stage Input type must match Pipeline Output type");
	lhs.stages_.push_back(std::make_shared<std::decay_t<R>>(std::forward<R>(rhs)));
	return std::move(lhs);
};
