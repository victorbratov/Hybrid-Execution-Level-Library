#pragma once
#include <memory>
#include <vector>
#include "./stages.hpp"

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
