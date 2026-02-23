#pragma once

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "../skeletons/logical_plan_node.hpp"
#include "../skeletons/pipeline.hpp"
#include "execution.hpp"
#include "logical_plan.hpp"
#include "orchestrator.hpp"
#include "physical_plan.hpp"
#include "worker.hpp"
#include <mpi.h>

namespace hell::engine {

/**
 * \brief Coordinator/worker engine that stores a workflow and maps it to ranks.
 */
class Engine {
      public:
	/** \brief Constructs an empty engine instance. */
	Engine() = default;

	/**
	 * \brief Sets the user pipeline that will be planned on execute.
	 * \param pipeline Typed user pipeline.
	 * \return Reference to this engine.
	 */
	template <typename... Stages>
	Engine& set_pipeline(skeletons::Pipeline<Stages...> pipeline) {
		using PipelineT = skeletons::Pipeline<Stages...>;
		user_pipeline_  = std::make_unique<UserPipelineModel<PipelineT>>(std::move(pipeline));
		return *this;
	}

	/**
	 * \brief Clears the pipeline and cached plans.
	 */
	void clear() {
		user_pipeline_.reset();
		logical_plan_.clear();
		physical_plan_.clear();
	}

	/**
	 * \brief Executes one planning step for the current rank.
	 * \return Rank-local execution report.
	 */
	[[nodiscard]] ExecutionReport execute() {
		const RankInfo rank_info = discover_rank_info();

		if (!user_pipeline_) {
			logical_plan_.clear();
			physical_plan_.clear();
			return execute_worker(rank_info.rank, rank_info.world_size, 0, logical_plan_, physical_plan_);
		}

		const auto erased_user_plan = user_pipeline_->erase();
		logical_plan_               = build_logical_plan(erased_user_plan);
		physical_plan_              = map_logical_plan_to_cluster(logical_plan_, rank_info.world_size);

		const size_t user_plan_nodes = user_pipeline_->size();

		if (rank_info.rank != 0) {
			return execute_worker(rank_info.rank, rank_info.world_size, user_plan_nodes, logical_plan_, physical_plan_);
		}

		return execute_orchestrator(user_plan_nodes, rank_info.rank, rank_info.world_size, logical_plan_, physical_plan_);
	}

	/**
	 * \brief Returns the last built logical plan.
	 * \return Reference to cached logical plan.
	 */
	[[nodiscard]] const LogicalPlan& logical_plan() const {
		return logical_plan_;
	}

	/**
	 * \brief Returns the last built physical plan.
	 * \return Reference to cached physical plan.
	 */
	[[nodiscard]] const PhysicalPlan& physical_plan() const {
		return physical_plan_;
	}

      private:
	struct UserPipelineConcept {
		virtual ~UserPipelineConcept() = default;

		[[nodiscard]] virtual std::vector<skeletons::LogicalPlanNode> erase() const = 0;
		[[nodiscard]] virtual size_t                                  size() const  = 0;
	};

	template <typename Pipeline>
	struct UserPipelineModel final : UserPipelineConcept {
		explicit UserPipelineModel(Pipeline value) : value_(std::move(value)) {
		}

		[[nodiscard]] std::vector<skeletons::LogicalPlanNode> erase() const override {
			return erase_pipeline(value_);
		}

		[[nodiscard]] size_t size() const override {
			return Pipeline::pipeline_size();
		}

		Pipeline value_;
	};

	template <typename Pipeline>
	[[nodiscard]] static std::vector<skeletons::LogicalPlanNode> erase_pipeline(const Pipeline& pipeline) {
		std::vector<skeletons::LogicalPlanNode> nodes;
		nodes.reserve(Pipeline::pipeline_size());
		std::apply([&nodes](const auto&... stages) {
			(nodes.push_back(skeletons::LogicalPlanNode::from(stages)), ...);
		},
		           pipeline.stages());
		return nodes;
	}

	[[nodiscard]] static RankInfo discover_rank_info() {
		RankInfo info{};
		int      initialized = 0;
		MPI_Initialized(&initialized);
		if (initialized) {
			MPI_Comm_rank(MPI_COMM_WORLD, &info.rank);
			MPI_Comm_size(MPI_COMM_WORLD, &info.world_size);
		}
		return info;
	}

	std::unique_ptr<UserPipelineConcept> user_pipeline_;
	LogicalPlan                          logical_plan_;
	PhysicalPlan                         physical_plan_;
};

} // namespace hell::engine
