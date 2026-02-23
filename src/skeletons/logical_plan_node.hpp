#pragma once

#include <algorithm>
#include <concepts>
#include <memory>
#include <ostream>
#include <span>
#include <sstream>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <vector>

#include "../core/farm.hpp"
#include "../transport/channel.hpp"
#include "farm.hpp"
#include "sink.hpp"
#include "source.hpp"
#include "stage.hpp"

namespace hell::skeletons {

/** \brief Serialized payload type used by erased node executors. */
using ErasedMessage       = transport::ByteBuffer;
/** \brief Type-erased channel reader for serialized payloads. */
using ErasedChannelReader = transport::ChannelReader<ErasedMessage>;
/** \brief Type-erased channel writer for serialized payloads. */
using ErasedChannelWriter = transport::ChannelWriter<ErasedMessage>;

/**
 * \brief Compile-time mapping from skeleton type to `LogicalPlanNode::Kind`.
 */
template <typename T>
struct LogicalPlanNodeKindTag;

/** \brief Trait: true when `T` is a `Source` skeleton. */
template <typename T>
struct IsSourceSkeleton : std::false_type {
};

template <hell::transport::Serializable Out>
struct IsSourceSkeleton<Source<Out>> : std::true_type {
};

/** \brief Trait: true when `T` is a `Stage` skeleton. */
template <typename T>
struct IsStageSkeleton : std::false_type {
};

template <hell::transport::Serializable In, hell::transport::Serializable Out>
struct IsStageSkeleton<Stage<In, Out>> : std::true_type {
};

/** \brief Trait: true when `T` is a `Farm` skeleton. */
template <typename T>
struct IsFarmSkeleton : std::false_type {
};

template <hell::transport::Serializable In, hell::transport::Serializable Out>
struct IsFarmSkeleton<Farm<In, Out>> : std::true_type {
};

/** \brief Trait: true when `T` is a `Sink` skeleton. */
template <typename T>
struct IsSinkSkeleton : std::false_type {
};

template <hell::transport::Serializable In>
struct IsSinkSkeleton<Sink<In>> : std::true_type {
};

/**
 * \brief Type-erased logical skeleton node plus metadata and execution entrypoint.
 */
class LogicalPlanNode {
      public:
	/**
	 * \brief Logical node category.
	 */
	enum class Kind {
		SOURCE,
		STAGE,
		FARM,
		SINK,
		UNKNOWN
	};

	/**
	 * \brief Creates an erased node from a concrete skeleton.
	 * \tparam Skeleton Concrete skeleton type.
	 * \param skeleton Concrete skeleton instance.
	 * \return Type-erased logical plan node.
	 */
	template <typename Skeleton>
	static LogicalPlanNode from(Skeleton skeleton) {
		using NodeT = std::remove_cvref_t<Skeleton>;
		return LogicalPlanNode(std::move(skeleton), classify<NodeT>());
	}

	LogicalPlanNode(const LogicalPlanNode&)            = default;
	LogicalPlanNode(LogicalPlanNode&&) noexcept        = default;
	LogicalPlanNode& operator=(const LogicalPlanNode&) = default;
	LogicalPlanNode& operator=(LogicalPlanNode&&) noexcept = default;
	~LogicalPlanNode()                                     = default;

	/** \brief Returns node kind metadata. */
	[[nodiscard]] Kind kind() const {
		return kind_;
	}

	/** \brief Returns concrete C++ type metadata for this node. */
	[[nodiscard]] std::type_index concrete_type() const {
		return concrete_type_;
	}

	/** \brief Returns declared input type metadata for this node. */
	[[nodiscard]] std::type_index input_type() const {
		return input_type_;
	}

	/** \brief Returns declared output type metadata for this node. */
	[[nodiscard]] std::type_index output_type() const {
		return output_type_;
	}

	/** \brief Returns debug type name for the concrete skeleton. */
	[[nodiscard]] std::string_view debug_name() const {
		return debug_name_;
	}

	/**
	 * \brief Executes this node using erased channels.
	 * \param input Optional input channel depending on node kind.
	 * \param output Optional output channel depending on node kind.
	 * \param st Stop token controlling cooperative cancellation.
	 */
	void execute(ErasedChannelReader* input, ErasedChannelWriter* output, std::stop_token st = {}) const {
		if (model_) {
			model_->execute(input, output, st);
		}
	}

	/**
	 * \brief Checks whether this erased node stores the requested skeleton type.
	 * \tparam Skeleton Skeleton type to test.
	 * \return True if stored type matches `Skeleton`.
	 */
	template <typename Skeleton>
	[[nodiscard]] bool holds() const {
		return model_ && model_->concrete_type() == std::type_index(typeid(std::remove_cvref_t<Skeleton>));
	}

	/**
	 * \brief Returns pointer to stored skeleton when type matches.
	 * \tparam Skeleton Requested concrete skeleton type.
	 * \return Pointer to stored value or `nullptr` on mismatch.
	 */
	template <typename Skeleton>
	[[nodiscard]] const std::remove_cvref_t<Skeleton>* as() const {
		if (!holds<Skeleton>()) {
			return nullptr;
		}
		return &static_cast<const Model<std::remove_cvref_t<Skeleton>>&>(*model_).value;
	}

      private:
	struct Concept {
		virtual ~Concept() = default;

		[[nodiscard]] virtual std::type_index concrete_type() const                                       = 0;
		virtual void                            execute(ErasedChannelReader*, ErasedChannelWriter*, std::stop_token) const = 0;
	};

	template <typename Skeleton>
	struct Model final : Concept {
		explicit Model(Skeleton v) : value(std::move(v)) {
		}

		[[nodiscard]] std::type_index concrete_type() const override {
			return std::type_index(typeid(Skeleton));
		}

		void execute(ErasedChannelReader* input, ErasedChannelWriter* output, std::stop_token st) const override {
			execute_impl(value, input, output, st);
		}

		Skeleton value;
	};

	template <typename Skeleton>
	static void execute_impl(const Skeleton& skeleton, ErasedChannelReader* input, ErasedChannelWriter* output, std::stop_token st) {
		using NodeT = std::remove_cvref_t<Skeleton>;
		if constexpr (IsSourceSkeleton<NodeT>::value) {
			using Out = typename NodeT::OutputType;
			if (output == nullptr) {
				throw std::invalid_argument("LogicalPlanNode::execute source requires output channel");
			}
			auto generator = skeleton.generator_function();
			while (!st.stop_requested()) {
				auto item = generator();
				if (!item.has_value()) {
					break;
				}
				if (!output->send(transport::serialize<Out>(*item), st)) {
					break;
				}
			}
			output->close();
		} else if constexpr (IsStageSkeleton<NodeT>::value) {
			using In  = typename NodeT::InputType;
			using Out = typename NodeT::OutputType;
			if (input == nullptr || output == nullptr) {
				throw std::invalid_argument("LogicalPlanNode::execute stage requires input and output channels");
			}
			auto worker = skeleton.work_function();
			while (!st.stop_requested()) {
				auto item = input->recv(st);
				if (!item.has_value()) {
					break;
				}
				auto decoded = transport::deserialize<In>(std::move(*item));
				auto result  = worker(std::move(decoded));
				if (!output->send(transport::serialize<Out>(result), st)) {
					break;
				}
			}
			output->close();
		} else if constexpr (IsFarmSkeleton<NodeT>::value) {
			using In  = typename NodeT::InputType;
			using Out = typename NodeT::OutputType;
			if (input == nullptr || output == nullptr) {
				throw std::invalid_argument("LogicalPlanNode::execute farm requires input and output channels");
			}

			const size_t workers_hint = skeleton.policy().num_workers;
			const size_t workers      = workers_hint > 0 ? workers_hint : std::max<size_t>(1, std::thread::hardware_concurrency());

			core::FarmExecutor<ErasedMessage, ErasedMessage> executor([worker = skeleton.work_function()](ErasedMessage item) {
				auto decoded = transport::deserialize<In>(std::move(item));
				auto result  = worker(std::move(decoded));
				return transport::serialize<Out>(result);
			},
			                                                workers);
			executor.run(*input, *output, st);
		} else if constexpr (IsSinkSkeleton<NodeT>::value) {
			using In = typename NodeT::InputType;
			if (input == nullptr) {
				throw std::invalid_argument("LogicalPlanNode::execute sink requires input channel");
			}
			auto consumer = skeleton.consumer_function();
			while (!st.stop_requested()) {
				auto item = input->recv(st);
				if (!item.has_value()) {
					break;
				}
				auto decoded = transport::deserialize<In>(std::move(*item));
				consumer(std::move(decoded));
			}
		}
	}

	template <typename Skeleton>
	explicit LogicalPlanNode(Skeleton skeleton, Kind kind)
	    : kind_(kind),
	      concrete_type_(typeid(std::remove_cvref_t<Skeleton>)),
	      input_type_(typeid(typename std::remove_cvref_t<Skeleton>::InputType)),
	      output_type_(typeid(typename std::remove_cvref_t<Skeleton>::OutputType)),
	      debug_name_(typeid(std::remove_cvref_t<Skeleton>).name()),
	      model_(std::make_shared<Model<std::remove_cvref_t<Skeleton>>>(std::move(skeleton))) {
	}

	template <typename Skeleton>
	static consteval Kind classify() {
		return LogicalPlanNodeKindTag<std::remove_cvref_t<Skeleton>>::value;
	}

	Kind                     kind_          = Kind::UNKNOWN;
	std::type_index          concrete_type_ = typeid(void);
	std::type_index          input_type_    = typeid(void);
	std::type_index          output_type_   = typeid(void);
	std::string_view         debug_name_;
	std::shared_ptr<Concept> model_;
};

template <typename T>
struct LogicalPlanNodeKindTag {
	static constexpr LogicalPlanNode::Kind value = LogicalPlanNode::Kind::UNKNOWN;
};

template <hell::transport::Serializable Out>
struct LogicalPlanNodeKindTag<Source<Out>> {
	static constexpr LogicalPlanNode::Kind value = LogicalPlanNode::Kind::SOURCE;
};

template <hell::transport::Serializable In, hell::transport::Serializable Out>
struct LogicalPlanNodeKindTag<Stage<In, Out>> {
	static constexpr LogicalPlanNode::Kind value = LogicalPlanNode::Kind::STAGE;
};

template <hell::transport::Serializable In, hell::transport::Serializable Out>
struct LogicalPlanNodeKindTag<Farm<In, Out>> {
	static constexpr LogicalPlanNode::Kind value = LogicalPlanNode::Kind::FARM;
};

template <hell::transport::Serializable In>
struct LogicalPlanNodeKindTag<Sink<In>> {
	static constexpr LogicalPlanNode::Kind value = LogicalPlanNode::Kind::SINK;
};

/**
 * \brief Converts node-kind enum value to a stable string label.
 * \param kind Node kind.
 * \return String literal for the provided kind.
 */
[[nodiscard]] constexpr std::string_view logical_plan_node_kind_name(LogicalPlanNode::Kind kind) {
	switch (kind) {
		case LogicalPlanNode::Kind::SOURCE:
			return "SOURCE";
		case LogicalPlanNode::Kind::STAGE:
			return "STAGE";
		case LogicalPlanNode::Kind::FARM:
			return "FARM";
		case LogicalPlanNode::Kind::SINK:
			return "SINK";
		default:
			return "UNKNOWN";
	}
}

/**
 * \brief Builds a one-line human-readable description for a node.
 * \param node Node to describe.
 * \return Formatted description string.
 */
[[nodiscard]] inline std::string describe_node(const LogicalPlanNode& node) {
	std::ostringstream stream;
	stream << "kind=" << logical_plan_node_kind_name(node.kind()) << ", concrete=" << node.debug_name() << ", input="
	       << node.input_type().name() << ", output=" << node.output_type().name();
	return stream.str();
}

/**
 * \brief Prints a logical plan to an output stream.
 * \param out Target output stream.
 * \param plan Plan to print.
 */
inline void print_plan(std::ostream& out, std::span<const LogicalPlanNode> plan) {
	out << "Logical plan (" << plan.size() << " nodes)\n";
	for (size_t i = 0; i < plan.size(); ++i) {
		out << "[" << i << "] " << describe_node(plan[i]) << '\n';
	}
}

/**
 * \brief Prints a logical plan to an output stream.
 * \param out Target output stream.
 * \param plan Plan to print.
 */
inline void print_plan(std::ostream& out, const std::vector<LogicalPlanNode>& plan) {
	print_plan(out, std::span<const LogicalPlanNode>(plan.data(), plan.size()));
}

} // namespace hell::skeletons
