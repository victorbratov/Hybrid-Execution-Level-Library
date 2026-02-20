#pragma once

#include <memory>
#include <concepts>
#include <ostream>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <vector>

#include "farm.hpp"
#include "sink.hpp"
#include "source.hpp"
#include "stage.hpp"

namespace hell::skeletons {

template <typename T>
struct LogicalPlanNodeKindTag;

class LogicalPlanNode {
      public:
	enum class Kind {
		SOURCE,
		STAGE,
		FARM,
		SINK,
		UNKNOWN
	};

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

	[[nodiscard]] Kind kind() const {
		return kind_;
	}

	[[nodiscard]] std::type_index concrete_type() const {
		return concrete_type_;
	}

	[[nodiscard]] std::type_index input_type() const {
		return input_type_;
	}

	[[nodiscard]] std::type_index output_type() const {
		return output_type_;
	}

	[[nodiscard]] std::string_view debug_name() const {
		return debug_name_;
	}

	template <typename Skeleton>
	[[nodiscard]] bool holds() const {
		return model_ && model_->concrete_type() == std::type_index(typeid(std::remove_cvref_t<Skeleton>));
	}

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

		[[nodiscard]] virtual std::type_index concrete_type() const = 0;
	};

	template <typename Skeleton>
	struct Model final : Concept {
		explicit Model(Skeleton v) : value(std::move(v)) {
		}

		[[nodiscard]] std::type_index concrete_type() const override {
			return std::type_index(typeid(Skeleton));
		}

		Skeleton value;
	};

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

	Kind                    kind_          = Kind::UNKNOWN;
	std::type_index         concrete_type_ = typeid(void);
	std::type_index         input_type_    = typeid(void);
	std::type_index         output_type_   = typeid(void);
	std::string_view        debug_name_;
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

[[nodiscard]] inline std::string describe_node(const LogicalPlanNode& node) {
	std::ostringstream stream;
	stream << "kind=" << logical_plan_node_kind_name(node.kind()) << ", concrete=" << node.debug_name() << ", input="
	       << node.input_type().name() << ", output=" << node.output_type().name();
	return stream.str();
}

inline void print_plan(std::ostream& out, std::span<const LogicalPlanNode> plan) {
	out << "Logical plan (" << plan.size() << " nodes)\n";
	for (size_t i = 0; i < plan.size(); ++i) {
		out << "[" << i << "] " << describe_node(plan[i]) << '\n';
	}
}

inline void print_plan(std::ostream& out, const std::vector<LogicalPlanNode>& plan) {
	print_plan(out, std::span<const LogicalPlanNode>(plan.data(), plan.size()));
}

} // namespace hell::skeletons
