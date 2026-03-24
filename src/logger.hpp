#pragma once

#include "./commons.hpp"
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string_view>
#include <unistd.h>

/**
 * @enum LogLevel
 * @brief Represents the severity level of a log message.
 */
enum class LogLevel : uint8_t {
	DEBUG,
	INFO,
	WARNING,
	ERROR,
	FATAL,
};

/**
 * @brief Converts a LogLevel to its corresponding string representation.
 * @param level The LogLevel.
 * @return A string view of the log level name.
 */
constexpr std::string_view log_level_to_string(LogLevel level) {
	switch (level) {
		case LogLevel::DEBUG:
			return "DEBUG";
		case LogLevel::INFO:
			return "INFO";
		case LogLevel::WARNING:
			return "WARNING";
		case LogLevel::ERROR:
			return "ERROR";
		case LogLevel::FATAL:
			return "FATAL";
	}
	return "UNKNOWN";
}

/**
 * @class Logger
 * @brief Thread-safe, node-aware logging utility.
 *
 * Supports writing logs to both the console and a specific file per node.
 */
class Logger {
      public:
	/**
	 * @brief Gets the singleton Logger instance.
	 * @return Reference to the Logger singleton.
	 */
	static Logger& instance() {
		static Logger instance;
		return instance;
	}

	/**
	 * @brief Initializes the Logger for this specific node.
	 * @param rank The MPI rank of the node.
	 * @param logs_dir_path The directory to store log files. If empty, uses HELL_LOGS_DIR env or "logs/<datetime>".
	 */
	void init(int rank, std::filesystem::path logs_dir_path = "") {
		{
			std::lock_guard lock(mtx_);
			rank_ = rank;

			if (logs_dir_path.empty()) {
				logs_dir_path = get_env("HELL_LOGS_DIR", "logs/" + get_current_datetime_str());
			}

			std::filesystem::create_directories(logs_dir_path);
			auto log_file_path = logs_dir_path / std::format("node_{:03d}.log", rank_);
			log_file_.open(log_file_path, std::ios::out | std::ios::trunc);

			if (!log_file_.is_open()) {
				std::cerr << "Failed to open log file: " << log_file_path << std::endl;
			}
		}
		log_internal(LogLevel::INFO, std::format("logger initialized on node {} with PID - {}", rank_, ::getpid()));
	}

	/**
	 * @brief Sets the minimum severity level to be logged.
	 * @param level The minimum LogLevel.
	 */
	void set_log_level(LogLevel level) {
		log_level_ = level;
	}

	/**
	 * @brief Toggles whether to duplicate logs to standard output.
	 * @param value True to duplicate to stdout, false otherwise.
	 */
	void set_log_to_console(bool value) {
		log_to_console_ = value;
	}

	/**
	 * @brief Logs a formatted message with a specific severity.
	 * @tparam Args The types of the formatting arguments.
	 * @param level The severity level.
	 * @param fmt_string The format string.
	 * @param args The format arguments.
	 */
	template <typename... Args>
	void log(LogLevel level, const std::format_string<Args...> fmt_string, Args&&... args) {
		if (level < log_level_)
			return;
		auto message = std::format(fmt_string, std::forward<Args>(args)...);
		log_internal(level, message);
	}

	/**
	 * @brief Logs a DEBUG level message.
	 */
	template <typename... Args>
	void debug(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::DEBUG, fmt_string, std::forward<Args>(args)...);
	}

	template <typename... Args>
	void info(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::INFO, fmt_string, std::forward<Args>(args)...);
	}

	template <typename... Args>
	void warning(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::WARNING, fmt_string, std::forward<Args>(args)...);
	}

	template <typename... Args>
	void error(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::ERROR, fmt_string, std::forward<Args>(args)...);
	}

	/**
	 * @brief Logs a FATAL level message.
	 */
	template <typename... Args>
	void fatal(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::FATAL, fmt_string, std::forward<Args>(args)...);
	}

	/**
	 * @brief Writes a distinct textual block into the log file.
	 * @param header The title string of the block.
	 * @param body The contents of the block.
	 */
	void write_block(std::string_view header, std::string_view body) {
		std::lock_guard lock(mtx_);
		auto            ts    = timestamp();
		auto            block = std::format(
                        "[{}] [N{:03d}]\n╔══ {} ══\n{}\n╚══ end ══\n",
                        ts,
                        rank_,
                        header,
                        body);
		if (log_file_.is_open()) {
			log_file_ << block;
			log_file_.flush();
		}
	}

      private:
	Logger() = default;

	std::string timestamp() const {
		return get_timestamp_str();
	}

	void log_internal(LogLevel level, const std::string& message) {
		std::lock_guard lock(mtx_);
		auto            line = std::format("[{}] [N{:03d}] [{}] {}\n", timestamp(), rank_, log_level_to_string(level), message);
		if (log_file_.is_open()) {
			log_file_ << line;
			log_file_.flush();
		}
		if (log_to_console_) {
			std::cout << line;
		}
	}

	std::mutex    mtx_;
	std::ofstream log_file_;
	int           rank_           = -1;
	LogLevel      log_level_      = LogLevel::DEBUG;
	bool          log_to_console_ = true;
};

/**
 * @brief Global convenience method for accessing the logger instance.
 * @return Reference to the Logger singleton.
 */
inline Logger& logger() {
	return Logger::instance();
}
