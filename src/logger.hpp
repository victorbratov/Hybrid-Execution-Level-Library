#pragma once

#include <cstdlib>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string_view>
#include <unistd.h>

enum class LogLevel : uint8_t {
	DEBUG,
	INFO,
	WARNING,
	ERROR,
	FATAL,
};

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

class Logger {
      public:
	static Logger& instance() {
		static Logger instance;
		return instance;
	}

	void init(int rank, const std::filesystem::path& logs_dir_path = "logs") {
		{
			std::lock_guard lock(mtx_);
			rank_ = rank;

			std::filesystem::create_directories(logs_dir_path);
			auto log_file_path = logs_dir_path / std::format("node_{:03d}.log", rank_);
			log_file_.open(log_file_path, std::ios::out | std::ios::trunc);

			if (!log_file_.is_open()) {
				std::cerr << "Failed to open log file: " << log_file_path << std::endl;
			}
		}
		log_internal(LogLevel::INFO, std::format("logger initialized on node {} with PID - {}", rank_, ::getpid()));
	}

	void set_log_level(LogLevel level) {
		log_level_ = level;
	}

	void set_log_to_console(bool value) {
		log_to_console_ = value;
	}

	template <typename... Args>
	void log(LogLevel level, const std::format_string<Args...> fmt_string, Args&&... args) {
		if (level < log_level_)
			return;
		auto message = std::format(fmt_string, std::forward<Args>(args)...);
		log_internal(level, message);
	}

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

	template <typename... Args>
	void fatal(const std::format_string<Args...> fmt_string, Args&&... args) {
		log(LogLevel::FATAL, fmt_string, std::forward<Args>(args)...);
	}

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
		auto    now  = std::chrono::system_clock::now();
		auto    ms   = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
		auto    time = std::chrono::system_clock::to_time_t(now);
		std::tm tm;
		localtime_r(&time, &tm);
		return std::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, ms.count());
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

inline Logger& logger() {
	return Logger::instance();
}
