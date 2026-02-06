#pragma once

#include <chrono>
#include <cstdlib>
#include <format>
#include <string>

/**
 * @brief Retrieves an environment variable or returns a default value.
 */
inline std::string get_env(const std::string& key, const std::string& default_value) {
	const char* val = std::getenv(key.c_str());
	return val ? std::string(val) : default_value;
}

/**
 * @brief Retrieves a boolean environment variable or returns a default value.
 */
inline bool get_env_bool(const std::string& key, bool default_value) {
	const char* val = std::getenv(key.c_str());
	if (!val)
		return default_value;
	std::string s(val);
	for (auto& c : s)
		c = static_cast<char>(std::tolower(c));
	return s == "true" || s == "1" || s == "yes" || s == "on";
}

/**
 * @brief Gets a filesystem-safe datetime string (e.g., "20260324_120000").
 */
inline std::string get_current_datetime_str() {
	auto    now  = std::chrono::system_clock::now();
	auto    time = std::chrono::system_clock::to_time_t(now);
	std::tm tm;
	localtime_r(&time, &tm);
	return std::format("{:04d}{:02d}{:02d}_{:02d}{:02d}{:02d}",
	                   tm.tm_year + 1900,
	                   tm.tm_mon + 1,
	                   tm.tm_mday,
	                   tm.tm_hour,
	                   tm.tm_min,
	                   tm.tm_sec);
}

/**
 * @brief Gets a human-readable timestamp for logging.
 */
inline std::string get_timestamp_str() {
	auto    now  = std::chrono::system_clock::now();
	auto    ms   = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
	auto    time = std::chrono::system_clock::to_time_t(now);
	std::tm tm;
	localtime_r(&time, &tm);
	return std::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}",
	                   tm.tm_year + 1900,
	                   tm.tm_mon + 1,
	                   tm.tm_mday,
	                   tm.tm_hour,
	                   tm.tm_min,
	                   tm.tm_sec,
	                   ms.count());
}
