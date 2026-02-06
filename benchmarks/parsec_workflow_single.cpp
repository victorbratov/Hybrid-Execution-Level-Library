#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iostream>

namespace {

struct TradeTask {
	uint64_t id;
	double   spot;
	double   strike;
	double   rate;
	double   vol;
	double   maturity;
	uint32_t paths;
	uint32_t steps;
	uint64_t seed;
	int32_t  is_call;
};

struct PriceMoments {
	double mean;
	double variance;
};

struct BaselinePacket {
	TradeTask    trade;
	PriceMoments base;
};

struct BumpPacket {
	TradeTask    trade;
	PriceMoments base;
	PriceMoments up;
};

struct RiskResult {
	uint64_t id;
	double   price;
	double   delta;
	double   gamma;
	double   var95;
};

struct Rng64 {
	uint64_t state;

	explicit Rng64(uint64_t seed) : state(seed ? seed : 0x9E3779B97F4A7C15ULL) {
	}

	uint64_t next_u64() {
		state ^= state >> 12;
		state ^= state << 25;
		state ^= state >> 27;
		return state * 2685821657736338717ULL;
	}

	double uniform() {
		constexpr double inv = 1.0 / static_cast<double>(1ULL << 53);
		return static_cast<double>(next_u64() >> 11) * inv;
	}
};

inline TradeTask make_trade(uint64_t i, uint32_t paths, uint32_t steps) {
	const double base = static_cast<double>(i % 1000U);
	return TradeTask{
	        i,
	        80.0 + std::fmod(base * 0.13, 60.0),
	        70.0 + std::fmod(base * 0.11, 70.0),
	        0.005 + std::fmod(base * 0.00003, 0.035),
	        0.12 + std::fmod(base * 0.0002, 0.45),
	        0.25 + std::fmod(base * 0.0007, 2.0),
	        paths,
	        steps,
	        0xD1B54A32D192ED03ULL ^ (i * 0x9E3779B97F4A7C15ULL),
	        static_cast<int32_t>(i & 1ULL),
	};
}

inline PriceMoments simulate_price(const TradeTask& t, double spot_shift) {
	Rng64 rng(t.seed ^ static_cast<uint64_t>((spot_shift + 2.0) * 1000003.0));

	const double dt = t.maturity / static_cast<double>(std::max<uint32_t>(1, t.steps));
	const double sqrt_dt = std::sqrt(dt);
	const double drift = (t.rate - 0.5 * t.vol * t.vol) * dt;
	const double diffusion = t.vol * sqrt_dt;
	const double discount = std::exp(-t.rate * t.maturity);

	double sum = 0.0;
	double sum_sq = 0.0;

	for (uint32_t p = 0; p < t.paths; ++p) {
		double s = t.spot * (1.0 + spot_shift);
		for (uint32_t step = 0; step < t.steps; ++step) {
			double u1 = std::max(1e-12, rng.uniform());
			double u2 = rng.uniform();
			double z = std::sqrt(-2.0 * std::log(u1)) * std::cos(6.283185307179586 * u2);
			s *= std::exp(drift + diffusion * z);
		}

		double payoff = 0.0;
		if (t.is_call == 1) {
			payoff = std::max(0.0, s - t.strike);
		} else {
			payoff = std::max(0.0, t.strike - s);
		}

		const double pv = payoff * discount;
		sum += pv;
		sum_sq += pv * pv;
	}

	const double n = static_cast<double>(t.paths);
	const double mean = sum / n;
	const double variance = std::max(0.0, sum_sq / n - mean * mean);
	return PriceMoments{mean, variance};
}

inline BaselinePacket stage_baseline(const TradeTask& trade) {
	return BaselinePacket{trade, simulate_price(trade, 0.0)};
}

inline BumpPacket stage_up_bump(const BaselinePacket& in, double bump) {
	return BumpPacket{in.trade, in.base, simulate_price(in.trade, bump)};
}

inline RiskResult stage_risk(const BumpPacket& in, double bump) {
	const PriceMoments down = simulate_price(in.trade, -bump);
	const double delta = (in.up.mean - down.mean) / (2.0 * in.trade.spot * bump);
	const double gamma = (in.up.mean - 2.0 * in.base.mean + down.mean) /
	                     (in.trade.spot * in.trade.spot * bump * bump);
	const double var95 = in.base.mean - 1.6448536269514722 * std::sqrt(std::max(0.0, in.base.variance));
	return RiskResult{in.trade.id, in.base.mean, delta, gamma, var95};
}

} // namespace

int main(int argc, char** argv) {
	const uint64_t trades = (argc > 1) ? std::stoull(argv[1]) : 10000ULL;
	const uint32_t paths = (argc > 2) ? static_cast<uint32_t>(std::stoul(argv[2])) : 4096U;
	const uint32_t steps = (argc > 3) ? static_cast<uint32_t>(std::stoul(argv[3])) : 96U;
	constexpr double bump = 0.01;

	auto start = std::chrono::steady_clock::now();

	double checksum = 0.0;
	for (uint64_t i = 0; i < trades; ++i) {
		TradeTask t = make_trade(i, paths, steps);
		BaselinePacket b0 = stage_baseline(t);
		BumpPacket b1 = stage_up_bump(b0, bump);
		RiskResult out = stage_risk(b1, bump);
		checksum += out.price + out.delta + out.gamma + out.var95;
	}

	auto end = std::chrono::steady_clock::now();
	auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

	std::cout << "benchmark: parsec_workflow_single\n";
	std::cout << "scenario: monte_carlo_risk\n";
	std::cout << "trades: " << trades << "\n";
	std::cout << "paths_per_trade: " << paths << "\n";
	std::cout << "timesteps: " << steps << "\n";
	std::cout << "stages: 3\n";
	std::cout << "elapsed_ms: " << elapsed_ms << "\n";
	std::cout << "throughput_trades_per_sec: "
	          << (trades * 1000.0 / static_cast<double>(std::max<long long>(1, elapsed_ms))) << "\n";
	std::cout << "throughput_opts_per_sec: "
	          << (trades * 1000.0 / static_cast<double>(std::max<long long>(1, elapsed_ms))) << "\n";
	std::cout << "checksum: " << checksum << "\n";

	return 0;
}
