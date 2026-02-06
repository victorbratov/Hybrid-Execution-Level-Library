#include <mpi.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr int TAG_WORK = 1;
constexpr int TAG_STOP = 2;
constexpr int TAG_RESULT = 3;

struct GrayImage {
	uint32_t             width = 0;
	uint32_t             height = 0;
	std::vector<uint8_t> pixels;
};

struct SobelInputChunk {
	uint32_t             chunk_id = 0;
	uint32_t             total_chunks = 0;
	uint32_t             image_width = 0;
	uint32_t             image_height = 0;
	uint32_t             core_start_row = 0;
	uint32_t             core_rows = 0;
	uint32_t             ext_start_row = 0;
	uint32_t             ext_rows = 0;
	std::vector<uint8_t> ext_pixels;

	void serialize(std::vector<uint8_t>& buf) const {
		auto append_u32 = [&](uint32_t value) {
			auto* ptr = reinterpret_cast<const uint8_t*>(&value);
			buf.insert(buf.end(), ptr, ptr + sizeof(value));
		};

		append_u32(chunk_id);
		append_u32(total_chunks);
		append_u32(image_width);
		append_u32(image_height);
		append_u32(core_start_row);
		append_u32(core_rows);
		append_u32(ext_start_row);
		append_u32(ext_rows);
		append_u32(static_cast<uint32_t>(ext_pixels.size()));
		buf.insert(buf.end(), ext_pixels.begin(), ext_pixels.end());
	}

	static SobelInputChunk deserialize(const uint8_t* data, size_t size) {
		const size_t fixed_u32_count = 9;
		const size_t header_size = sizeof(uint32_t) * fixed_u32_count;
		if (size < header_size) {
			throw std::runtime_error("Invalid SobelInputChunk payload header.");
		}

		auto read_u32 = [&](size_t offset) {
			uint32_t value = 0;
			std::memcpy(&value, data + offset, sizeof(value));
			return value;
		};

		SobelInputChunk chunk;
		chunk.chunk_id = read_u32(0 * sizeof(uint32_t));
		chunk.total_chunks = read_u32(1 * sizeof(uint32_t));
		chunk.image_width = read_u32(2 * sizeof(uint32_t));
		chunk.image_height = read_u32(3 * sizeof(uint32_t));
		chunk.core_start_row = read_u32(4 * sizeof(uint32_t));
		chunk.core_rows = read_u32(5 * sizeof(uint32_t));
		chunk.ext_start_row = read_u32(6 * sizeof(uint32_t));
		chunk.ext_rows = read_u32(7 * sizeof(uint32_t));
		const uint32_t pixels_size = read_u32(8 * sizeof(uint32_t));

		if (size < header_size + pixels_size) {
			throw std::runtime_error("Invalid SobelInputChunk payload size.");
		}

		chunk.ext_pixels.assign(data + header_size, data + header_size + pixels_size);
		return chunk;
	}
};

struct SobelOutputChunk {
	uint32_t             chunk_id = 0;
	uint32_t             total_chunks = 0;
	uint32_t             image_width = 0;
	uint32_t             image_height = 0;
	uint32_t             core_start_row = 0;
	uint32_t             core_rows = 0;
	std::vector<uint8_t> edge_pixels;

	void serialize(std::vector<uint8_t>& buf) const {
		auto append_u32 = [&](uint32_t value) {
			auto* ptr = reinterpret_cast<const uint8_t*>(&value);
			buf.insert(buf.end(), ptr, ptr + sizeof(value));
		};

		append_u32(chunk_id);
		append_u32(total_chunks);
		append_u32(image_width);
		append_u32(image_height);
		append_u32(core_start_row);
		append_u32(core_rows);
		append_u32(static_cast<uint32_t>(edge_pixels.size()));
		buf.insert(buf.end(), edge_pixels.begin(), edge_pixels.end());
	}

	static SobelOutputChunk deserialize(const uint8_t* data, size_t size) {
		const size_t fixed_u32_count = 7;
		const size_t header_size = sizeof(uint32_t) * fixed_u32_count;
		if (size < header_size) {
			throw std::runtime_error("Invalid SobelOutputChunk payload header.");
		}

		auto read_u32 = [&](size_t offset) {
			uint32_t value = 0;
			std::memcpy(&value, data + offset, sizeof(value));
			return value;
		};

		SobelOutputChunk chunk;
		chunk.chunk_id = read_u32(0 * sizeof(uint32_t));
		chunk.total_chunks = read_u32(1 * sizeof(uint32_t));
		chunk.image_width = read_u32(2 * sizeof(uint32_t));
		chunk.image_height = read_u32(3 * sizeof(uint32_t));
		chunk.core_start_row = read_u32(4 * sizeof(uint32_t));
		chunk.core_rows = read_u32(5 * sizeof(uint32_t));
		const uint32_t pixels_size = read_u32(6 * sizeof(uint32_t));

		if (size < header_size + pixels_size) {
			throw std::runtime_error("Invalid SobelOutputChunk payload size.");
		}

		chunk.edge_pixels.assign(data + header_size, data + header_size + pixels_size);
		return chunk;
	}
};

class SobelCollector {
	std::string          output_path_;
	GrayImage            output_;
	uint32_t             total_chunks_ = 0;
	uint32_t             received_chunks_ = 0;
	std::vector<uint8_t> seen_;

      public:
	explicit SobelCollector(std::string output_path) : output_path_(std::move(output_path)) {
	}

	void add(const SobelOutputChunk& chunk) {
		if (total_chunks_ == 0) {
			total_chunks_ = chunk.total_chunks;
			output_.width = chunk.image_width;
			output_.height = chunk.image_height;
			output_.pixels.assign(static_cast<size_t>(output_.width) * output_.height, 0);
			seen_.assign(total_chunks_, 0);
		}

		if (chunk.total_chunks != total_chunks_ || chunk.image_width != output_.width || chunk.image_height != output_.height) {
			throw std::runtime_error("Inconsistent chunk metadata in collector.");
		}
		if (chunk.chunk_id >= total_chunks_) {
			throw std::runtime_error("Chunk id out of range.");
		}
		if (seen_[chunk.chunk_id]) {
			return;
		}

		const size_t expected_size = static_cast<size_t>(chunk.core_rows) * chunk.image_width;
		if (chunk.edge_pixels.size() != expected_size) {
			throw std::runtime_error("Chunk pixel count mismatch.");
		}

		for (uint32_t local_row = 0; local_row < chunk.core_rows; ++local_row) {
			const uint32_t global_row = chunk.core_start_row + local_row;
			if (global_row >= output_.height) {
				throw std::runtime_error("Chunk writes outside image bounds.");
			}

			auto* dst = output_.pixels.data() + static_cast<size_t>(global_row) * output_.width;
			auto* src = chunk.edge_pixels.data() + static_cast<size_t>(local_row) * output_.width;
			std::memcpy(dst, src, output_.width);
		}

		seen_[chunk.chunk_id] = 1;
		++received_chunks_;
	}

	bool done() const {
		return total_chunks_ != 0 && received_chunks_ == total_chunks_;
	}

	void write_output() const {
		if (!done()) {
			throw std::runtime_error("Cannot write output before all chunks are collected.");
		}

		std::ofstream out(output_path_, std::ios::binary);
		if (!out) {
			throw std::runtime_error("Could not open output image: " + output_path_);
		}
		out << "P5\n" << output_.width << " " << output_.height << "\n255\n";
		out.write(reinterpret_cast<const char*>(output_.pixels.data()), static_cast<std::streamsize>(output_.pixels.size()));
	}
};

static std::string read_token(std::istream& in) {
	std::string token;
	while (in >> token) {
		if (!token.empty() && token[0] == '#') {
			std::string ignored;
			std::getline(in, ignored);
			continue;
		}
		return token;
	}
	throw std::runtime_error("Unexpected end of PGM header.");
}

static GrayImage read_pgm_p5(const std::string& path) {
	std::ifstream in(path, std::ios::binary);
	if (!in) {
		throw std::runtime_error("Could not open input image: " + path);
	}

	const std::string magic = read_token(in);
	if (magic != "P5") {
		throw std::runtime_error("Only binary PGM (P5) input is supported.");
	}

	const int width = std::stoi(read_token(in));
	const int height = std::stoi(read_token(in));
	const int maxval = std::stoi(read_token(in));
	if (width <= 0 || height <= 0 || maxval != 255) {
		throw std::runtime_error("Unsupported PGM dimensions or maxval.");
	}

	in.get();

	GrayImage image;
	image.width = static_cast<uint32_t>(width);
	image.height = static_cast<uint32_t>(height);
	image.pixels.resize(static_cast<size_t>(width) * static_cast<size_t>(height));

	in.read(reinterpret_cast<char*>(image.pixels.data()), static_cast<std::streamsize>(image.pixels.size()));
	if (in.gcount() != static_cast<std::streamsize>(image.pixels.size())) {
		throw std::runtime_error("Input image data is incomplete.");
	}

	return image;
}

static SobelOutputChunk process_chunk_parallel(const SobelInputChunk& in, uint32_t thread_count) {
	if (in.image_width < 3 || in.image_height < 3) {
		throw std::runtime_error("Image must be at least 3x3 for Sobel.");
	}
	if (in.ext_pixels.size() != static_cast<size_t>(in.ext_rows) * in.image_width) {
		throw std::runtime_error("Input chunk has invalid ext pixel size.");
	}

	SobelOutputChunk out;
	out.chunk_id = in.chunk_id;
	out.total_chunks = in.total_chunks;
	out.image_width = in.image_width;
	out.image_height = in.image_height;
	out.core_start_row = in.core_start_row;
	out.core_rows = in.core_rows;
	out.edge_pixels.assign(static_cast<size_t>(out.core_rows) * out.image_width, 0);

	auto at = [&](uint32_t global_x, uint32_t global_y) -> uint8_t {
		const uint32_t local_y = global_y - in.ext_start_row;
		return in.ext_pixels[static_cast<size_t>(local_y) * in.image_width + global_x];
	};

	const uint32_t workers = std::max(1u, std::min(thread_count, in.core_rows));
	const uint32_t rows_per_worker = in.core_rows / workers;
	const uint32_t row_remainder = in.core_rows % workers;

	std::vector<std::jthread> threads;
	threads.reserve(workers);

	uint32_t next_row = 0;
	for (uint32_t worker = 0; worker < workers; ++worker) {
		const uint32_t span = rows_per_worker + (worker < row_remainder ? 1u : 0u);
		const uint32_t begin = next_row;
		const uint32_t end = begin + span;
		next_row = end;

		threads.emplace_back([&, begin, end] {
			for (uint32_t local_row = begin; local_row < end; ++local_row) {
				const uint32_t y = in.core_start_row + local_row;
				for (uint32_t x = 0; x < in.image_width; ++x) {
					uint8_t value = 0;
					if (x > 0 && x + 1 < in.image_width && y > 0 && y + 1 < in.image_height) {
						const int xi = static_cast<int>(x);
						const int yi = static_cast<int>(y);
						const int gx =
						    -at(xi - 1, yi - 1) + at(xi + 1, yi - 1) - 2 * at(xi - 1, yi) + 2 * at(xi + 1, yi) - at(xi - 1, yi + 1) + at(xi + 1, yi + 1);
						const int gy =
						    at(xi - 1, yi - 1) + 2 * at(xi, yi - 1) + at(xi + 1, yi - 1) - at(xi - 1, yi + 1) - 2 * at(xi, yi + 1) - at(xi + 1, yi + 1);
						const int magnitude = static_cast<int>(std::sqrt(static_cast<double>(gx * gx + gy * gy)));
						value = static_cast<uint8_t>(std::clamp(magnitude, 0, 255));
					}
					out.edge_pixels[static_cast<size_t>(local_row) * out.image_width + x] = value;
				}
			}
		});
	}

	return out;
}

static std::vector<SobelInputChunk> make_chunks(const GrayImage& image, uint32_t requested_chunks) {
	if (image.width < 3 || image.height < 3) {
		throw std::runtime_error("Image must be at least 3x3 for Sobel.");
	}

	const uint32_t interior_rows = image.height - 2;
	const uint32_t total_chunks = std::max(1u, std::min(requested_chunks, interior_rows));
	const uint32_t base_rows = interior_rows / total_chunks;
	const uint32_t extra_rows = interior_rows % total_chunks;

	std::vector<SobelInputChunk> chunks;
	chunks.reserve(total_chunks);

	uint32_t next_core_start = 1;
	for (uint32_t chunk_id = 0; chunk_id < total_chunks; ++chunk_id) {
		const uint32_t rows_for_chunk = base_rows + (chunk_id < extra_rows ? 1u : 0u);

		SobelInputChunk chunk;
		chunk.chunk_id = chunk_id;
		chunk.total_chunks = total_chunks;
		chunk.image_width = image.width;
		chunk.image_height = image.height;
		chunk.core_start_row = next_core_start;
		chunk.core_rows = rows_for_chunk;
		chunk.ext_start_row = chunk.core_start_row - 1;
		chunk.ext_rows = rows_for_chunk + 2;
		chunk.ext_pixels.resize(static_cast<size_t>(chunk.ext_rows) * image.width);

		for (uint32_t local_row = 0; local_row < chunk.ext_rows; ++local_row) {
			const uint32_t global_row = chunk.ext_start_row + local_row;
			auto* dst = chunk.ext_pixels.data() + static_cast<size_t>(local_row) * image.width;
			auto* src = image.pixels.data() + static_cast<size_t>(global_row) * image.width;
			std::memcpy(dst, src, image.width);
		}

		next_core_start += rows_for_chunk;
		chunks.push_back(std::move(chunk));
	}

	return chunks;
}

static void mpi_send_chunk(int destination_rank, const SobelInputChunk& chunk) {
	std::vector<uint8_t> payload;
	chunk.serialize(payload);
	MPI_Send(payload.data(), static_cast<int>(payload.size()), MPI_BYTE, destination_rank, TAG_WORK, MPI_COMM_WORLD);
}

static SobelOutputChunk mpi_recv_result(int* source_rank) {
	MPI_Status status{};
	MPI_Probe(MPI_ANY_SOURCE, TAG_RESULT, MPI_COMM_WORLD, &status);
	if (source_rank != nullptr) {
		*source_rank = status.MPI_SOURCE;
	}

	int count = 0;
	MPI_Get_count(&status, MPI_BYTE, &count);
	std::vector<uint8_t> payload(static_cast<size_t>(count));
	MPI_Recv(payload.data(), count, MPI_BYTE, status.MPI_SOURCE, TAG_RESULT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	return SobelOutputChunk::deserialize(payload.data(), payload.size());
}

static void coordinator_run(const std::string& input_path, const std::string& output_path, uint32_t thread_count, uint32_t requested_chunks, int world_size) {
	const GrayImage image = read_pgm_p5(input_path);
	const std::vector<SobelInputChunk> chunks = make_chunks(image, requested_chunks);
	SobelCollector collector(output_path);

	if (world_size == 1) {
		for (const auto& chunk : chunks) {
			collector.add(process_chunk_parallel(chunk, thread_count));
		}
		collector.write_output();
		return;
	}

	const uint32_t total_chunks = static_cast<uint32_t>(chunks.size());
	uint32_t next_to_send = 0;
	uint32_t received = 0;
	std::vector<uint8_t> stopped(static_cast<size_t>(world_size), 0);

	for (int worker = 1; worker < world_size; ++worker) {
		if (next_to_send < total_chunks) {
			mpi_send_chunk(worker, chunks[next_to_send]);
			++next_to_send;
		} else {
			MPI_Send(nullptr, 0, MPI_BYTE, worker, TAG_STOP, MPI_COMM_WORLD);
			stopped[static_cast<size_t>(worker)] = 1;
		}
	}

	while (received < total_chunks) {
		int source = 0;
		SobelOutputChunk result = mpi_recv_result(&source);
		collector.add(result);
		++received;

		if (next_to_send < total_chunks) {
			mpi_send_chunk(source, chunks[next_to_send]);
			++next_to_send;
		} else {
			MPI_Send(nullptr, 0, MPI_BYTE, source, TAG_STOP, MPI_COMM_WORLD);
			stopped[static_cast<size_t>(source)] = 1;
		}
	}

	for (int worker = 1; worker < world_size; ++worker) {
		if (!stopped[static_cast<size_t>(worker)]) {
			MPI_Send(nullptr, 0, MPI_BYTE, worker, TAG_STOP, MPI_COMM_WORLD);
		}
	}

	collector.write_output();
}

static void worker_run(uint32_t thread_count) {
	for (;;) {
		MPI_Status status{};
		MPI_Probe(0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

		if (status.MPI_TAG == TAG_STOP) {
			MPI_Recv(nullptr, 0, MPI_BYTE, 0, TAG_STOP, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			break;
		}

		if (status.MPI_TAG != TAG_WORK) {
			throw std::runtime_error("Worker received unexpected MPI tag.");
		}

		int count = 0;
		MPI_Get_count(&status, MPI_BYTE, &count);
		std::vector<uint8_t> payload(static_cast<size_t>(count));
		MPI_Recv(payload.data(), count, MPI_BYTE, 0, TAG_WORK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		const SobelInputChunk in = SobelInputChunk::deserialize(payload.data(), payload.size());
		const SobelOutputChunk out = process_chunk_parallel(in, thread_count);

		std::vector<uint8_t> output_payload;
		out.serialize(output_payload);
		MPI_Send(output_payload.data(), static_cast<int>(output_payload.size()), MPI_BYTE, 0, TAG_RESULT, MPI_COMM_WORLD);
	}
}

} // namespace

int main(int argc, char** argv) {
	int provided = 0;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);

	try {
		int rank = 0;
		int world_size = 0;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		MPI_Comm_size(MPI_COMM_WORLD, &world_size);

		if (provided < MPI_THREAD_FUNNELED) {
			if (rank == 0) {
				std::cerr << "Error: MPI implementation does not provide MPI_THREAD_FUNNELED.\n";
			}
			MPI_Abort(MPI_COMM_WORLD, 1);
		}

		if (argc < 3 || argc > 5) {
			if (rank == 0) {
				std::cerr << "Usage: " << argv[0] << " <input.pgm> <output.pgm> [threads] [chunks]\n";
				std::cerr << "Note: this example expects binary PGM (P5) format.\n";
			}
			MPI_Finalize();
			return 1;
		}

		const std::string input_path = argv[1];
		const std::string output_path = argv[2];
		const uint32_t default_threads = std::max(1u, std::thread::hardware_concurrency());
		const uint32_t thread_count = (argc >= 4) ? static_cast<uint32_t>(std::stoul(argv[3])) : default_threads;
		const uint32_t chunks = (argc >= 5) ? static_cast<uint32_t>(std::stoul(argv[4])) : thread_count * 4;

		if (rank == 0) {
			coordinator_run(input_path, output_path, thread_count, chunks, world_size);
		} else {
			worker_run(thread_count);
		}
	} catch (const std::exception& ex) {
		int rank = 0;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		if (rank == 0) {
			std::cerr << "Error: " << ex.what() << '\n';
		}
		MPI_Abort(MPI_COMM_WORLD, 1);
	}

	MPI_Finalize();
	return 0;
}
