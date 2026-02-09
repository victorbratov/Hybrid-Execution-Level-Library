CXX      := "mpic++"
CXXFLAGS := "-std=c++17 -Wall -Wextra -O3 -I./include"

OUT_DIR    := "out"
BUILD_DIR  := "build"
SINGLE_HDR := 'out/hell.hpp'
ENTRY_HDR  := "src/main.hpp"

default: test

bundle:
	@echo "=== BUNDLING LIBRARY ==="
	@mkdir -p {{OUT_DIR}}
	python3 -m quom {{ENTRY_HDR}} {{SINGLE_HDR}}

test: bundle
	@echo "=== BUILDING TEST RUNNER ==="
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} -I{{OUT_DIR}} -I. tests/*.cpp -o {{BUILD_DIR}}/test_runner
	@echo "\n=== RUNNING TESTS ==="
	./{{BUILD_DIR}}/test_runner

clean:
	@echo "Cleaning up..."
	@rm -rf {{BUILD_DIR}}
	@rm -rf {{OUT_DIR}}
	@echo "Done."
