# justfile

# Compiler and flags
CXX      := "mpic++"
CXXFLAGS := "-std=c++17 -Wall -Wextra -I./include"

# Build artifacts
TARGET    := "hell"
BUILD_DIR := "build"

# Default recipe: build the project
default: build

# Build recipe: compiles and links the project
build:
	@echo "=== COMPILING ==="
	@mkdir -p {{BUILD_DIR}}
	@# Use find to get all .cpp files and execute a shell command for each.
	@# This is a very robust way to handle compilation in just.
	@find src -name "*.cpp" -exec sh -c ' \
	    SOURCE="$0"; \
	    OBJ_NAME=$(basename "$SOURCE" .cpp).o; \
	    OBJ_PATH="{{BUILD_DIR}}/$OBJ_NAME"; \
	    echo "  CXX   $SOURCE -> $OBJ_PATH"; \
	    {{CXX}} {{CXXFLAGS}} -c "$SOURCE" -o "$OBJ_PATH" \
	' {} \;

	@echo "\n=== LINKING ==="
	@# Find all object files and link them.
	@find {{BUILD_DIR}} -name '*.o' | xargs {{CXX}} {{CXXFLAGS}} -o {{TARGET}}
	@echo "  LINK  {{TARGET}}"
	@echo "\nBuild complete."

# Clean recipe
clean:
	@echo "Cleaning project..."
	@rm -rf {{BUILD_DIR}}
	@rm -f {{TARGET}}