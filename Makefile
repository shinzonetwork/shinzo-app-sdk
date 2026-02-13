RUST_DEFRA_DIR ?= $(HOME)/go/src/github.com/sourcenetwork/defradb.rs-shinzo
FFI_LIB_DIR = pkg/rustffi/lib
FFI_HEADER = pkg/rustffi/defra.h

.PHONY: rust-ffi-build rust-ffi-copy build test clean

# Build the Rust FFI static library
rust-ffi-build:
	cd $(RUST_DEFRA_DIR) && cargo build --release -p ffi

# Generate the C header and copy artifacts into the Go package
rust-ffi-copy: rust-ffi-build
	mkdir -p $(FFI_LIB_DIR)
	cd $(RUST_DEFRA_DIR) && cbindgen --config crates/ffi/cbindgen.toml --crate ffi --output $(CURDIR)/$(FFI_HEADER)
	cp $(RUST_DEFRA_DIR)/target/release/libffi.a $(FFI_LIB_DIR)/libffi.a
	@echo "Rust FFI artifacts copied: $(FFI_LIB_DIR)/libffi.a, $(FFI_HEADER)"

# Build Go project with CGO enabled (needed for Rust FFI)
build:
	CGO_ENABLED=1 go build ./...

# Run tests (use -tags rustffi to include Rust FFI tests)
test:
	go test ./pkg/...

# Run Rust FFI tests specifically
test-rustffi:
	CGO_ENABLED=1 go test -v ./pkg/rustffi/...

clean:
	rm -f $(FFI_LIB_DIR)/libffi.a
	rm -f $(FFI_HEADER)
