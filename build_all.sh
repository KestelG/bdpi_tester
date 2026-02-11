#!/bin/bash
echo "Building for Linux targets..."
cargo build --target x86_64-unknown-linux-gnu --release
cargo build --target aarch64-unknown-linux-gnu --release
cargo build --target x86_64-unknown-linux-musl --release

echo "Building for Windows targets..."
cargo build --target x86_64-pc-windows-gnu --release
cargo build --target i686-pc-windows-gnu --release


echo "Build complete!"