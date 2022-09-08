.PHONY: build dogfood

build:
	cargo build

build-tls:
	cargo build --features tls

dogfood: build
	target/debug/abq test \
		--reporter line --reporter junit-xml \
		-- target/debug/abq_cargo --all-features

dogfood-tls: build-tls
	target/debug/abq test \
		--reporter line --reporter junit-xml \
		-- target/debug/abq_cargo --all-features
