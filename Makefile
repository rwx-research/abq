.PHONY: build dogfood

build:
	cargo build

dogfood: build
	target/debug/abq test \
		--auto-workers \
		--reporter line --reporter junit-xml \
		-- target/debug/abq_cargo --all-features
