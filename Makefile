check:
	cargo check                                                # check syntax
	cargo doc                                                  # check that building docs doesn't error
	cargo test --features="fnv,log,smallvec"
	cargo test --doc --features="fnv,log,smallvec"
	cargo test --benches --features="fnv,log,smallvec"         # run tests with all features
	cargo readme > .README.md
	diff README.md .README.md                                  # check that the README.md was generated using cargo readme
	cargo fmt -- --check                                       # check that cargo fmt was used
	# test various combinations of features:
	cargo test
	cargo test --doc
	cargo test --benches
	cargo test --features="fnv"
	cargo test --features="fnv,smallvec"
	cargo test --features="smallvec"
	@echo "\033[1;32mSUCCESS\033[0m"

clean:
	cargo clean
	rm .README.md

.PHONY: check clean default
