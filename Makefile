check:
	cargo check                                                # check syntax
	cargo doc                                                  # check that building docs doesn't error
	cargo test --all --benches                                 # run tests with no features
	cargo test --all --benches --features="use_fnv,use_log"    # run tests with all features
	cargo readme > .README.md
	diff README.md .README.md                                  # check that the README.md was generated using cargo readme
	cargo fmt -- --check                                       # check that cargo fmt was used

clean:
	cargo clean
	rm .README.md

.PHONY: check clean default
