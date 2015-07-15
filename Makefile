REBAR := ./rebar

.PHONY: all deps doc test clean release

all: compile

compile: deps
	$(REBAR) compile

deps:
	$(REBAR) get-deps

doc:
	$(REBAR) doc skip_deps=true

test:
	$(REBAR) -v 1 eunit skip_deps=true

clean:
	$(REBAR) clean

release: all test
	dialyzer --src src/*.erl deps/*/src/*.erl

generate: compile
	cd rel && ../$(REBAR) generate

run: generate
	./rel/edis/bin/edis start

console: generate
	./rel/edis/bin/edis console

erl: compile
	erl -pa ebin/ -pa lib/*/ebin/ -s edis
