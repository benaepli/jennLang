# Turnpike

The repository is organized as follows:

```
bin/            # main.ml and specifications (for example, CRAQ.jenn)
lib/            # compiler front-end (lexer, parser, ast) here + simulator
README.md       
jennLang.opam   # ignore
output.csv      # main.ml generates trace to this file, to be passed to python SAT solver
main.py         # python SAT solver, checks if given trace is linearizable
```

## Installing dependencies

The following instructions were tested on Ubuntu 20.04.

Install [OCaml](https://ocaml.org/install) and [Dune](https://dune.build/).
```
bash -c "sh <(curl -fsSL https://raw.githubusercontent.com/ocaml/opam/master/shell/install.sh)" # install OCaml
opam init 
opam install ocaml-lsp-server odoc ocamlformat utop # install Dune
```

If necessary, install [Python](https://www.python.org/).
```
apt install python3
```

Clone this repository.
```
git clone https://github.com/jl3953/jennLang
```

## Generating and checking an execution trace

First, generate an execution trace. Second, check if that trace is linearizable.

Generate an execution trace to `output.csv`. 
```
eval $(opam env --switch=default) # run once at the start of each session
dune exec _build/default/bin/main.exe
```
Notes:
- You may modify the input specification file by modifying [this line](https://github.com/jl3953/jennLang/blob/main/bin/main.ml#L352).
- You may modify the output trace file by modifying [this line](https://github.com/jl3953/jennLang/blob/main/bin/main.ml#L327).


Check if that execution trace is linearizable using the python SAT solver.
```
python3 main.py
```
Notes:
- You may modify the input trace file by modifying [this line](https://github.com/jl3953/jennLang/blob/main/main.py#L312).
- This checker skips over any initialization and failover instructions.


## Adjusting trace generation parameters
Coming soon, currently on another branch.
