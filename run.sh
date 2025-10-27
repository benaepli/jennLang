#!/usr/bin/bash

apt update
apt install htop -y
apt install bubblewrap -y

# set up vim
echo 'set nu' >> /root/.vimrc
echo 'set autoindent' >> /root/.vimrc
echo 'set tabstop=4 shiftwidth=4 expandtab' >> /root/.vimrc
echo 'set rtp^="/root/.opam/default/share/ocp-indent/vim"' >> /root/.vimrc

# install OCaml and Dune
echo -ne '\n' | bash -c "sh <(curl -fsSL https://raw.githubusercontent.com/ocaml/opam/master/shell/install.sh)" # install to /usr/bin/bash
eval $(opam env)
yes | opam init
yes | opam install ocaml-lsp-server odoc ocamlformat utop dune ppx_deriving batteries core csv

# install Python
apt install python3 python3-pip
pip install z3-solver

git clone https://github.com/jl3953/jennLang