TMP_DOC_DIR:=/tmp/minimal_ocaml
scratch:=/tmp/l/github/scratch

default: all

-include Makefile.ocaml

run:
		DONTLOG=src/suffix_file.ml,src/util.ml,read_from_disk,disk_calc_reachable dune exec test/test.exe -- simulation

# for auto-completion of Makefile target
clean::
