(** A control file, which records various bits of information about the IO
    state; updated atomically via rename *)

(** 

NOTE: structure of IO directory:

Typically the "generation" is some number 1234 say. Then we expect these files to exist:

{[
control
sparse.1234/{sparse.data,sparse.map}
upper.1234/{upper.data,upper.offset}
]}
*)

open Sexplib.Std
open Util

module T = struct
  type t = {
    generation : int;
    (** generation is incremented on every GC completion, to signal that
        RO instances should reload *)
    sparse_dir : string;
    (** subdir which contains sparse file data and sparse file map *)
    upper_dir : string;
    (** subdir which contains the suffix file, and offset file *)
  }
  [@@deriving sexp]
end

include T
include Add_load_save_funs (T)
