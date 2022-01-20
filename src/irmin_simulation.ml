(** A pretend version of irmin/Tezos *)

open Util

(** Irmin-like objects; can refer to older objects; typically
   persisted in a file before creating another object (FIXME how is
   this invariant enforced in Irmin, that an older object appears
   earlier in the data file?) *)
type irmin_obj = {
  children : irmin_obj list; 
  id       : int; (* each obj has an id, for debugging *)
  off      : int option; 
  (* each obj may be persisted, in which case there is an offset in
     the file where the obj is stored *)
}


(** A control file, which records various bits of information about
   the IO state; updated atomically via rename from tmp *)
module Control = struct
  open Sexplib.Std
  module T = struct 
    type t = {
      generation      : string; 
      (** incremented on every GC completion, to signal that RO instances should reload *)
      sparse_fn       : string; (* the data in the sparse file *)
      sparse_map_fn   : string; (* the sparse file map from virtual offset to real offset *)
      upper_fn        : string; (* the suffix file for the upper layer *)
      upper_offset_fn : string; (* the offset from which the upper layer starts *)
    }[@@deriving sexp]
  end
  include T
  include Add_load_save_funs(T)
end


(** Structure of IO directory:

Typically "gen" is some number 1234 say. Then we expect these files to exist:

control
sparse.1234
sparse_map.1234
upper.1234
upper_offset.1234

*)

(** Common filenames *)
module Fn = struct
  let control      = "control" (** Control file *)
  let sparse       = "sparse" (** Sparse data file *)
  let sparse_map   = "sparse_map" (** Sparse map file *)
  let upper        = "upper" (** Upper data file *)
  let upper_offset = "upper_offset" (* Upper offset *)
end

module IO = struct

  type upper = File.suffix_file
  type sparse = Sparse_file.t

  module Sparse = Sparse_file

  module Upper = File

  (* NOTE fields are mutable because we swap them out at some point *)
  type t = { mutable control:Control.t; mutable sparse:sparse; mutable upper:upper; }

  let ( / ) = Filename.concat

  (* here, fn is a directory which contains the sparse, upper, freeze control etc *)
  let open_ dir = 
    assert(Sys.file_exists dir);
    assert(Unix.stat dir |> fun st -> st.st_kind = Unix.S_DIR);
    assert(Unix.stat (dir / Fn.control) |> fun st -> st.st_kind = Unix.S_REG);
    let control = Control.load (dir / Fn.control) in
    log "Loaded control file";
    let sparse = Sparse.open_ro ~map_fn:(dir / Fn.sparse_map) (dir / Fn.sparse) in
    let upper = 
      let suffix_offset = Upper.load_offset (dir / Fn.upper_offset) in
      Upper.open_suffix_file ~suffix_offset (dir / Fn.upper) 
    in
    { control; sparse; upper }

  (** Various functions we need to support *)

  let size t = t.upper.size()

  let append t bs = t.upper.append bs

  let pread t : off:int ref -> len:int -> buf:bytes -> int = 
    fun ~off ~len ~buf ->
    match !off >= t.upper.suffix_offset with
    | true -> t.upper.pread ~off ~len ~buf
    | false -> 
      (* need to pread in the sparse file *)
      Sparse.pread ~off ~len ~buf      
                   
                   
end
