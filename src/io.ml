(** Implementation of the IO functions for the pack store, using a sparse file + suffix
   file. *)

open Util

(* Shorter aliases/abbrevs *)
module Sparse = Sparse_file
module Upper = Suffix_file
module Control = Io_control

type upper = Suffix_file.t
type sparse = Sparse_file.t                  
type control = Control.t

(** NOTE fields are mutable because we swap them out at some point
    when we switch from the old state to the new state *)
type t = { 
  root:string; 
  mutable ctrl:control; 
  mutable sparse:sparse; 
  mutable upper:upper 
}

let open_sparse_dir root sparse_dir =   
  Sparse.open_ro 
    ~map_fn:Fn.(root / sparse_dir / sparse_dot_map) 
    Fn.(root / sparse_dir / sparse_dot_data)

let open_upper_dir root upper_dir =
  let suffix_offset = Upper.load_offset Fn.(root / upper_dir / upper_dot_offset) in
  Upper.open_suffix_file ~suffix_offset Fn.(root / upper_dir / upper_dot_data)


(* root is a directory which contains the sparse, upper, freeze control etc *)
let open_ root = 
  assert(Sys.file_exists root);
  assert(Unix.stat root |> fun st -> st.st_kind = Unix.S_DIR);
  (* FIXME Unix.stat can throw exception if no file present; need to
     handle errors from open a bit better *)
  assert(Unix.stat Fn.(root / control_s) |> fun st -> st.st_kind = Unix.S_REG);
  let ctrl = Control.load Fn.(root / control_s) in
  log "Loaded control file";
  let sparse = open_sparse_dir root ctrl.sparse_dir in
  log "Loaded sparse file";
  let upper = open_upper_dir root ctrl.upper_dir in
  log "Loaded upper file";
  log "IO created";
  { root; ctrl; sparse; upper }

(** Various functions we need to support in IO for the store.pack file *)

let size t = t.upper.size()

let append t bs = t.upper.append bs

let pread t : off:int ref -> len:int -> buf:bytes -> (int,unit) result = 
  fun ~off ~len ~buf ->
  match !off >= t.upper.suffix_offset with
  | true -> Ok (t.upper.pread ~off ~len ~buf)
  | false -> 
    (* need to pread in the sparse file *)
    Sparse.translate_vreg t.sparse ~virt_off:!off ~len |> function
    | None -> 
      (* can't find the requested region in the sparse file *)
      Error ()
    | Some roff ->
      (* can read from sparse file at real offset roff *)
      File.fd_to_file t.sparse.fd |> fun file -> 
      assert(file.pread ~off:(ref roff) ~len ~buf = len);
      off:=!off + len;
      Ok len                   

let unsafe_pread t : off:int ref -> len:int -> buf:bytes -> int = 
  fun ~off ~len ~buf ->
  pread t ~off ~len ~buf |> function
  | Ok n -> n
  | Error () -> failwith "unsafe_pread: unable to read requested region from sparse file" 
