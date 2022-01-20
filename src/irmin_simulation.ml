(** A pretend version of irmin/Tezos *)

[@@@warning "-27"](* FIXME remove *)

open Util


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
                   
end



module Irmin_obj = struct

  (** Irmin-like objects; can refer to older objects; typically
      persisted in a file before creating another object (FIXME how is
      this invariant enforced in Irmin, that an older object appears
      earlier in the data file?) *)
  type t = {
    id        : int; (* each obj has an id, for debugging *)
    typ       : [ `Normal | `Commit ];
    ancestors : t list; 
    mutable off : int option; 
    (* each obj may be persisted, in which case there is an offset in
       the file where the obj is stored; if an offset is Some, then all
       ancestors must also have been written to disk, so their offsets
       will also be Some *)
  }

  let is_saved obj = obj.off <> None
  
  module On_disk = struct
    open Sexplib.Std
    module T = struct
      type t = {
        id: int;
        typ       : [ `Normal | `Commit ];
        ancestors : int list; (* list of offsets *)
      }[@@deriving sexp]
    end
    include T
    include Add_load_save_funs(T)
  end

  let get_off obj = obj.off |> Option.get

  let save io (obj:t) = 
    assert(obj.ancestors |> List.for_all is_saved);
    let { id;typ;ancestors; _ } = obj in
    let obj : On_disk.t = { id; typ; ancestors=List.map get_off ancestors } in
    IO.append io (On_disk.to_bytes obj);
    ()
end
open Irmin_obj

type irmin_obj = Irmin_obj.t

(** We simulate Irmin: the main process creates a new object every
   second, with references to log(n) previous objects, where some
   references can be to the same object. Objects are written to
   disk. Some objects are "commits"; they tend to reference many
   objects; future objects can only reference objects reachable from
   the most recent commit. At some point a recent commit is selected
   and GC is performed on objects earlier than the commit: only
   objects reachable from the commit are maintained. The GC is
   triggered periodically, happens in another process, and should not
   interrupt the main process.  *)
module Simulation = struct

  type irmin_obj_set = (int,irmin_obj) Hashtbl.t
  
  type t = {
    io: IO.t;
    mutable clock: int;
    mutable min_free_id : int;
    mutable last_commit: irmin_obj option;
    mutable last_commit_objs: irmin_obj_set;
    (** Reachable objects from last commit *)
  }

  let calc_reachable obj = 
    let set = Hashtbl.create 1000 in
    let rec go obj = 
      Hashtbl.mem set obj.id |> function
      | true -> (* already traversed *)
        ()
      | false -> 
        (* traverse ancestors first *)
        obj.ancestors |> List.iter go;
        Hashtbl.add set obj.id obj;
        ()
    in
    go obj;
    set


  let save_object t obj =
    let off = IO.size t.io in
    obj.off <- Some off;
    

  module Step = struct
     let gc t =
       match t.last_commit with 
       | None -> ()
       | Some obj -> 
         (* we know reachable objects from t.last_commit_objs *)
         ()
        
     let create_commit t = ()

     let create_object t = ()
   end
  open Step

  let step t =    
    (* choose whether to create a commit, a normal object, or initiate
       GC wrt the last commit *)
    t.clock <- t.clock +1;
    match t.clock with
    | _ when t.clock mod 30 = 0 -> 
      (* GC *)
      gc t
    | _ when t.clock mod 20 = 0 -> 
      (* create a commit *)
      create_commit t
    | _ -> 
      (* create a new object *)
      create_object t

  

end

