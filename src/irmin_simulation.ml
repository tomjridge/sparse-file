(** A pretend version of irmin/Tezos *)

[@@@warning "-27"](* FIXME remove *)

open Util


(** A control file, which records various bits of information about
   the IO state; updated atomically via rename from tmp *)
module Control = struct
  open Sexplib.Std
  module T = struct 
    type t = {
      generation      : int; 
      (** incremented on every GC completion, to signal that RO instances should reload *)
      sparse_dir      : string; (* subdir which contains sparse file data and sparse file map *)
      upper_dir       : string; (* subdir which contains the suffix file, and offset file *)
    }[@@deriving sexp]
  end
  include T
  include Add_load_save_funs(T)
end


(** Structure of IO directory:

Typically "gen" is some number 1234 say. Then we expect these files to exist:

control
sparse.1234/{sparse.data,sparse.map}
upper.1234/{upper.data,upper.offset}
*)

(** Common filenames *)
module Fn = struct
  let ( / ) = Filename.concat
  let control          = "control" (** Control file *)
end


module Sparse = Sparse_file
module Upper = Suffix_file

module IO = struct


  type upper = Suffix_file.t
  type sparse = Sparse_file.t
  type control = Control.t



  (* NOTE fields are mutable because we swap them out at some point *)
  type t = { root:string; mutable ctrl:control; mutable sparse:sparse; mutable upper:upper }

  let open_sparse_dir root sparse_dir =   
    Sparse.open_ro 
      ~map_fn:Fn.(root / sparse_dir / "sparse.map") 
      Fn.(root / sparse_dir / "sparse.data")
      
  let open_upper_dir root upper_dir =
    let suffix_offset = Upper.load_offset Fn.(root / upper_dir / "upper.offset") in
    Upper.open_suffix_file ~suffix_offset Fn.(root / upper_dir / "upper.data")
    

  (* root is a directory which contains the sparse, upper, freeze control etc *)
  let open_ root = 
    assert(Sys.file_exists root);
    assert(Unix.stat root |> fun st -> st.st_kind = Unix.S_DIR);
    assert(Unix.stat Fn.(root / control) |> fun st -> st.st_kind = Unix.S_REG);
    let ctrl = Control.load Fn.(root / control) in
    let dot_gen = ctrl.generation |> string_of_int in
    log "Loaded control file";
    let sparse = open_sparse_dir root ("sparse"^dot_gen) in
    let upper = open_upper_dir root ("upper"^dot_gen) in
    { root; ctrl; sparse; upper }

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
   second, with references to previous objects, where some references
   can be to the same object. Objects are written to disk. Some
   objects are "commits"; future objects can only reference objects
   reachable from the most recent commit (or that have been created
   since then). At some point a recent commit is selected and GC is
   performed on objects earlier than the commit: only objects
   reachable from the commit are maintained. The GC is triggered
   periodically, happens in another process, and should not interrupt
   the main process.  *)
module Simulation = struct

  type irmin_obj_set = (int,irmin_obj) Hashtbl.t
  
  type t = {
    io: IO.t;
    mutable clock: int;
    mutable min_free_id : int;
    mutable worker_pid : int option;
    mutable last_commit: irmin_obj option;
    mutable last_commit_objs: irmin_obj_set;
    (** Reachable objects from last commit *)
    mutable normal_objs_created_since_last_commit: irmin_obj_set;
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
    Irmin_obj.save t.io obj
    

  module Step = struct
    let gc t =
      match t.last_commit with 
      | None -> ()
      | Some obj -> 
        (* we want to launch a separate process to traverse the
           objects from the last commit, store in a new sparse file,
           calculate a new upper, and then switch IO.t *)
        ()
    let pre_create t =
      let id = t.min_free_id <- t.min_free_id+1; t.min_free_id -1 in       
      let ancestors = 
        let possible = 
          List.of_seq (Hashtbl.to_seq_values t.last_commit_objs) @
          List.of_seq (Hashtbl.to_seq_values t.normal_objs_created_since_last_commit)
        in
        (* filter some of these out *)
        List.filter (fun x -> Random.float 1.0 < 0.9) possible
      in
      let obj = Irmin_obj.{ id; typ=`Normal; ancestors; off=None } in
      obj

    let create_commit t = 
      let obj = pre_create t in
      let obj = { obj with typ=`Commit } in
      (* save it here, right after it is created *)
      save_object t obj;
      t.last_commit <- Some obj;
      t.last_commit_objs <- calc_reachable obj;
      Hashtbl.clear t.normal_objs_created_since_last_commit;
      ()

    let create_object t = 
      let obj = pre_create t in
      let obj = { obj with typ=`Normal } in
      (* save it here, right after it is created *)
      save_object t obj;
      Hashtbl.add t.normal_objs_created_since_last_commit obj.id obj;
      ()
  end
  open Step

  let handle_worker_termination t = 
    (* after termination, we expect the new files to be created as per
       current control.generation +1 (say, 1235); we use the existence
       of control.1235 -- to be renamed to control -- as the
       indication that all these files exist *)
    let suc_gen = t.io.ctrl.generation +1 in
    let suc_gen_s = string_of_int suc_gen in
    assert(Sys.file_exists Fn.(t.io.root / control^"."^suc_gen_s));
    (* new data is always being written to current upper; we need to
       ensure it is also copied to next upper *)
    let next_upper = IO.open_upper_dir t.io.root ("upper."^suc_gen_s) in
    let _ = 
      let len1 = t.io.upper.size() in
      let len2 = next_upper.size() in
      match len2 < len1 with
      | false -> ()
      | true -> 
        (* need to append bytes from end of t.io.upper to next_upper *)
        let pread = File.Pread.{pread=t.io.upper.pread} in
        let pwrite = File.Pwrite.{pwrite=next_upper.pwrite} in
        let len = len1 - len2 in
        File.copy ~src:pread ~dst:pwrite ~src_off:(len1-len) ~len ~dst_off:len2;
        ()
    in
    (* now we perform the switch *)
    (* FIXME TODO *)
    ()

  let check_worker_status t = 
    match t.worker_pid with 
    | None -> ()
    | Some pid -> 
      let pid0,status = Unix.(waitpid [WNOHANG] pid) in
      match pid0 with
      | 0 -> 
        (* worker still processing *)
        ()
      | _ when pid0=pid -> (
          (* worker has terminated *)
          match status with 
          | WEXITED 0 -> handle_worker_termination t
          | WEXITED n -> failwith (P.s "Worker terminated unsuccessfully with %d" n)
          | _ -> failwith "Worker terminated abnormally")
      | _ -> failwith (P.s "Unexpected pid0 value %d" pid0)

  let step t =    
    (* choose whether to create a commit, a normal object, or initiate
       GC wrt the last commit *)
    t.clock <- t.clock +1;
    check_worker_status t;
    match t.clock with
    | _ when t.clock mod 41 = 0 -> 
      (* GC *)
      gc t
    | _ when t.clock mod 19 = 0 -> 
      (* create a commit *)
      create_commit t
    | _ -> 
      (* create a new object *)
      create_object t

  

end

