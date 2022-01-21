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

let sparse_dot_map = "sparse.map"
let sparse_dot_data = "sparse.data"
let upper_dot_offset = "upper.offset"
let upper_dot_data = "upper.data"

module IO = struct


  type upper = Suffix_file.t
  type sparse = Sparse_file.t
  type control = Control.t



  (* NOTE fields are mutable because we swap them out at some point *)
  type t = { root:string; mutable ctrl:control; mutable sparse:sparse; mutable upper:upper }

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
    assert(Unix.stat Fn.(root / control) |> fun st -> st.st_kind = Unix.S_REG);
    let ctrl = Control.load Fn.(root / control) in
    let gen_s = ctrl.generation |> string_of_int in
    log "Loaded control file";
    let sparse = open_sparse_dir root ("sparse."^gen_s) in
    log "Loaded sparse file";
    let upper = open_upper_dir root ("upper."^gen_s) in
    log "Loaded upper file";
    log "IO created";
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

  let unsafe_pread t : off:int ref -> len:int -> buf:bytes -> int = 
    fun ~off ~len ~buf ->
    pread t ~off ~len ~buf |> function
    | Ok n -> n
    | Error () -> failwith "unsafe_pread: unable to read requested region from sparse file"
                   
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

  (* NOTE it is extremely inefficient to try to load objects from disk
     without knowing how long they are; we bypass this problem by
     writing the object length with the object *)

  let save io (obj:t) = 
    assert(obj.ancestors |> List.for_all is_saved);
    let { id;typ;ancestors; _ } = obj in
    let obj : On_disk.t = { id; typ; ancestors=List.map get_off ancestors } in
    let bs = On_disk.to_bytes obj in
    (* extend so we can insert length *)
    let bs = 
      Bytes.length bs |> fun n ->
      Bytes.extend bs 8 0 |> fun bs -> 
      Bytes.set_int64_be bs 0 (Int64.of_int n);
      bs
    in                                                  
    IO.append io bs;
    ()

  module Read_from_disk = struct
    type t = {
      off:int;
      len:int; (** including the length of the offset field *)
      obj:On_disk.t
    }
  end

  let read_from_disk = 
    let buf8 = Bytes.create 8 in
    fun ~(pread:File.Pread.t) ~off ->
      let off0 = off in
      let off = ref off in
      (* first read the length of the obj *)
      let n = pread.pread ~off ~len:8 ~buf:buf8 in
      assert(n=8);
      let len = Bytes.get_int64_be buf8 0 |> Int64.to_int in
      (* now read the obj *)
      let buf = Bytes.create len in
      let n = pread.pread ~off ~len ~buf in
      assert(n=len);
      let obj = On_disk.of_bytes buf in
      Read_from_disk.{ off=off0;len=8+len;obj }

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

  let save_object t obj =
    let off = IO.size t.io in
    obj.off <- Some off;
    Irmin_obj.save t.io obj

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

  module Disk_reachable = struct
    include Int_map

    (** For a given offset, we record the length of the bytes
       representing the object at that offset, and the list of offsets
       for the ancestors *)
    type entry = { len:int; ancestors:int list }
    type t = entry Int_map.t ref 
    (** The result of [disk_calc_reachable] is a hashtbl from offset
       to entry *)

    (* FIXME may want to make reach info a map rather than a hashtbl,
       sorted by offset *)
  end
  module Dr = Disk_reachable

  let disk_calc_reachable ~(io:IO.t) ~(off:int) : Disk_reachable.t =
    let pread = File.Pread.{pread=IO.unsafe_pread io} in
    (* map from offset to (len,<list of offs immediately reachable from this off) *)
    let dreach : Disk_reachable.t = ref Int_map.empty in
    let rec go off = 
      Disk_reachable.mem off !dreach |> function
      | true -> () (* already traversed *)
      | false -> 
        let r = Irmin_obj.read_from_disk ~pread ~off in
        dreach := Disk_reachable.add off Disk_reachable.{len=r.len; ancestors=r.obj.ancestors} !dreach;
        List.iter go r.obj.ancestors
    in
    go off;
    dreach
  (* FIXME another way to do this is just to look at all objects
     created from the last gc, in order, and follow their refs to
     other objects; this uses sequential disk access and may be
     faster; if we refer to an object in the sparse file (ie reachable
     from previous GC commit) then we could use a pre-calculated
     reachability graph; for the moment, we just do the extremely dumb
     thing;
  *)

  module Worker = struct    
    let suc_gen_s io = io.IO.ctrl.generation +1 |> Int.to_string

    (* FIXME we need to filer dreach by ancestors of commit_off;
       commit_off isn't used currently; FIXME need a good name for the
       off from which we create the sparse file; split_off? pivot_off? *)
    let create_sparse_file ~io ~commit_off ~dreach =
      let suc_gen = suc_gen_s io in
      Unix.mkdir Fn.(io.root / "sparse."^suc_gen) 0o770; (* FIXME perm? *)
      let sparse = Sparse.create Fn.(io.root / "sparse."^suc_gen / sparse_dot_data) in
      begin 
        dreach |> Disk_reachable.iter (fun off Dr.{len;_} -> 
            match off < io.upper.suffix_offset with
            | true -> (
                (* copy from sparse *)
                Sparse.translate_vreg io.sparse ~virt_off:off ~len |> function
                | None -> failwith (P.s "couldn't locate virtual offset %d in sparse file" off)
                | Some real_off -> 
                  Sparse.append_region sparse ~src:io.sparse.fd ~src_off:real_off ~len ~virt_off:off;
                  ())
            | false -> (
                (* copy from upper *)
                let real_off = off - io.upper.suffix_offset in
                Sparse.append_region sparse ~src:io.upper.fd ~src_off:real_off ~len ~virt_off:off;
                ()))
      end;
      Sparse.save_map sparse ~map_fn:Fn.(io.root / "sparse."^suc_gen / sparse_dot_map);
      Sparse.close sparse;
      log (P.s "Created new sparse file in dir %s" "sparse."^suc_gen);
      ()

    let create_upper_file ~io ~off = 
      let suc_gen = suc_gen_s io in
      let upper_dot_suc_gen = "upper."^suc_gen in
      Unix.mkdir Fn.(io.root / upper_dot_suc_gen) 0o770; (* FIXME perm? *)
      let upper = Upper.create_suffix_file ~suffix_offset:off Fn.(io.root / upper_dot_suc_gen / upper_dot_data) in
      Upper.save_offset ~off Fn.(io.root / upper_dot_suc_gen / upper_dot_offset);
      let len = io.upper.size () - off in
      File.(copy ~src:Pread.{pread=upper.pread} 
              ~src_off:(off - io.upper.suffix_offset) 
              ~len 
              ~dst:Pwrite.{pwrite=(fd_to_file upper.fd).pwrite} 
              ~dst_off:0);
      upper.close()
        
    (* FIXME maybe create_control_file_dot_n, and take an int? *)
    let create_control_file ~io = 
      let suc_gen = suc_gen_s io in
      let ctrl = Control.{ 
          generation=io.ctrl.generation+1;
          sparse_dir="sparse."^suc_gen;
          upper_dir="upper."^suc_gen }
      in
      Control.save ctrl Fn.(io.root / "control."^suc_gen);
      ()

    let run_worker ~dir ~commit_offset = 
      (* load the control, sparse, upper; traverse from commit_offset
         and store in new sparse file; create new upper file;
         terminate *)
      let io = IO.open_ dir in
      (* load preobj at commit_offset, figure out (off,len) info,
         construct new sparse file *)
      let dreach = disk_calc_reachable ~io ~off:commit_offset in
      log (P.s "Worker: calculated disk reachable objects for offset %d" commit_offset);
      (* now create the sparse file for <commit_offset, given the reachable objects *)
      create_sparse_file ~io ~commit_off:commit_offset ~dreach;
      (* and create the new upper file >= commit_offset *)
      create_upper_file ~io ~off:commit_offset;
      (* and create new control file *)
      create_control_file ~io;
      ()

    let fork_worker ~dir ~commit_offset = 
      Stdlib.flush_all ();
      let r = Unix.fork () in
      match r with 
      | 0 -> (run_worker ~dir ~commit_offset; Unix._exit 0)
      | pid -> `Pid pid
  end

  module Step = struct
    let gc t =
      match t.last_commit with 
      | None -> ()
      | Some obj -> 
        (* we want to launch a separate process to traverse the
           objects from the last commit, store in a new sparse file,
           calculate a new upper, and then switch IO.t *)
        let `Pid pid = Worker.fork_worker ~dir:t.io.root ~commit_offset:(Irmin_obj.get_off obj) in
        t.worker_pid <- Some pid

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

  let remove_old_files ~new_ctrl = ()

  let handle_worker_termination (t:Simulation.t) = 
    (* after termination, we expect the new files to be created as per
       current control.generation +1 (say, 1235); we use the existence
       of control.1235 -- to be renamed to control -- as the
       indication that all these files exist *)
    let suc_gen = t.io.ctrl.generation +1 in
    let suc_gen_s = string_of_int suc_gen in
    let new_ctrl_fn = Fn.(t.io.root / control^"."^suc_gen_s) in
    assert(Sys.file_exists new_ctrl_fn);
    let new_ctrl = Control.load new_ctrl_fn in
    let next_upper = IO.open_upper_dir Fn.(t.io.root / new_ctrl.upper_dir) in
    let _ = 
      (* new data is always being written to current upper; we need to
       ensure it is also copied to next upper *)
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
    (* FIXME open_sparse_dir should just take a single string? or label as ~dir ~name ?*)
    let next_sparse = IO.open_sparse_dir t.io.root new_ctrl.sparse_dir in
    (* now we perform the switch: rename control.suc_gen over control;
       mutate t.io.sparse; t.io.upper *)
    (* FIXME worker should ensure everything synced before termination *)
    Unix.rename Fn.(t.io.root / new_ctrl_fn) Fn.(t.io.root / control);
    t.io.ctrl <- new_ctrl;
    let old_sparse, old_upper = t.io.sparse,t.io.upper in
    t.io.sparse <- new_sparse;
    t.io.upper <- new_upper;
    Sparse.close old_sparse;
    old_upper.close ();
    remove_old_files ~new_ctrl;
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

