open Util

open struct
  (* Shorter aliases/abbrevs *)
  module Sparse = Sparse_file
  module Upper = Suffix_file
  module Control = Io_control
end

module Disk_reachable = struct
  open Int_map

  (** For a given offset, we record the length of the bytes representing the object at
      that offset, and the list of offsets for the ancestors *)
  type entry = { len:int; ancestors:int list }
  type t = entry Int_map.t
  (** The result of [disk_calc_reachable] is a map from offset to entry *)

  let iter f (x:t) = iter f x

  let log = if List.mem "disk_calc_reachable" Util.dontlog_envvar then fun _ -> () else Util.log 

  let disk_calc_reachable ~(io:Io.t) ~(off:int) : t =
    log "started disk_calc_reachable";
    let pread = File.Pread.{pread=Io.unsafe_pread io} in
    (* map from offset to (len,<list of offs immediately reachable from this off>) *)
    let dreach : t ref = ref Int_map.empty in
    let rec go off = 
      mem off !dreach |> function
      | true -> () (* already traversed *)
      | false -> 
        log "disk_calc_reachable: about to pread";
        let r = Irmin_obj.read_from_disk ~pread ~off in
        log "disk_calc_reachable: finished pread";
        dreach := add off {len=r.len; ancestors=r.obj.ancestors} !dreach;
        List.iter go r.obj.ancestors
    in
    go off;
    !dreach
    (* FIXME another way to do this is just to look at all objects created from the last
       gc, in order, and follow their refs to other objects; this uses sequential disk
       access and may be faster; if we refer to an object in the sparse file (ie reachable
       from previous GC commit) then we could use a pre-calculated reachability graph; for
       the moment, we just do the extremely dumb thing; *)

end
module Dr = Disk_reachable

(**/**)
let suc_gen_s io = io.Io.ctrl.generation +1 |> Int.to_string
(**/**)


(* FIXME we need to filer dreach by ancestors of commit_off; commit_off isn't used
   currently; FIXME need a good name for the off from which we create the sparse file;
   split_off? pivot_off? *)
let create_sparse_file ~io ~(dreach:Dr.t) =
  let suc_gen = suc_gen_s io in
  Unix.mkdir Fn.(io.root / "sparse."^suc_gen) 0o770; (* FIXME perm? *)
  let sparse = Sparse.create Fn.(io.root / "sparse."^suc_gen / sparse_dot_data) in
  begin 
    dreach |> Dr.iter (fun off Dr.{len;_} -> 
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
  log "creating upper file";
  let suc_gen = suc_gen_s io in
  let upper_dot_suc_gen = "upper."^suc_gen in
  Unix.mkdir Fn.(io.root / upper_dot_suc_gen) 0o770; (* FIXME perm? *)
  let upper = Upper.create_suffix_file ~suffix_offset:off Fn.(io.root / upper_dot_suc_gen / upper_dot_data) in
  Upper.save_offset ~off Fn.(io.root / upper_dot_suc_gen / upper_dot_offset);
  let len = Upper.size io.upper - off in
  log (P.s "creating upper file: performing copy len=%d off=%d upper_size=%d" len off (Upper.size io.upper));
  File.(copy 
          ~src:Pread.{pread=Upper.pread io.upper} (* from the OLD upper! *)
          ~src_off:off
          ~len 
          ~dst:Pwrite.{pwrite=(fd_to_file upper.fd).pwrite} 
          ~dst_off:0);
  log "creating upper file: closing";
  Upper.close upper

(* FIXME maybe create_control_file_dot_n, and take an int? or create_next_ctrl_file; or
   just param all the worker steps with a (root,next_ctrl) *)
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
  log (P.s "worker: running with dir=%s commit_offset=%d" dir commit_offset);
  (* load the control, sparse, upper; traverse from commit_offset and store in new sparse
     file; create new upper file; terminate *)
  let io = Io.open_ dir in
  log "worker: IO opened";
  (* load preobj at commit_offset, figure out (off,len) info, construct new sparse file *)
  let dreach = Dr.disk_calc_reachable ~io ~off:commit_offset in
  log (P.s "worker: calculated disk reachable objects for offset %d" commit_offset);
  create_sparse_file ~io ~dreach;
  log "worker: sparse file created";
  (* and create the new upper file >= commit_offset *)
  create_upper_file ~io ~off:commit_offset;
  log "worker: upper file created";
  (* and create new control file *)
  create_control_file ~io;
  log "worker: control file created";
  log "worker: final sleep";
  Unix.sleepf 0.2; 
  (* FIXME remove this after testing - just to check new upper is extended properly by
     main thread *)
  log "worker: terminating";      
  ()

let fork_worker ~dir ~commit_offset = 
  Stdlib.flush_all ();
  let r = Unix.fork () in
  match r with 
  | 0 -> (run_worker ~dir ~commit_offset; Unix._exit 0)
  | pid -> `Pid pid
