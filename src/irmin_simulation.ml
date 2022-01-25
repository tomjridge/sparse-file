(** A toy version of irmin/Tezos, in order to implement layered store/GC *)

[@@@warning "-27"](* FIXME remove *)

open Util

(* Shorter aliases/abbrevs *)
module Sparse = Sparse_file
module Upper = Suffix_file
module Control = Io_control

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
    io: Io.t;
    mutable clock: int;
    mutable min_free_id : int;
    mutable worker_pid : int option;
    mutable last_commit: irmin_obj option;
    mutable last_commit_objs: irmin_obj_set;
    (** Reachable objects from last commit *)
    mutable normal_objs_created_since_last_commit: irmin_obj_set;
  }

  let save_object t obj =
    let off = Io.size t.io in
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

  let suc_gen_s io = io.Io.ctrl.generation +1 |> Int.to_string


  module Step = struct
    let gc t =
      match t.worker_pid with 
      | Some pid -> 
        (* FIXME this is perhaps a case where we need to signal something is wrong *)
        log (P.s "main: worker pid %d already executing; skipping gc" pid)
      | None -> 
        match t.last_commit with 
        | None -> ()
        | Some obj -> 
          (* we want to launch a separate process to traverse the objects from the last
             commit, store in a new sparse file, calculate a new upper, and then switch
             Io.t *)
          (* first, make sure upper is flushed, so worker can read the commit at commit_offset *)
          t.io.upper.fsync();
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
        List.filter (fun x -> Random.float 1.0 < 0.5) possible
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

  module Main_process = struct
    let remove_old_files ~new_ctrl = () (* FIXME TODO *)

    let handle_worker_termination (t:t) = 
      log "main: handle_worker_termination";
      (* after termination, we expect the new files to be created as per current
         control.generation +1 (say, 1235); we use the existence of control.1235 -- to be
         renamed to control -- as the indication that all these files exist *)
      let suc_gen = suc_gen_s t.io in
      let new_ctrl_name = control_s^"."^suc_gen in
      let new_ctrl_pth = Fn.(t.io.root / new_ctrl_name) in
      assert(Sys.file_exists new_ctrl_pth);
      log "handle_worker_termination: loading new control";      
      let new_ctrl = Io_control.load new_ctrl_pth in
      log "handle_worker_termination: loading new upper";
      let next_upper = Io.open_upper_dir t.io.root new_ctrl.upper_dir in
      let _ = 
        (* new data is always being written to current upper; we need to ensure it is also
           copied to next upper *)
        (* FIXME we should check the case when this code is actually exercised; perhaps
           insert a "sleepf 1.0" at the end of the worker thread *)
        let len1 = t.io.upper.size() in
        let len2 = next_upper.Upper.size() in
        match len2 < len1 with
        | false -> ()
        | true -> 
          log "handle_worker_termination: extending new upper";
          (* need to append bytes from end of t.io.upper to next_upper *)
          let pread = File.Pread.{pread=t.io.upper.pread} in
          let pwrite = File.Pwrite.{pwrite=next_upper.pwrite} in
          let len = len1 - len2 in
          File.copy ~src:pread ~dst:pwrite ~src_off:(len1-len) ~len ~dst_off:len2;
          ()
      in
      log "handle_worker_termination: loading new sparse";
      (* FIXME open_sparse_dir should just take a single string? or label as ~dir ~name
         ?*)
      let next_sparse = Io.open_sparse_dir t.io.root new_ctrl.sparse_dir in
      (* now we perform the switch: rename control.suc_gen over control; mutate
         t.io.sparse; t.io.upper *)
      (* FIXME worker should ensure everything synced before termination *)
      log "main: renaming new control over old control";
      Unix.rename Fn.(t.io.root / new_ctrl_name) Fn.(t.io.root / control_s);
      (* FIXME probably want a sync on the directory as well *)
      t.worker_pid <- None;
      t.io.ctrl <- new_ctrl;
      let old_sparse, old_upper = t.io.sparse,t.io.upper in
      (* perform the switch *)
      t.io.sparse <- next_sparse;
      t.io.upper <- next_upper;
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
      (* choose whether to create a commit, a normal object, or initiate GC wrt the last
         commit *)
      t.clock <- t.clock +1;
      check_worker_status t;
      match t.clock with
      | _ when t.clock = 250 -> Unix._exit 0 (* extend this for further testing *)
      | _ when t.clock mod 41 = 0 -> 
        (* GC *)
        gc t
      | _ when t.clock mod 19 = 0 -> 
        (* create a commit *)
        create_commit t
      | _ -> 
        (* create a new object *)
        create_object t

    let run_main () = 
      let root = "./tmp/" in
      try Unix.mkdir root 0o770 with Unix.(Unix_error (Unix.EEXIST,_,_)) -> ();
      let ctrl = Control.{ generation=0; sparse_dir="sparse.0"; upper_dir="upper.0" } in
      Control.save ctrl Fn.(root / control_s);
      (* create empty sparse file *)
      let _sparse = 
        Unix.mkdir Fn.(root / ctrl.sparse_dir) 0o770;
        let sp = Sparse.create Fn.(root / ctrl.sparse_dir / sparse_dot_data) in
        Sparse.save_map sp ~map_fn:Fn.(root / ctrl.sparse_dir / sparse_dot_map);
        Sparse.close sp
      in      
      (* create initial upper *)
      let _upper = 
        Unix.mkdir Fn.(root / ctrl.upper_dir) 0o770;
        (* FIXME perhaps change ~suffix_offset to just ~off *)
        let up = Upper.create_suffix_file ~suffix_offset:0 Fn.(root / ctrl.upper_dir / upper_dot_data) in
        Upper.save_offset ~off:0 Fn.(root / ctrl.upper_dir / upper_dot_offset);        
        up.close()
      in    
      (* open the IO we just created *)
      let io = Io.open_ root in
      log "Opened IO";
      let t = { 
        io; 
        clock=1; 
        min_free_id=0; 
        worker_pid=None; 
        last_commit=None; 
        last_commit_objs=Hashtbl.create 10; 
        normal_objs_created_since_last_commit=Hashtbl.create 10 
      }
      in
      log "About to start loop...";
      let rec loop () =
        P.p "Stepping t... clock is %d\n%!" t.clock;
        step t;
        Unix.sleepf 0.1;
        loop ()
      in
      loop ()
                                     
  end (* Main_process *)

end (* Simulation *)

module Test() = struct
  let _ = Simulation.Main_process.run_main ()
end
