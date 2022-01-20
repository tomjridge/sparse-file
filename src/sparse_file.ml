(** An implementation of generic sparse files. *)

(**

NOTE there is a "region manager" whose job is to coalesce regions, and
check subregions are not added twice. This should be used to calculate
the set of regions which are then copied to the sparse file.

{2 Use of the sparse file} 

When we come to use the sparse file, in our setting, we should never
touch any of the "gap" regions. For example, for the sparse file:

{[ [aaa][000][bbb]... ]}

If we copy from the "a" region, we can either start at the beginning,
or somewhere in the middle, and copy up to the end, but we should
never copy bytes from the following "0" region.

At the moment, this is not enforced by the sparse file. The region
manager cannot enforce this either, because the region manager is
concerned only with the creation of the sparse file, not the
subsequent copying from the sparse file.

*)

open Util

module Private = struct

  
  (* NOTE if we write data at off1, and then rewrite some different
     data at off1, both lots of data will be recorded in the sparse
     file, but only the second lot of data will be accessible at off1;
     we could add runtime checking to detect this *)

  (* NOTE also that if we write n bytes at off1, and then different
     data at off1+m, where m<n, we record both lots of data in the
     sparse file, and each lot of data is accessible, even though this
     sparse file does not correspond to any proper original file;
     again we could add runtime checking to detect this *)

  module Sparse = struct
    open struct module Map_ = Small_int_int_map end

    type t = { fn:string; fd: Unix.file_descr; mutable map:Map_.t; readonly:bool }

    (* Construction functions ---- *)
             
    (* We create the file as fn.tmp and fn.map.tmp while constructing,
       then promote when finished *)
    let create fn = 
      let fd = Unix.(openfile (fn^".tmp") [O_RDWR;O_CREAT;O_TRUNC] 0o660) in
      { fn; fd; map=Map_.empty; readonly=false }
      
    (* NOTE we never rely on the seek ptr being in the right place, so
       it is fine to side-effect alter the seek ptr *)
    let current_offset t : int = Unix.(lseek t.fd 0 SEEK_CUR)    

    let length t : int = Unix.(lseek t.fd 0 SEEK_END)

    (* [map_add t off1 off2] records the binding (off1 -> off2) in the
       offset map; off1 is the location in the original file; off2 is
       the location in the sparse file *)
    let map_add t off1 off2 = t.map <- Map_.add t.map off1 off2

    (** [copy_from ~src ~off ~len] copies len bytes from file
       descr. src at offset off, to the sparse file (len must not be
       0; no other validity checks are made on off and len); a new map
       entry is added; the lseek position in the src file is changed
       as a side effect; before the copy starts, the sparse file seek
       ptr is positioned at the end of the file to ensure we append to
       the sparse file *)
    let copy_from t ~src ~off ~len =
      assert(len > 0);
      let buf_sz = 4096 in
      let buf = Bytes.create buf_sz in 
      (* position to write at end of sparse file *)
      ignore (Unix.(lseek t.fd 0 SEEK_END));
      let coff = current_offset t in
      (* perform the copy *)
      ignore (Unix.(lseek src off SEEK_SET));
      len |> iter_k (fun ~k len -> 
          match len > 0 with 
          | false -> ()
          | true -> 
            Unix.read src buf 0 (min buf_sz len) |> fun n' -> 
            assert(n' > 0);
            assert(Unix.write t.fd buf 0 n' = n');
            k (len - n'));
      (* add map entry *)
      map_add t off coff;
      ()

    let get_fd t = t.fd

    (** [close t] first writes the map into file "fn.map.tmp". Then
       "fn.tmp" is closed. Then fn.tmp is renamed to fn, and
       fn.map.tmp is renamed to fn.map. *)
    let close t = 
      match t.readonly with 
      | true -> Unix.close t.fd
      | false -> 
        Map_.save t.map (t.fn^".map.tmp");
        Unix.close t.fd;
        Unix.rename (t.fn^".tmp") t.fn;
        Unix.rename (t.fn^".map.tmp") (t.fn^".map");
        ()

    (* Readonly functions ---- *)
        
    let open_ fn = 
      assert(Sys.file_exists fn);
      (* if we crashed before renaming fn.map.tmp to fn.map, we should
         try to recover here *)
      (match Sys.file_exists (fn^".map.tmp") && not (Sys.file_exists (fn^".map")) with
       | false -> ()
       | true -> Unix.rename (fn^".map.tmp") (fn^".map"));
      assert(Sys.file_exists (fn^".map"));
      let fd = Unix.(openfile fn [O_RDONLY] 0) in
      { fn; fd; map=Map_.load (fn^".map"); readonly=true }

    let map_find_opt t off1 = Map_.find_opt t.map off1

    (* find the end of the region at off1, by looking in the offset
       map to find the next highest off2; expensive; use only for
       debug *)
    let debug_region_end t off1 = 
      (* take all the off2 values, and
         find the smallest one larger than map(off1) *)
      let off2 = 
        map_find_opt t off1 |> function
        | Some x -> x
        | None -> failwith (Printf.sprintf "Unable to find offset %d in map" off1)
      in
      let end_ = ref Unix.(lseek t.fd 0 SEEK_END) in
      Map_.iter t.map (fun _k v -> 
          if v > off2 && v < !end_ then end_:=v else ());
      !end_

    (* debug pretty print *)
    module Show = struct

      (* we order by start, ie by offset in the sparse file *)
      open Sexplib.Std
      type region = { start: int; end_:int; off1:int; mutable data:string }[@@deriving sexp]

      type region_s = region list[@@deriving sexp]

      module Map_i = Map.Make(Int)
          
      (* from a sparse file, get a list of regions *)
      let to_regions t = 
        (* get sorted off2s in sparse file *)
        Map_.bindings t.map |> fun kvs -> 
        List.map snd kvs |> fun vs -> 
        List.sort Int.compare vs |> fun vs -> 
        (* now form a map from region start to region end *)
        (vs,Map_i.empty) |> iter_k (fun ~k (vs,map) -> 
            match vs with 
            | [] -> (assert(kvs=[]);Map_i.empty)
            | [off2] -> Map_i.add off2 (length t) map
            | off2::off2'::rest -> k (off2'::rest,Map_i.add off2 off2' map))
        |> fun end_map -> 
        (* now iterate over all the bindings in t.map, constructing
           the regions; leave data empty for now *)
        Map_.bindings t.map |> fun kvs -> 
        kvs |> List.map (fun (off1,off2) -> { start=off2; end_=Map_i.find off2 end_map; off1; data="" })
        (* sort in r.start order *)
        |> List.sort (fun r1 r2 -> Int.compare r1.start r2.start)  |> fun regs -> 
        regs
        
      let to_string (regs:region_s) = 
        Sexplib.Sexp.to_string_hum (sexp_of_region_s regs)      
    end

    let to_string = Show.to_string
  end
  

  module Test() = struct
    
    (* performance test *)
    let perf_test () =
      let elapsed () = Mtime_clock.elapsed () |> Mtime.Span.to_s in
      (* copy 100 bytes every 100 bytes from a huge file *)
      let fn = Filename.temp_file "" ".tmp" in
      Printf.printf "(time %f) Creating huge 1GB file %s\n%!" (elapsed()) fn;
      let large_file = 
        (* create *)
        assert(Sys.command (Filename.quote_command "touch" [fn]) = 0);
        (* make huge *)
        assert(Sys.command (Filename.quote_command "truncate" ["--size=1GB";fn]) = 0);
        (* open *)
        let fd = Unix.(openfile fn [O_RDONLY] 0) in
        fd
      in
      (* open sparse file *)
      let fn2 = Filename.temp_file "" ".tmp" in
      Printf.printf "(time %f) Opening sparse file %s\n%!" (elapsed()) fn2;
      let t = Sparse.create fn2 in
      (* now copy n bytes every delta bytes *)
      Printf.printf "(time %f) Copying to sparse file\n%!" (elapsed());
      let len,delta = 100,500 in
      let count = ref 0 in
      0 |> iter_k (fun ~k off -> 
          match off+len < 1_000_000_000 with
          | true -> 
            Sparse.copy_from t ~src:large_file ~off ~len;
            incr count;
            k (off+delta)
          | false -> ());
      Printf.printf "(time %f) Finished; number of regions: %d\n%!" (elapsed()) !count;
      Printf.printf "(time %f) Closing sparse file\n%!" (elapsed());
      Sparse.close t;
      Printf.printf "(time %f) Finished\n%!" (elapsed());
      ()
      
    (* typical run: 

dune exec test/test.exe
(time 0.000106) Creating huge 1GB file /tmp/8f53c9.tmp
(time 0.002654) Opening sparse file /tmp/e6de54.tmp
(time 0.002677) Copying to sparse file
(time 12.xxxxx) Finished; number of regions: 2000000
(time 12.329643) Closing sparse file
(time 12.589131) Finished

ls -al /tmp/
  -rw-rw----  1 tom  tom  191M Jan 14 17:15 e6de54.tmp
  -rw-rw----  1 tom  tom   31M Jan 14 17:15 e6de54.tmp.map

    *)

  end

end

include Private.Sparse
