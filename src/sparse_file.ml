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

At the moment, this is enforced by the sparse file: attempting to use
[find_vreg] to translate a virtual region to a real region in the
sparse file will return None. The region manager cannot enforce this
either, because the region manager is concerned only with the creation
of the sparse file, not the subsequent copying from the sparse file.

*)

(* TODO: 

- X maintain a map of virt_off->(real_off,len)
- X ability to read within a region
- strengthen invariants eg no double entries for a given offset
*)

open Util

module Private = struct

  type map = (int * int) Int_map.t

  module Map_ = struct
    include Int_map
    let load fn = 
      assert(Sys.file_exists fn);
      assert( (Unix.stat fn).st_size mod (3*8) = 0 ||
              failwith (P.s 
                          "%s: attempted to load int->int*int map \
                           whose size was not a multiple of 3*8" __FILE__));
      let ints = Small_int_file_v1.load fn in
      assert(List.length ints mod 3 = 0);
      (ints,empty) |> iter_k (fun ~k (ints,m) -> 
          match ints with 
          | [] -> m
          | x::y::z::rest ->
            let m = add x (y,z) m in
            k (rest,m)
          | _ -> failwith "impossible")

    let save t fn = 
      let x = ref [] in
      t |> bindings |> List.iter (fun (voff,(off,len)) -> x:= voff::off::len::!x);
      Small_int_file_v1.save !x fn

    (** For saving and loading in human readable form *)
    module Human_readable = struct
      module T = struct
        open Sexplib.Std
        type ent = { voff:int; off:int; len:int }[@@deriving sexp]
        type t = ent list[@@deriving sexp]
      end
      include T
      include Add_load_save_funs(T)
    end
    module Hr = Human_readable

    let save_hum t fn =
      t |> bindings |> List.map (fun (voff,(off,len)) -> Hr.{voff;off;len}) |> fun x -> 
      Hr.save x fn

    (* step through a list of (voff,off,len); sort (voff,len) by voff,
       and print which regions are not mapped *)
    let debug_vols t = Hr.(
        t |> iter_k (fun ~k xs -> 
            match xs with 
            | [] -> ()
            | [_x] -> ()
            | x::y::rest -> 
              match y.voff = x.voff+x.len with 
              | false -> 
                Util.log (P.s "Missing region from voff %d of length %d " 
                       (x.voff+x.len) (y.voff - (x.voff+x.len)));
                k (y::rest)
              | true -> 
                k (y::rest)))            

    let load_hum fn =
      Hr.load fn |> fun t -> 
      debug_vols t; t |> List.map (fun Hr.{voff;off;len} -> (voff,(off,len))) |> List.to_seq |> Int_map.of_seq
      
  end


  module type SPARSE = sig
    
    (* type virt_off := int *)
    type real_off := int
    (* type len := int *)
    type fd := Unix.file_descr

    (** The [fd] component is available for reading regions found via
       [find_vreg] *)
    type t = private { fd:fd; mutable map:map; readonly:bool }

    val create: string -> t
      
    (** Open the sparse file, with the given map from virtual offset to real offset *)
    val open_ro : map_fn:string -> string -> t

    (** Explicitly save the map; the map is not automatically saved -
       it is up to the user to remember to save any updated map before
       closing the file. *)
    val save_map: t -> map_fn:string -> unit

    (** [close t] closes the underlying fd; this does not ensure the
        map is saved; use [save_map] for that. *)
    val close: t -> unit

    (** [map_add ~virt_off ~real_off ~len] adds a binding from virtual
       region [(virt_off,len)] to real region [(real_off,len)] *)
    val map_add: t -> virt_off:int -> real_off:int -> len:int -> unit

    (** [append_region t ~src ~src_off ~len ~virt_off] appends len bytes
        from fd [src] at offset [src_off], to the end of sparse file t.

        [len] must not be 0; no other validity checks are made on off
        and len.

        A new map entry [virt_off -> (real_off,len)] is added, where
        real_off was the real offset of the end of the sparse file
        before the copy took place.

        The lseek position in the src file is changed as a side
        effect; before the copy starts, the sparse file seek ptr is
        positioned at the end of the file to ensure we append to the
        sparse file.
    *)
    val append_region: t -> src:fd -> src_off:int -> len:int -> virt_off:int -> unit

    (** [translate_vreg ~virt_off ~len] returns [Some real_off] to indicate
       that the virtual region [(virt_off,len)] maps to the real
       region [(real_off,len)].  Returns None if the virtual region is
       only partially contained in the sparse file, or not contained
       at all. NOTE it is expected that None is treated like an error,
       since the user should never be accessing a sparse file region
       which doesn't exist.  *)
    val translate_vreg: t -> virt_off:int -> len:int -> real_off option


    (** Debugging *)
    val to_string: t -> string
  end

  
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

    type t = { 
      fd          : Unix.file_descr; 
      mutable map : map; 
      readonly    : bool 
    }
    
    (* Construction functions ---- *)
             
    (* We create the file as fn.tmp and fn.map.tmp while constructing,
       then promote when finished *)
    let create fn = 
      let fd = Unix.(openfile fn [O_RDWR;O_CREAT;O_TRUNC] 0o660) in
      { fd; map=Map_.empty; readonly=false }

    let open_ro ~map_fn fn = 
      assert(Sys.file_exists fn);
      assert(Sys.file_exists map_fn);
      let fd = Unix.(openfile fn [O_RDONLY] 0) in
      let map = Map_.load map_fn in
      { fd; map; readonly=true }

    let save_map t ~map_fn = Map_.save t.map map_fn
        
    let close t = Unix.close t.fd

    let map_add t ~virt_off ~real_off ~len = t.map <- Map_.add virt_off (real_off,len) t.map

    let append_region t ~src ~src_off ~len ~virt_off =
      assert(len > 0);
      let buf_sz = 4096 in
      let buf = Bytes.create buf_sz in 
      (* seek to end of sparse file *)
      let real_off = Unix.(lseek t.fd 0 SEEK_END) in
      (* perform the copy; first seek in src *)
      ignore (Unix.(lseek src src_off SEEK_SET));
      len |> iter_k (fun ~k len -> 
          match len > 0 with 
          | false -> ()
          | true -> 
            Unix.read src buf 0 (min buf_sz len) |> fun n' -> 
            assert(n' > 0);
            assert(Unix.write t.fd buf 0 n' = n');
            k (len - n'));
      (* add map entry *)
      map_add t ~virt_off ~real_off ~len;
      ()
      
    let translate_vreg t ~virt_off ~len = 
      Map_.find_last_opt (fun off' -> off' <= virt_off) t.map |> function
      | None ->
        log (P.s "%s: No virtual offset found <= %d" __FILE__ virt_off);
        None
      | Some (voff',(roff',len')) -> 
        (* NOTE things from the map are primed; voff'<=virt_off *)
        (* virtual      ,real
           voff'        ,roff'
           virt_off     ,roff' + (virt_off - voff')
           virt_off+len ,roff' + (virt_off - voff') + len
        *)                      
        let delta = virt_off - voff' in
        (* we can read from the real region provided: roff' + delta
           + len <= roff' + len'; ie delta + len <= len' *)
        match delta + len <= len' with
        | false -> 
          log (P.s "%s: sparse region (%d,%d) does not contain enough \
                    bytes to read region (%d,%d)" __FILE__ roff' len' virt_off len);
          None    
        | true -> 
          Some(roff'+delta)      


    let pp_ref = ref (fun _ -> failwith "unpatched")
    let to_string (t:t) : string = !pp_ref t
    
  end

  module Check_sparse_sig = (Sparse : SPARSE)


  (** Debug module, providing conversion of sparse file into string repr *)
  module Debug = struct
    open Sparse

    (* we order regions by roff in the sparse file *)
    open Sexplib.Std
    type region = { roff:int; len:int; voff:int; mutable data:string }[@@deriving sexp]

    type region_s = region list[@@deriving sexp]
              
    (* from a sparse file, get a list of regions *)
    let to_regions t = 
      let file = File.fd_to_file t.fd in
      Map_.bindings t.map |> 
      List.map (fun (v,(r,l)) -> { roff=r; len=l; voff=v; data=""} ) |> 
      List.sort (fun r1 r2 -> Int.compare r1.roff r2.roff) |> 
      List.map (fun r -> 
          (* read some data from the region *)
          let len = min 8 r.len in
          let data = file.pread_string ~off:(ref r.roff) ~len in
          r.data <- data;
          r) 
        
    let to_string t  = 
      t |> to_regions |> sexp_of_region_s |> Sexplib.Sexp.to_string_hum 

    let _ = Sparse.pp_ref := to_string
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
      0 |> iter_k (fun ~k src_off -> 
          match src_off+len < 1_000_000_000 with
          | true -> 
            Sparse.append_region t ~src:large_file ~src_off ~len ~virt_off:src_off;
            incr count;
            k (src_off+delta)
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





(*
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
      t.map |> Map_.iter (fun _k v -> 
          if v > off2 && v < !end_ then end_:=v else ());
      !end_
*)


(*
    (* debug pretty print *)
    module Show = struct


    let to_string = Show.to_string
*)
