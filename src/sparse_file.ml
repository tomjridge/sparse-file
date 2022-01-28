(** An implementation of generic sparse files. *)

(**

{1 Introduction}

A sparse file is a concept from file systems, where a file may have "gaps" which don't
consume space on disk; see e.g. {{: https://en.wikipedia.org/wiki/Sparse_file}wikipedia}.

What does generic mean? Usually sparse files only allow blocks within the file to be
sparse; it is not possible to have a sparse region with a size less than a block for
example. In order to implement GC for Irmin, we want a "generic sparse file", where
objects on disk that are unreachable are replaced with sparse gaps, which are typically
small regions of 100 to 200 bytes. Thus, block-level sparse files are not sufficient, and
we need to allow small regions to be sparse.

{1 Irmin usecase}

The Irmin usecase requires, given a file and a list of live regions (where a region is
denoted by an offset, length pair [(off,len)]), to construct a sparse version of the file
containing only the data for the live regions, but still allowing each live region to be
accessed by the original offset. Once a sparse file is created, it is accessed in RDONLY
mode (so, no updates are made after initial creation of the sparse file).

{1 Example}

Suppose we have the original file, which stores objects:

{v File: [aaa][bbb][ccc][ddd] v}

Suppose object [bbb] is not live. Then the sparse file might look like:

{v Sparse file: [aaa][000][ccc][ddd] v}

Here, the gap is represented by [000].


{1 Implementation}

The implementation of a sparse file involves two underlying files: the sparse-data file
and the sparse-map file. 

{b sparse-data:} This consists of regions of the original file. Continuing the example
above, we might have: 

{v 
sparse-file: [aaa][000][ccc][ddd]

  is implemented by 

sparse-data: [ccc][aaa][ddd] 
sparse-map: ...
v}

Note that the two original adjacent regions c and d are no longer adjacent in the
sparse-data file. This is because regions may be added to the sparse file in arbitrary
order, and no constraints are made that regions must first be coalesced (for example).

{b sparse-map:} This maintains a mapping from "original offset", to "offset in
sparse-data".

For example, our original file was:

{v
[aaa][bbb][ccc][ddd] 
^    ^    ^    ^
offa |    offc |
     offb      offd
v}

Where region a starts at [offa], region b at [offb] etc. In the sparse-data, we might have:

{v
[ccc][aaa][ddd] 
^    ^    ^
oc   oa   od
v}

i.e., region c starts at real offset [oc] in the sparse-data file, etc.

The sparse-map contains the entries:

{v
Original offset | Real offset in sparse-data
--------------------------------------------
offa            | oa
offc            | oc
offd            | od
v}

Actually, the sparse-map also includes region length information. So, for example, [offa]
maps to [(oa,lena)], where [lena] is the length of region a. This is so that the sparse
file can enforce various properties like "don't read across regions" (see below).


{1 Creating the sparse file}

From an initially empty sparse file, there is a function {!val:append_region}. See the
documentation for that function. When finished, the sparse-map must be written to a
separate file using {!val:save_map}. Then the sparse-data file can be closed.


{1 Restrictions on reading from the sparse file}

{b Don't touch gaps:} The basic restriction for our sparse file implementation is that we
should never attempt to read from the gaps. Thus, if we read from the "a" region, we can
either start at the beginning, or somewhere in the middle, and copy up to the end, but we
should never copy bytes from the following "0" region.

{b Don't read across regions:} Consider the region [ [ccc][ddd] ] in the original file,
which is actually made up of two regions in the sparse file. At the moment, we do not
enforce that the two regions are stored adjacent in the sparse file: maybe region c is
added at one point, and much later region d is also added; in this case, region c and
region d may be stored non-adjacent in the sparse-data file. Thus, attempting to read from
the beginning of the c region to the end of the d region might naively include lots of
unexpected data; fixing this might be non-trivial to implement. However, for the Irmin
usecase, we can simply forbid this. We allow reading within a region, but we do not allow
reading across regions, even if the regions may have been adjacent in the original file.

{1 Problems with the Irmin usecase}

One problem with Irmin is that objects can be stored without explicit length
information. Then trying to decode an object involves starting with a read of [n] bytes,
trying to decode, and if decoding fails, reading [2*n] bytes etc. until we have enough
bytes to successfully decode an object. For the sparse file, it may well be that even this
initial attempt to read [n] bytes cannot succeed: [n] bytes is much greater than the
length of the region holding the object.

The restrictions above (don't touch gaps; don't read across regions) are clearly difficult
to enforce if the decoder doesn't know the length of the object it is trying to decode.

One possible solution is just to return 0 bytes when attempting to read beyond a
particular sparse region. Obviously this is not really "semantically correct", but since
the object should be decoded without touching the zero bytes, no harm should arise.

Consider the example:

{v 
sparse-data: [ccc][aaa][ddd] 
v}

If we start trying to read from the beginning of [aaa], we can read [lena] bytes without
problem. If we read [lena+n] bytes, we should actually receive bytes which look like: [
[aaa][000] ], i.e., the last [n] bytes are just [0].

{1 OCaml interface}

*)

(* TODO: - maybe strengthen invariants eg no double entries for a given offset *)

open Util

module type SPARSE = sig

  (* type virt_off := int *)
  type real_off := int
  (* type len := int *)
  type fd := Unix.file_descr
               
  type map

  (** The type of sparse files. NOTE The [fd] component is available for reading regions
      found via [find_vreg] *)
  type t = private { fd:fd; mutable map:map; readonly:bool }

  val create: string -> t
  (** Create an empty sparse file (sparse-map is also empty) *)

  (** Open the sparse file, with the given sparse-map from virtual offset to real offset *)
  val open_ro : map_fn:string -> string -> t

  (** Explicitly save the sparse-map; the map is not automatically saved - it is up to the
      user to remember to save any updated map before closing the file. *)
  val save_map: t -> map_fn:string -> unit

  (** [close t] closes the underlying fd; this does not ensure the map is saved; use
      [save_map] for that. *)
  val close: t -> unit

  (** [map_add ~virt_off ~real_off ~len] adds a binding from virtual region
      [(virt_off,len)] to real region [(real_off,len)] *)
  val map_add: t -> virt_off:int -> real_off:int -> len:int -> unit

  (** [append_region t ~src ~src_off ~len ~virt_off] appends len bytes from fd [src] at
      offset [src_off], to the end of sparse file t.

      A new map entry [virt_off -> (real_off,len)] is added, where real_off was the real
      offset of the end of the sparse file before the copy took place.

      NOTE [len] must not be 0; no other validity checks are made on off and len. It is
      possible to add the same region multiple times for example, or add overlapping
      regions, etc. {b These usages should be avoided.} FIXME perhaps we should check
      explicitly for these kinds of problems?
  *)
  val append_region: t -> src:fd -> src_off:int -> len:int -> virt_off:int -> unit

  (** [translate_vreg ~virt_off ~len] returns [Some real_off] to indicate that the virtual
      region [(virt_off,len)] maps to the real region [(real_off,len)] in sparse-data.
      Returns None if the virtual region is only partially contained in the sparse file,
      or not contained at all. NOTE it is expected that None is treated like an error,
      since the user should never be accessing a sparse file region which doesn't
      exist. *)
  val translate_vreg: t -> virt_off:int -> len:int -> real_off option


  (** Debugging *)
  val to_string: t -> string
end


module Private = struct

  type map = (int * int) Int_map.t

  module Map_ = struct
    include Int_map
    let load fn = 
      let ints = Small_int_file_v1.load fn in
      let ok = List.length ints mod 3 = 0 in
      let _check_ints_size = 
        if not ok then 
          failwith (P.s "%s: file %s did not contain 3*n ints" __FILE__ fn)
      in
      assert(ok);
      (ints,empty) |> iter_k (fun ~k (ints,m) -> 
          match ints with 
          | [] -> m
          | voff::off::len::rest ->
            (* each set of 3 ints corresponds to voff (virtual offset), off and len *)
            let m = add voff (off,len) m in
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

    (* step through a list of (voff,off,len); sort (voff,len) by voff, and print which
       regions are not mapped *)
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

    let save_hum t fn =
      t |> bindings |> List.map (fun (voff,(off,len)) -> Hr.{voff;off;len}) |> fun x -> 
      Hr.save x fn

    let load_hum fn =
      Hr.load fn |> fun t -> 
      debug_vols t; t |> List.map (fun Hr.{voff;off;len} -> (voff,(off,len))) |> List.to_seq |> Int_map.of_seq      
  end
  
  (* NOTE if we write data at off1, and then rewrite some different data at off1, both
     lots of data will be recorded in the sparse file, but only the second lot of data
     will be accessible at off1; we could add runtime checking to detect this *)

  (* NOTE also that if we write n bytes at off1, and then different data at off1+m, where
     m<n, we record both lots of data in the sparse file, and each lot of data is
     accessible, even though this sparse file does not correspond to any proper original
     file; again we could add runtime checking to detect this *)

  module Sparse = struct

    type nonrec map = map

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

include (Private.Sparse : SPARSE)


