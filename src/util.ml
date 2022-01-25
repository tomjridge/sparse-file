(** Essentially the Y combinator; useful for anonymous recursive
    functions. The k argument is the recursive callExample:

    {[
      iter_k (fun ~k n -> 
          if n = 0 then 1 else n * k (n-1))

    ]}


*)
let iter_k f (x:'a) =
  let rec k x = f ~k x in
  k x  

let log s = print_endline s

(** Printf abbreviations *)
module P = struct
  include Printf
  let s = sprintf 
  let p = printf
end

let dontlog_envvar = 
  let s = try Unix.getenv "DONTLOG" with Not_found -> "" in
  let xs = String.split_on_char ',' s in
  log (P.s "%s: DONTLOG envvar: %s" __FILE__ (String.concat " " xs));
  xs

open struct
(** Turn off logging if the filename is in the "DONTLOG" envvar *)
let log = 
  if List.mem __FILE__ dontlog_envvar then fun _s -> () else log
end

(** For testing *)
let random_int_list ~size = 
  let ints = 
    (0,[]) |> iter_k (fun ~k (len,xs) -> 
        if len >=size then xs else
          k (len+1,(Random.int (1024 * 1024 * 1024 -1))::xs))
  in
  ints


(** A small (easily held in memory) file containing just ints; loaded
   via mmap; created via mmap (so, you need to know the number of ints
   upfront).  *)
module Small_int_file_v1 = struct 
  let load fn : int list = 
    (* check that the size is a multiple of 8 bytes, then mmap and
       read from array *)
    let fsz = Unix.(stat fn |> fun st -> st.st_size) in      
    ignore(fsz mod 8 = 0 || failwith "File size is not a multiple of 8");
    let sz = fsz / 8 in (* size of resulting int list *)
    let fd = Unix.(openfile fn [O_RDWR] 0o660(*dummy*)) in
    (* NOTE O_RDWR seems to be required even though we only open in
       readonly mode; FIXME after bug fix, maybe we can now use O_RDONLY *)
    let shared = false in 
    (* if another mmap is open on the same region, with shared=true,
       then we can't open with shared=false? *)
    log (P.s "Small_int_file.load: calling Unix.map_file fn=%s" fn);
    let mmap = Bigarray.(Unix.map_file fd Int c_layout shared [| sz |]) 
               |> Bigarray.array1_of_genarray
    in
    log "Small_int_file.load: finished calling Unix.map_file";
    (* now iterate through, constructing the map *)
    let ints = List.init sz (fun i -> mmap.{i}) in
    Unix.close fd;
    Gc.full_major (); (* reclaim mmap; finalizes mmap *)
    ints

  let save (ints: int list) fn : unit =
    let fd = Unix.(openfile fn [O_CREAT;O_RDWR;O_TRUNC] 0o660) in
    let shared = true in
    let sz = List.length ints in
    log (P.s "Small_int_file.save: calling Unix.map_file fn=%s" fn);
    let mmap = Bigarray.(Unix.map_file fd Int c_layout shared [| sz |]) 
               |> Bigarray.array1_of_genarray
    in
    log "Small_int_file.save: finished calling Unix.map_file";
    let i = ref 0 in
    (* NOTE that Map.iter operates in key order, lowest first *)
    ints|> List.iter (fun k -> 
        mmap.{ !i } <- k;
        i:=!i + 1;
        ());
    Unix.fsync fd;
    Unix.close fd;
    Gc.full_major (); (* reclaim mmap; finalizes mmap *)
    ()

  module Test() = struct
    let _ = 
      let size = 1_000_000 in
      let ints = random_int_list ~size in
      assert(List.length ints = size);
      let fn = Filename.temp_file "test" ".tmp" in
      save ints fn;
      let ints' = load fn in
      Unix.unlink fn;
      assert(ints = ints');
      ()
  end
end

module Int_map = Map.Make(Int)

(*
(** A small (fits into memory), non-volatile, int->int map *)
module UNUSED_Small_int_int_map = struct
  module Map_ = Int_map
  type t = int Map_.t

  let empty : t = Map_.empty
  let add = Map_.add
  let find_opt = Map_.find_opt
  let iter = Map_.iter
  let bindings = Map_.bindings
  let find_last_opt = Map_.find_last_opt

  (** Load the map from a file; assume the file consists of (int->int) bindings *)
  let load: string -> t = fun fn -> 
    let sz = Unix.(stat fn |> fun st -> st.st_size) in      
    (* check that the size is a multiple of 16 bytes, then mmap and
       read from array *)
    ignore(sz mod 16 = 0 || failwith "File size is not a multiple of 16");
    let ints = Small_int_file_v1.load fn in
    (* now iterate through, constructing the map *)
    (ints,Map_.empty) |> iter_k (fun ~k (i,m) -> 
        match i < len with
        | false -> m
        | true -> 
          let m = Map_.add mmap.{ i } mmap.{ i+1 } m in
          k (i+2,m))
    |> fun m ->
    Unix.close fd;
    m

  (** Save the map to file *)
  let save: t -> string -> unit = fun m fn ->
    let ints = 
      let x = ref [] in
      m |> Map_.iter (fun k v -> x:=k::v::!x);
      !x
    in
    Small_int_file_v1.save ints fn

  (* FIXME test currently failing *)
  module Test() = struct
    let _ = 
      let size = 1_000_000 in
      let fn = Filename.temp_file "test" ".tmp" in
      let kvs = random_int_list ~size |> List.rev_map (fun x -> (x,x+1)) in
      let m = Map_.of_seq (List.to_seq kvs) in
      let kvs = Map_.bindings m in
      save m fn;
      let m = load fn in
      let bs = Map_.bindings m in
      assert(bs = kvs);
      ()
  end

end
*)


module Add_load_save_funs(S:sig type t[@@deriving sexp] end) = struct
  open S

  let save t fn = Sexplib.Sexp.save_hum fn (t |> sexp_of_t)

  let load fn = Sexplib.Sexp.load_sexp fn |> t_of_sexp

  let to_string t = Sexplib.Sexp.to_string_hum (t |> sexp_of_t)
  
  (* this loads from the file name s! *)
  let _of_string s = Sexplib.Sexp.load_sexp s |> t_of_sexp

  let of_string s = Sexplib.Sexp.of_string s |> t_of_sexp
      
  let to_bytes t = t |> to_string |> Bytes.unsafe_of_string

  let of_bytes bs = bs |> Bytes.unsafe_to_string |> of_string

end

(** This module provides the infix [/] operator for string concat *)
module Fn = struct
  let ( / ) = Filename.concat
end

(** Common strings for filenames *)
let control_s = "control" (** The control file is always called "control" *)
let sparse_dot_map = "sparse.map"
let sparse_dot_data = "sparse.data"
let upper_dot_offset = "upper.offset"
let upper_dot_data = "upper.data"
