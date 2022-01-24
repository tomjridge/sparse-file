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



(** A small (easily held in memory) file containing just ints; loaded
   via mmap; created via mmap (so, you need to know the number of ints
   upfront) *)
module Small_int_file = struct
  let load fn : Unix.file_descr * (int, Bigarray.int_elt, Bigarray.c_layout) Bigarray.Array1.t = 
    let sz = Unix.(stat fn |> fun st -> st.st_size) in      
    (* check that the size is a multiple of 8 bytes, then mmap and
       read from array *)
    ignore(sz mod 8 = 0 || failwith "File size is not a multiple of 16");
    let fd = Unix.(openfile fn [O_RDWR] 0o660(*dummy*)) in
    (* NOTE O_RDWR seems to be required even though we only open in
       readonly mode *)
    let shared = false in (* if another mmap is open on the same
                             region, with shared=true, then we can't
                             open with shared=false? *)
    log (P.s "Small_int_file.load: calling Unix.map_file fn=%s" fn);
    let mmap = Bigarray.(Unix.map_file fd Int c_layout shared [| sz |]) 
               |> Bigarray.array1_of_genarray
    in
    log "Small_int_file.load: finished calling Unix.map_file";
    (* now iterate through, constructing the map *)
    fd,mmap

  let save (ints: int list) fn =
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
end

module Int_map = Map.Make(Int)

(** A small (fits into memory), non-volatile, int->int map *)
module Small_int_int_map = struct
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
    let fd,mmap = Small_int_file.load fn in
    (* now iterate through, constructing the map *)
    let len = Bigarray.Array1.dim mmap in
    (0,Map_.empty) |> iter_k (fun ~k (i,m) -> 
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
    Small_int_file.save ints fn

  module Test() = struct
    let _ = 
      let fn = Filename.temp_file "test" ".tmp" in
      (* let fd = Unix.(openfile fn [O_CREAT;O_TRUNC;O_RDWR] 0o660) in *)
      let kvs = [(0,0);(1,1);(2,2)] in
      let m = Map_.of_seq (List.to_seq kvs) in
      save m fn;
      let m = load fn in
      let bs = Map_.bindings m in
      assert(bs = kvs);
      ()
  end

end



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

