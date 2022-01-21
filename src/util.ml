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


(** A small (easily held in memory) file containing just ints; loaded
   via mmap; created via mmap (so, you need to know the number of ints
   upfront) *)
module Small_int_file = struct
  let load fn : (int, Bigarray.int_elt, Bigarray.c_layout) Bigarray.Array1.t = 
    let sz = Unix.(stat fn |> fun st -> st.st_size) in      
    (* check that the size is a multiple of 8 bytes, then mmap and
       read from array *)
    ignore(sz mod 8 = 0 || failwith "File size is not a multiple of 16");
    let fd = Unix.(openfile fn [O_RDONLY] 0) in
    let shared = false in
    let mmap = Bigarray.(Unix.map_file fd Int c_layout shared [| sz |]) 
               |> Bigarray.array1_of_genarray
    in
    (* now iterate through, constructing the map *)
    Unix.close fd;
    mmap

  let save (ints: int list) fn =
    let fd = Unix.(openfile fn [O_CREAT;O_RDWR;O_TRUNC] 0o660) in
    let shared = true in
    let sz = List.length ints in
    let mmap = Bigarray.(Unix.map_file fd Int c_layout shared [| sz |]) 
               |> Bigarray.array1_of_genarray
    in
    let i = ref 0 in
    (* NOTE that Map.iter operates in key order, lowest first *)
    ints|> List.iter (fun k -> 
        mmap.{ !i } <- k;
        i:=!i + 1;
        ());
    Unix.close fd;      
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
    let mmap = Small_int_file.load fn in
    (* now iterate through, constructing the map *)
    let len = Bigarray.Array1.dim mmap in
    (0,Map_.empty) |> iter_k (fun ~k (i,m) -> 
        match i < len with
        | false -> m
        | true -> 
          let m = Map_.add mmap.{ i } mmap.{ i+1 } m in
          k (i+2,m))
    |> fun m ->
    m

  (** Save the map to file *)
  let save: t -> string -> unit = fun m fn ->
    let ints = 
      let x = ref [] in
      m |> Map_.iter (fun k v -> x:=k::v::!x);
      !x
    in
    Small_int_file.save ints fn
end


let log s = print_endline s


module Add_load_save_funs(S:sig type t[@@deriving sexp] end) = struct
  open S

  let save t fn = Sexplib.Sexp.save_hum fn (t |> sexp_of_t)

  let load fn = Sexplib.Sexp.load_sexp fn |> t_of_sexp

  let to_string t = Sexplib.Sexp.to_string_hum (t |> sexp_of_t)
      
  let to_bytes t = t |> to_string |> Bytes.unsafe_of_string

end

(** Printf abbreviations *)
module P = struct
  include Printf
  let s = sprintf 
end
