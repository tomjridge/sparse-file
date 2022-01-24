(** Open a sparse file, print some debugging info *)

(* open Util *)

let args = Sys.argv |> Array.to_list |> List.tl

module Fn = struct
  let ( / ) = Filename.concat
end

let _ = 
  match args with 
  | [ dir; map_fn; _data_fn ] -> 
    (* let _sp = Sparse_file.open_ro ~map_fn:Fn.(dir/map_fn) Fn.(dir/data_fn) in *)
    let _ = Util.Small_int_file.load Fn.(dir/map_fn) in
    ()
  | _ -> failwith "Unrecognized command line args"

