let args = Sys.argv |> Array.to_list |> List.tl

(*
let _ = 
  if List.mem "small_int_int_map" args then 
    let open Util.UNUSED_Small_int_int_map.Test() in
    ()
*)

let _ = 
  if List.mem "small_int_file_v1" args then
    let open Util.Small_int_file_v1.Test() in
    ()

let _ = 
  if List.mem "small_int_file_v2" args then
    let open Small_int_file_v2.Test() in
    ()

let _ = 
  (* only perform if "perf" is in cl args *)
  if List.mem "perf" args then
    let open Sparse_file.Private.Test() in
    perf_test()

let _ = 
  if List.mem "region_manager" args then 
    let open Region_manager.Test() in
    ()


let _ = 
  if List.mem "simulation" args then
    let open Irmin_simulation.Test() in
    ()

