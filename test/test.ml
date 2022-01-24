let args = Sys.argv |> Array.to_list |> List.tl

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

let _ = 
  if List.mem "int_load_save" args then 
    let open Util.Small_int_int_map.Test() in
    ()
