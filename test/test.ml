let args = Sys.argv |> Array.to_list |> List.tl

let _ = 
  (* only perform if "perf" is in cl args *)
  if List.mem "perf" args then
    let open Sparse_file.Private.Test() in
    perf_test()

(* always perform this test *)
let _ = 
  let open Region_manager.Test() in
  ()
