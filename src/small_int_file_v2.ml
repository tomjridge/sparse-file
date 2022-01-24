(* DEPRECATED

(** An implementation of a "small" (fits in memory) list of ints. NOTE
   this is much slower than Small_int_file_v1; could be made much
   faster by batching writes *)

open Util

let save ints fn =
  let fd = Unix.(openfile fn [O_CREAT;O_RDWR;O_TRUNC] 0o660) in
  let file = File.fd_to_file fd in
  let buf8 = Bytes.create 8 in
  let off = ref 0 in
  ints |> iter_k (fun ~k xs ->
      match xs with 
      | [] -> ()
      | x::xs -> 
        Bytes.set_int64_be buf8 0 (Int64.of_int x);
        file.pwrite ~off buf8;
        k xs);
  file.close()
  
let load fn =
  let fd = Unix.(openfile fn [O_RDONLY] 0o660) in
  let file = File.fd_to_file fd in
  let buf8 = Bytes.create 8 in
  let off = ref 0 in
  let len = file.size () in
  let ints = 
    [] |> iter_k (fun ~k xs ->
        match !off >= len with 
        | true -> List.rev xs
        | false -> 
          let n = file.pread ~off ~len:8 ~buf:buf8 in
          assert(n=8);
          let x = Bytes.get_int64_be buf8 0 |> Int64.to_int in
          k (x::xs))
  in
  file.close();
  ints


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
*)
