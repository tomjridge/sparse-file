(** A suffix file models the suffix of a normal file from a given offset. 

Attempting to read/write before the suffix offset is forbidden and will result in an
exception. 

Attempting to read/write after the suffix offset will read/write in the underlying file
(with appropriate subtraction of the suffix offset).

Example:

{v
Original file: [abcdefghijkl]
                    ^suffix offset

Suffix file: [efghijkl]
              ^suffix offset
v}

*)

open Util

(** A suffix file is just a file descriptor, with a suffix offset *)
type t = {
  fd            : Unix.file_descr;
  suffix_offset : int;
}

open struct
let open' flgs ~suffix_offset fn = 
  Unix.openfile fn flgs 0o660 |> fun fd ->
  { fd; suffix_offset }
end

let create_suffix_file ~suffix_offset fn = 
  open' [ O_CREAT; O_EXCL; O_RDWR; O_CLOEXEC ] ~suffix_offset fn

let open_suffix_file ~suffix_offset fn = 
  open' [ O_RDWR; O_CLOEXEC ] ~suffix_offset fn

let close t = Unix.close t.fd


let size (t:t) = (Unix.fstat t.fd).st_size + t.suffix_offset
(** [size t] returns the logical size of the suffix file, including the suffix offset;
    this is just [size(t.fd)+t.suffix_offset] *)

let seek t off =
  let ok = off >= t.suffix_offset in
  if not ok then 
    failwith (P.s "%s: seek offset %d < suffix_offset %d" __FILE__ off t.suffix_offset);
  assert(ok);
  Unix.(lseek t.fd (off - t.suffix_offset) SEEK_SET) |> fun off' ->
  assert (off' = off - t.suffix_offset);
  ()
(** [seek t off] seeks to virtual address [off] in the suffix file; this is real address
    [off-t.suffix_offset] for [t.fd]. This function will fail if the seek offset is less
    than the suffix offset. *)

let pwrite t ~off bs =
  seek t !off;
  (* if an error occurs, and exception will be thrown *)
  let len = Bytes.length bs in
  let n = Unix.write t.fd bs 0 len in
  assert(n=len);
  off := !off + len;
  ()

let pread t ~off ~len ~buf =
  let len = min len (size t - !off) in
  seek t !off; (* NOTE already takes suffix_offset into account *)
  (len, 0)
  |> iter_k (fun ~k (len, pos) ->
      match len <= 0 with
      | true -> ()
      | false -> Unix.read t.fd buf pos len |> fun n -> k (len - n, pos + n));
  off := !off + len;
  len

let append t buf = pwrite t ~off:(ref (size t)) buf

let fsync t = Unix.fsync t.fd

let _ = pwrite


(** We usually want to store the offset in another file, so we provide these helper
    functions *)
module Private_offset_util = struct
  module T = struct open Sexplib.Std type t = {off:int}[@@deriving sexp] end
  include T
  include Add_load_save_funs(T)
end
open Private_offset_util

let save_offset ~off fn = save {off} fn
let load_offset fn = (load fn).off

