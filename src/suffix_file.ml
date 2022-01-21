(** {2 Suffix file} *)

open Util
open File

(** A suffix file models the suffix of a normal file from a given
   offset; attempting to read/write before this offset is forbidden
   and will result in an exception. *)

type t = {
  suffix_offset    : int;
  close            : unit -> unit;
  append           : bytes -> unit;
  fsync            : unit -> unit;
  size             : unit -> int;

  (* for pread/pwrite, the offset ref will be updated; pread and
     pwrite will try to read/write all the bytes *)
  pwrite           : off:int ref -> bytes -> unit;
  pwrite_string    : off:int ref -> string -> unit;
  pwrite_substring : off:int ref -> str:string -> str_off:int -> str_len:int -> unit;
  pread            : off:int ref -> len:int -> buf:bytes -> int; 
  (** returns the actual number of bytes read *)
  pread_string     : off:int ref -> len:int -> string;    
}


module Private_suffix_file = struct
  open Private_util

  let suffix_file_to_file : t -> File.t = function
      {
        suffix_offset=_;
        close;
        append;
        fsync;
        size;
        pwrite;
        pwrite_string;
        pwrite_substring;
        pread;
        pread_string;
      } -> 
      {
        close;
        append;
        fsync;
        size;
        pwrite;
        pwrite_string;
        pwrite_substring;
        pread;
        pread_string;
      }

  module With_fd_offset (S : sig
      val fd : Unix.file_descr
      val suffix_offset: int
    end) =
  struct
    open S

    let close () = Unix.close fd

    let fsync () = Unix.fsync fd

    let size () = (Unix.fstat fd).st_size + suffix_offset

    let seek off =
      assert(off >= suffix_offset);
      Unix.(lseek fd (off - suffix_offset) SEEK_SET) |> fun off' ->
      assert (off' = off - suffix_offset);
      ()

    let pwrite ~off bs =
      seek !off;
      (* if an error occurs, and exception will be thrown *)
      let len = Bytes.length bs in
      ignore (Unix.write fd bs 0 len);
      off := !off + len;
      ()

    let pread ~off ~len ~buf =
      let len = min len (size () - !off) in
      seek !off;
      (len, 0)
      |> iter_k (fun ~k (len, pos) ->
          match len <= 0 with
          | true -> ()
          | false -> Unix.read fd buf pos len |> fun n -> k (len - n, pos + n));
      off := !off + len;
      len

    include With_pwrite(struct let pwrite = pwrite end)
    include With_pread(struct let pread = pread end)

    let append buf = pwrite ~off:(ref (size ())) buf
  end

  let open' flgs ~suffix_offset fn = 
    Unix.openfile fn flgs 0o660 |> fun fd ->
    let open With_fd_offset (struct
        let fd = fd
        let suffix_offset = suffix_offset
      end) in
    {
      suffix_offset;
      close;
      append;
      fsync;
      size;
      pwrite;
      pwrite_string;
      pwrite_substring;
      pread;
      pread_string;
    }

  let create_suffix_file ~suffix_offset fn = 
    open' [ O_CREAT; O_EXCL; O_RDWR ] ~suffix_offset fn

  let open_suffix_file ~suffix_offset fn = 
    open' [ O_RDWR ] ~suffix_offset fn

  (** We usually want to store the offset in another file *)
  module Offset_util = struct
    module T = struct open Sexplib.Std type t = {off:int}[@@deriving sexp] end
    include T
    include Add_load_save_funs(T)
  end

  let save_offset ~off fn = Offset_util.(save {off} fn)
  let load_offset fn = Offset_util.(load fn).off
end

let suffix_file_to_file,create_suffix_file,open_suffix_file = Private_suffix_file.(suffix_file_to_file,create_suffix_file,open_suffix_file)

let save_offset,load_offset = Private_suffix_file.(save_offset,load_offset)


  
