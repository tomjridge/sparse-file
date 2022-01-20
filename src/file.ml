(** An API to use files, but without explicit file descriptors (so we
   can switch the implementation underneath at runtime, eg to
   sparse-file+suffix-file, and then to another
   sparse-file+suffix-file) 

    NOTE not thread safe
*)

open Util

type file = {
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

module Private_file = struct

  (** Functions we can implement given a pwrite *)
  module With_pwrite(S:sig
      val pwrite : off:int ref -> bytes -> unit
    end) = struct
    open S

    let pwrite_string ~off s = pwrite ~off (Bytes.unsafe_of_string s)

    let pwrite_substring ~off ~str ~str_off ~str_len =
      pwrite_string ~off (String.sub str str_off str_len)
  end

  (** Functions we can implement given pread *)
  module With_pread(S:sig
      val pread : off:int ref -> len:int -> buf:bytes -> int
    end) = struct
    open S

    let pread_string ~off ~len =
      let buf = Bytes.create len in
      let len' = pread ~off ~len ~buf in
      (* len' may be less than len *)
      Bytes.sub buf 0 len' |> Bytes.unsafe_to_string
  end


  module With_fd (S : sig
      val fd : Unix.file_descr
    end) =
  struct
    open S

    let close () = Unix.close fd

    (* FIXME to complete *)
    let fsync () = Unix.fsync fd

    let size () = (Unix.fstat fd).st_size

    let seek off =
      Unix.(lseek fd off SEEK_SET) |> fun off' ->
      assert (off' = off);
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

  let open' flgs fn = 
    Unix.openfile fn flgs 0o660 |> fun fd ->
    let open With_fd (struct
        let fd = fd
      end) in
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

  let create_file fn = open' [ O_CREAT; O_EXCL; O_RDWR ] fn

  let open_file fn = open' [ O_RDWR ] fn
end

let create_file,open_file = Private_file.(create_file,open_file)


(** {2 Suffix file} *)

(** A suffix file models the suffix of a normal file from a given
   offset; attempting to read/write before this offset is forbidden
   and will result in an exception. *)

type suffix_file = {
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
  open Private_file

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

end
