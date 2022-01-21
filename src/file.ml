(** General file utilities *)

open Util

module Pwrite = struct
  type t = {
    pwrite: off:int ref -> bytes -> unit;
  }
end

module Pread = struct
  type t = {
    pread : off:int ref -> len:int -> buf:bytes -> int; 
  }
end

include struct
  open Pwrite
  open Pread
  let copy ~(src:Pread.t) ~(dst:Pwrite.t) ~src_off ~len ~dst_off = 
    let {pread},{pwrite} = src,dst in
    let src_off = ref src_off in
    let dst_off = ref dst_off in
    let buf_sz = 8192 in
    let buf = Bytes.create buf_sz in
    len |> iter_k (fun ~k len -> 
        match len <=0 with
        | true -> ()
        | false -> 
          let n = pread ~off:src_off ~len:(min buf_sz len) ~buf in
          pwrite ~off:dst_off (Bytes.sub buf 0 n);
          k (len - n))
end


module Private_util = struct
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
end

(** {2 A regular file} *)

module File = struct
  type t = {
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
    (** pread assumes len <= length(buf) *)

    (** returns the actual number of bytes read *)
    pread_string     : off:int ref -> len:int -> string;    
  }
end

module Private_file = struct
  open Private_util

  module With_fd (S : sig
      val fd : Unix.file_descr
    end) =
  struct
    open S

    let close () = Unix.close fd

    (* FIXME perhaps add "last synced offset" metadata? *)
    let fsync () = Unix.fsync fd

    let size () = (Unix.fstat fd).st_size

    let seek off =
      Unix.(lseek fd off SEEK_SET) |> fun off' ->
      assert (off' = off);
      ()

    let pwrite ~off bs =
      seek !off;
      (* if an error occurs, an exception will be thrown *)
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

    let the_file = File.{
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
  end

  let open' flgs fn = 
    Unix.openfile fn flgs 0o660 |> fun fd ->
    let open With_fd (struct
        let fd = fd
      end) in
    the_file

  let fd_to_file fd = 
    let open With_fd (struct
        let fd = fd
      end) in
    the_file

  let create_file fn = open' [ O_CREAT; O_EXCL; O_RDWR; O_CLOEXEC ] fn

  let open_file fn = open' [ O_RDWR; O_CLOEXEC ] fn
end

let fd_to_file,create_file,open_file = Private_file.(fd_to_file,create_file,open_file)


