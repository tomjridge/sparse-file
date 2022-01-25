open Util

(** Irmin-like objects; can refer to older objects; typically
    persisted in a file before creating another object (FIXME how is
    this invariant enforced in Irmin, that an older object appears
    earlier in the data file?) *)
type t = {
  id        : int; (* each obj has an id, for debugging *)
  typ       : [ `Normal | `Commit ];
  ancestors : t list; 
  mutable off : int option; 
  (* each obj may be persisted, in which case there is an offset in
     the file where the obj is stored; if an offset is Some, then all
     ancestors must also have been written to disk, so their offsets
     will also be Some *)
}

let is_saved obj = obj.off <> None

module On_disk = struct
  open Sexplib.Std
  module T = struct
    type t = {
      id: int;
      typ       : [ `Normal | `Commit ];
      ancestors : int list; (* list of offsets *)
    }[@@deriving sexp]
  end
  include T
  include Add_load_save_funs(T)
end

let get_off obj = obj.off |> Option.get

(* NOTE it is extremely inefficient to try to load objects from disk
   without knowing how long they are; we bypass this problem by
   writing the object length with the object *)

let save io (obj:t) = 
  assert(obj.ancestors |> List.for_all is_saved);
  let { id;typ;ancestors; _ } = obj in
  let obj : On_disk.t = { id; typ; ancestors=List.map get_off ancestors } in
  let bs = On_disk.to_bytes obj in
  (* extend so we can insert length *)
  let bs = 
    Bytes.length bs |> fun n ->
    Bytes.extend bs 8 0 |> fun bs -> 
    Bytes.set_int64_be bs 0 (Int64.of_int n);
    bs
  in                                                  
  Io.append io bs;
  ()

module Read_from_disk = struct
  type t = {
    off:int;
    len:int; (** including the length of the offset field *)
    obj:On_disk.t
  }
end

let read_from_disk = 
  let buf8 = Bytes.create 8 in
  let log = if List.mem "read_from_disk" Util.dontlog_envvar then fun _ -> () else Util.log in
  fun ~(pread:File.Pread.t) ~off ->
    log "read_from_disk";
    let off0 = off in
    let off = ref off in
    (* first read the length of the obj *)
    let n = pread.pread ~off ~len:8 ~buf:buf8 in
    assert(n=8);
    let len = Bytes.get_int64_be buf8 0 |> Int64.to_int in
    (* now read the obj *)
    let buf = Bytes.create len in
    let n = pread.pread ~off ~len ~buf in
    assert(n=len);
    log "read_from_disk: converting buf to obj";
    let obj = On_disk.of_bytes buf in
    log "read_from_disk: returning";
    Read_from_disk.{ off=off0;len=8+len;obj }
