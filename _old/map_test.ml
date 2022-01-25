(** Testing a monomorphic interface to map *)

module type S = sig
  type key
  type t
  type value
  val empty : t
  val is_empty : t -> bool
  val mem : key -> t -> bool
  val add : key -> value -> t -> t
  val update : key -> (value option -> value option) -> t -> t
  val singleton : key -> value -> t
  val remove : key -> t -> t
(*  val merge :
    (key -> value option -> 'b option -> 'c option) -> t -> 'b t -> 'c t *)
  val union : (key -> value -> value -> value option) -> t -> t -> t
  val compare : (value -> value -> int) -> t -> t -> int
  val equal : (value -> value -> bool) -> t -> t -> bool
  val iter : (key -> value -> unit) -> t -> unit
  val fold : (key -> value -> 'b -> 'b) -> t -> 'b -> 'b
  val for_all : (key -> value -> bool) -> t -> bool
  val exists : (key -> value -> bool) -> t -> bool
  val filter : (key -> value -> bool) -> t -> t
  val filter_map : (key -> value -> value option) -> t -> t (* edited *)
  val partition : (key -> value -> bool) -> t -> t * t
  val cardinal : t -> int
  val bindings : t -> (key * value) list
  val min_binding : t -> key * value
  val min_binding_opt : t -> (key * value) option
  val max_binding : t -> key * value
  val max_binding_opt : t -> (key * value) option
  val choose : t -> key * value
  val choose_opt : t -> (key * value) option
  val split : key -> t -> t * value option * t
  val find : key -> t -> value
  val find_opt : key -> t -> value option
  val find_first : (key -> bool) -> t -> key * value
  val find_first_opt : (key -> bool) -> t -> (key * value) option
  val find_last : (key -> bool) -> t -> key * value
  val find_last_opt : (key -> bool) -> t -> (key * value) option
  val map : (value -> value) -> t -> t (* edited *)
  val mapi : (key -> value -> value) -> t -> t (* edited *)
  val to_seq : t -> (key * value) Seq.t
  val to_rev_seq : t -> (key * value) Seq.t
  val to_seq_from : key -> t -> (key * value) Seq.t
  val add_seq : (key * value) Seq.t -> t -> t
  val of_seq : (key * value) Seq.t -> t
end

module Make(S:sig include Map.OrderedType type value end) 
  : S with type key = S.t and type value=S.value 
= struct
  include S
  include Map.Make(S)
  type nonrec t = value t
end

module type S' = sig
  module S: S
  val the_ref: S.t ref
end

(* imperative interface *)
type ('k,'v) map = {
  add: 'k -> 'v -> unit;
  remove: 'k -> unit;
  find_opt: 'k -> 'v option;
  (* etc *)
  unpack: unit -> 
    (module S' with type S.key='k and type S.value='v)
    (* unpack reveals the underlying S module... and the ref'ed value *)
}

let make_imperative (type k v) ~(compare:k->k->int) =
  let module A = struct
    type t = k
    type value = v
    let compare: k -> k -> int = compare
  end
  in
  let module M = Make(A) in
  let r = ref M.empty in
  { add=(fun k v -> r:= M.add k v !r);
    remove=(fun k -> r:= M.remove k !r);
    find_opt=(fun k -> M.find_opt k !r);
    unpack=(fun () -> 
        let module S'' = struct
          module S = M
          let the_ref = r
        end
        in
        (module S''))
  }

let map_ = make_imperative ~compare:Int.compare

let _ : (int,'weak1) map = map_ 

let _ = map_.add 1 2

let _ = map_.add 2 3

(* can unpack the entire module to access further functionality *)
let size (type k v) (map:(k,v)map) = 
  let module M = (val map.unpack ()) in
  M.S.cardinal !M.the_ref

