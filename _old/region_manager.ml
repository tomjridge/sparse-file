(** The region manager tracks which regions are added to a sparse file, in order to: 

* Combine (coalesce) adjacent regions
* Ensure wellformedness (any non-0 part of the sparse file is covered by only 1 region)

In addition, for the irmin-pack usecase, we should also ensure, from
the sparse file, that we never read over the end of a region.

This is a small utility to complement the sparse file implementation.

{2 The problem}

We want to implement a sparse file, which is a "copy" of an original
file, where there are some "gaps". Various issues arise.

Consider the sparse file:

{[ [aaa][000][bbb][ccc]... ]}

This has two adjacent regions (b and c). We want to replace this with
a single region [[bbbccc]] (because this is more efficient when we do
the actual copy to the sparse file; and because we maybe want to
allow, for example, copying a part of that region which overlaps the b
data and the c data).

In short, we want:

INVARIANT (is-coalesced): the sparse file is coalesced, in that there
are no 2 adjacent regions (such as [ [bbb][ccc] ]).

INVARIANT (is-wellformed): the sparse file is wellformed in the sense that, for any
region [(off,len)], there are no other regions which start within the
range [(off,len)] (alternatively: any non-sparse-position in the
sparse file is contained in exactly one region).

NOTE that adding a region may require coalescing a region immediately
before, or after, or both.

{2 Implementation}

We implement the region as a map from (original file) offset to
length; this is ordered by offset. If we add a region, we check that
it does not overlap with any other regions; we then combine with any
adjacent regions (removing them from the map) and finally add the
coalesced region to the map. In order to check adjacent regions, we
can use OCaml's map functions [find_first_opt,find_last_opt], or use
Jane St. maps which provide even more functionality for working with
keys.

*)

open Util

module Map_ = Map.Make(Int)

type t = int Map_.t (** map from offset to len *)

let empty : t = Map_.empty

(** Wellformedness: check the regions are disjoint; requires a single
   pass through the map of regions *)
let wellformed t = 
  let kvs = Map_.to_seq t in
  (-1,-1,kvs) |> iter_k (fun ~k:kont (off,len,kvs) -> 
      match kvs () with 
      | Seq.Nil -> true
      | Cons (kv,rest) -> 
        let (off',len') = kv in
        (* suppose we have a region (0,10), covering bytes 0..9; then
           any later region should start at 11 or beyond; if the next
           region starts at 10 then the regions are not coalesced and
           we have invariant failure *)
        match off' <= off+len with
        | true -> false (* the bad case; not wellformed *)
        | false -> kont (off',len',rest))

(** combine two (optional) regions; one of the regions (but not both)
   may be "None"; returns (Some r) *)
let combine_region r1 r2 = 
  match r1,r2 with 
  | None, None -> assert(false)
  | Some r1, None -> Some r1
  | None, Some r2 -> Some r2
  | Some (off1,len1), Some (off2,len2) -> 
    assert(off1+len1=off2);
    Some (off1,len1+len2)

let ( +++ ) = combine_region

let add t (off,len) = 
  (* find the first region (off',len') where off' >= off *)
  let coalesce_after = 
    Map_.find_first_opt (fun off' -> off' >= off) t |> function 
    | None -> None
    | Some (off',len') -> 
      (* check that this occurs after the region we want to add *)
      assert(off' >= off+len);
      match off' = off+len with
      | true -> 
        (* region is adjacent *)
        Some(off',len')
      | false -> 
        None
  in
  (* find the first region (off',len') where off' <= off *)
  let coalesce_before = 
    Map_.find_last_opt (fun off' -> off' <= off) t |> function
    | None -> None
    | Some (off',len') -> 
      (* check that this occurs before the region we want to add *)
      assert(off'+len' <= off);
      match off'+len' = off with 
      | true -> 
        (* region is adjacent *)
        Some (off',len')
      | false -> 
        None
  in
  (* calculate new region *)
  let reg = coalesce_before +++ (Some(off,len)) +++ coalesce_after |> Option.get (* safe *) in
  (* remove old regions from map *)
  let t = 
    (match coalesce_after with | None -> t | Some (off,_) -> Map_.remove off t) |> fun t -> 
    (match coalesce_before with | None -> t | Some (off,_) -> Map_.remove off t)
  in
  (* add new region *)
  let t = 
    let (off,len) = reg in
    Map_.add off len t
  in
  t

let to_list t = t |> Map_.to_seq |> List.of_seq

module Test() = struct

  let t1 = empty
  let _ = assert(to_list t1 = [])

  let t2 = add empty (10,10) (* (10,10) *)
  let _ = assert(to_list t2 = [(10,10)])

  let t3 = add t2 (20,10) (* (10,20) *)
  let _ = assert(to_list t3 = [(10,20)])

  let t4 = add t2 (0,10) (* (0,20) *)
  let _ = assert(to_list t4 = [(0,20)])

  let t5 = add t2 (30,10) (* (10,10) (30,10) *)
  let _ = assert(to_list t5 = [(10,10);(30,10)])

  let t6 = add t5 (20,10) (* (10,30) *)
  let _ = assert(to_list t6 = [(10,30)])

  (* Following will throw an error because we can't add a subregion of
     an already existing region *)
  let t7 = try add t6 (20,1) with _ -> empty
  let _ = assert(to_list t7 = [])

  (* This passes, because even tho the region length is 0, it is still
     not allowed to start in a region already covered *)
  let t7 = try add t6 (10,0) with _ -> empty
  let _ = assert(to_list t7 = [])

  let t7 = try add t6 (39,0) with _ -> empty
  let _ = assert(to_list t7 = [])

  (* It is allowed to start a 0 length region at the end of an
     existing region FIXME we may want to outlaw 0 length regions *)
  let t7 = add t6 (40,0)
  let _ = assert(to_list t7 = [(10,30)])

end
