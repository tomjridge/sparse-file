(** A pretend version of irmin/Tezos *)


type irmin_obj = {
  children : irmin_obj list; 
  id       : int; (* each obj has an id, for debugging *)
  off      : int option; 
  (* each obj may be persisted, in which case there is an offset in
     the file where the obj is stored *)
}

module IO = struct

  type upper = File.suffix_file
  type sparse = Sparse_file.t

  (* NOTE fields are mutable because we swap them out at some point *)
  type t = { mutable sparse:sparse; mutable upper:upper; }

  

end
