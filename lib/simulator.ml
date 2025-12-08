module DA = BatDynArray

let src = Logs.Src.create "simulator" ~doc:"Simulator logs"

module Log = (val Logs.src_log src : Logs.LOG)

type expr =
  | EVar of string
  | EFind of expr * expr
  | EInt of int
  | EBool of bool
  | ENot of expr
  | EAnd of expr * expr
  | EOr of expr * expr
  | EEqualsEquals of expr * expr
  | EMap of (expr * expr) list
  | EList of expr list
  | EListPrepend of expr * expr
  | EListAppend of expr * expr
  | EListSubsequence of expr * expr * expr
  | EString of string
  | ELessThan of expr * expr
  | ELessThanEquals of expr * expr
  | EGreaterThan of expr * expr
  | EGreaterThanEquals of expr * expr
  | EKeyExists of expr * expr
  | EMapErase of expr * expr
  | EListLen of expr
  | EListAccess of expr * int
  | EPlus of expr * expr
  | EMinus of expr * expr
  | ETimes of expr * expr
  | EDiv of expr * expr
  | EMod of expr * expr
  | EMin of expr * expr
  | ETuple of expr list
  | ETupleAccess of expr * int
  | EUnit
  | ENil
  | EUnwrap of expr
  | ECoalesce of expr * expr
  | ECreatePromise
  | ECreateLock
  | ESome of expr
  | EIntToString of expr
  | EStore of expr * expr * expr
[@@deriving ord]

type lhs = LVar of string | LTuple of string list [@@deriving ord]

type instr =
  | Assign of lhs * expr (* jenndbg probably assigning map values? *)
  | Async of lhs * expr * string * expr list (* jenndbg RPC*)
  | Copy of lhs * expr
  | Resolve of lhs * expr
  | SyncCall of lhs * string * expr list
    (* | Write of string * string (*jenndbg write a value *) *)
[@@deriving ord]

(*
 * We use mutually recursive modules and types to define 'value'
 * and the 'ValueMap' hash table module, which requires 'value'
 * for its key implementation.
 *)
module rec Value : sig
  type t =
    | VInt of int
    | VBool of bool
    | VMap of t ValueMap.t
    | VList of t list
    | VOption of t option
    | VFuture of future_value
    | VLock of bool ref
    | VNode of int
    | VString of string
    | VUnit
    | VTuple of t array

  and future_value = {
    mutable value : t option;
    mutable waiters : (t -> unit) list;
  }
end = struct
  type t =
    | VInt of int
    | VBool of bool
    | VMap of t ValueMap.t
    | VList of t list
    | VOption of t option
    | VFuture of future_value
    | VLock of bool ref
    | VNode of int
    | VString of string
    | VUnit
    | VTuple of t array

  and future_value = {
    mutable value : t option;
    mutable waiters : (t -> unit) list;
  }
end

and ValueKey : sig
  type t = Value.t

  val compare : t -> t -> int
  val equal : t -> t -> bool
  val hash : t -> int
end = struct
  open Value

  type t = Value.t

  let compare_array cmp a1 a2 =
    let len1 = Array.length a1 in
    let len2 = Array.length a2 in
    let c = Int.compare len1 len2 in
    if c <> 0 then c
    else
      let rec loop i =
        if i = len1 then 0
        else
          let c = cmp a1.(i) a2.(i) in
          if c <> 0 then c else loop (i + 1)
      in
      loop 0

  let rec compare v1 v2 =
    match (v1, v2) with
    | VInt i1, VInt i2 -> Int.compare i1 i2
    | VBool b1, VBool b2 -> Bool.compare b1 b2
    | VString s1, VString s2 -> String.compare s1 s2
    | VNode n1, VNode n2 -> Int.compare n1 n2
    | VUnit, VUnit -> 0
    | VOption o1, VOption o2 -> Option.compare compare o1 o2
    | VTuple a1, VTuple a2 -> compare_array compare a1 a2
    | VList l1, VList l2 -> List.compare compare l1 l2
    | VMap m1, VMap m2 -> ValueMap.compare compare m1 m2
    | VFuture f1, VFuture f2 -> if f1 == f2 then 0 else 1
    | VLock l1, VLock l2 -> if l1 == l2 then 0 else 1
    (* Cross-type comparisons *)
    | VInt _, _ -> -1
    | _, VInt _ -> 1
    | VBool _, _ -> -1
    | _, VBool _ -> 1
    | VString _, _ -> -1
    | _, VString _ -> 1
    | VNode _, _ -> -1
    | _, VNode _ -> 1
    | VUnit, _ -> -1
    | _, VUnit -> 1
    | VOption _, _ -> -1
    | _, VOption _ -> 1
    | VTuple _, _ -> -1
    | _, VTuple _ -> 1
    | VList _, _ -> -1
    | _, VList _ -> 1
    | VMap _, _ -> -1
    | _, VMap _ -> 1
    | VFuture _, _ -> -1
    | _, VFuture _ -> 1

  let equal v1 v2 = compare v1 v2 = 0

  let rec hash v =
    match v with
    | VInt i -> Hashtbl.hash i
    | VBool b -> Hashtbl.hash b
    | VString s -> Hashtbl.hash s
    | VNode n -> Hashtbl.hash n
    | VUnit -> 0
    | VOption o -> ( match o with None -> 1 | Some v' -> 17 * hash v')
    | VTuple a -> Array.fold_left (fun acc x -> (23 * acc) + hash x) 5 a
    | VList l -> Hashtbl.hash l
    | VFuture f -> Hashtbl.hash f
    | VLock l -> Hashtbl.hash l
    | VMap m -> Hashtbl.hash m
end

and ValueMap : (Map.S with type key = Value.t) = Map.Make (ValueKey)

(* Type alias for convenience - expose constructors *)
type value = Value.t =
  | VInt of int
  | VBool of bool
  | VMap of value ValueMap.t
  | VList of value list
  | VOption of value option
  | VFuture of Value.future_value
  | VLock of bool ref
  | VNode of int
  | VString of string
  | VUnit
  | VTuple of value array

type future_value = Value.future_value = {
  mutable value : value option;
  mutable waiters : (value -> unit) list;
}

(* Get a human-readable type name for error messages *)
let type_name = function
  | VInt _ -> "int"
  | VBool _ -> "bool"
  | VString _ -> "string"
  | VMap _ -> "map"
  | VList _ -> "list"
  | VOption _ -> "option"
  | VFuture _ -> "future"
  | VLock _ -> "lock"
  | VNode _ -> "node"
  | VUnit -> "unit"
  | VTuple _ -> "tuple"

(* Helper functions to extract specific types with better error messages *)
let expect_int v =
  match v with
  | VInt i -> i
  | _ ->
      failwith
        (Format.asprintf "Type error: expected int, got %s" (type_name v))

let expect_bool v =
  match v with
  | VBool b -> b
  | _ ->
      failwith
        (Format.asprintf "Type error: expected bool, got %s" (type_name v))

let expect_node v =
  match v with
  | VNode n | VInt n -> n
  | _ ->
      failwith
        (Format.asprintf "Type error: expected node/int, got %s" (type_name v))

let expect_map v =
  match v with
  | VMap m -> m
  | _ ->
      failwith
        (Format.asprintf "Type error: expected map, got %s" (type_name v))

let expect_list v =
  match v with
  | VList l -> l
  | _ ->
      failwith
        (Format.asprintf "Type error: expected list, got %s" (type_name v))

let expect_future v =
  match v with
  | VFuture f -> f
  | _ ->
      failwith
        (Format.asprintf "Type error: expected future, got %s" (type_name v))

let expect_lock v =
  match v with
  | VLock l -> l
  | _ ->
      failwith
        (Format.asprintf "Type error: expected lock, got %s" (type_name v))

let expect_tuple v =
  match v with
  | VTuple arr -> arr
  | _ ->
      failwith
        (Format.asprintf "Type error: expected tuple, got %s" (type_name v))

let expect_option v =
  match v with
  | VOption o -> o
  | _ ->
      failwith
        (Format.asprintf "Type error: expected option, got %s" (type_name v))

let expect_string v =
  match v with
  | VString s -> s
  | _ ->
      failwith
        (Format.asprintf "Type error: expected string, got %s" (type_name v))

module Env = Hashtbl.Make (struct
  type t = string

  let hash = Hashtbl.hash
  let equal = ( = )
end)

type 'a label =
  | Instr of instr * 'a (* jenndbg assignments or RPCs *)
  | Pause of 'a (* Insert pause to allow the scheduler to interrupt! *)
  | Await of lhs * expr * 'a
  | SpinAwait of expr * 'a
  | Return of expr (* jenndbg return...I guess? *)
  (*| Read (* jenndbg read a value *)*)
  | Cond of expr * 'a * 'a
  | ForLoopIn of lhs * expr * 'a * 'a
  | Print of expr * 'a
  | Break of 'a
  | Lock of expr * 'a
  | Unlock of expr * 'a

let rec to_string_expr (e : expr) : string =
  match e with
  | EVar s -> s
  | EFind (e1, e2) ->
      "EFind(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EInt i -> string_of_int i
  | EBool b -> string_of_bool b
  | ENot e -> "ENot(" ^ to_string_expr e ^ ")"
  | EAnd (e1, e2) ->
      "EAnd(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EOr (e1, e2) -> "EOr(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EEqualsEquals (e1, e2) ->
      "EEqualsEquals(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EMap kvpairs ->
      "EMap("
      ^ String.concat ", "
          (List.map
             (fun (k, v) -> to_string_expr k ^ ": " ^ to_string_expr v)
             kvpairs)
      ^ ")"
  | EList exprs ->
      "EList(" ^ String.concat ", " (List.map to_string_expr exprs) ^ ")"
  | EListPrepend (e1, e2) ->
      "EListPrepend(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EListAppend (e1, e2) ->
      "EListAppend(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EListSubsequence (ls, start_idx, end_idx) ->
      "EListSubsequence(" ^ to_string_expr ls ^ ", " ^ to_string_expr start_idx
      ^ ", " ^ to_string_expr end_idx ^ ")"
  | EString s -> "\"" ^ s ^ "\""
  | ELessThan (e1, e2) ->
      "ELessThan(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | ELessThanEquals (e1, e2) ->
      "ELessThanEquals(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EGreaterThan (e1, e2) ->
      "EGreaterThan(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EGreaterThanEquals (e1, e2) ->
      "EGreaterThanEquals(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EKeyExists (k, mp) ->
      "EKeyExists(" ^ to_string_expr k ^ ", " ^ to_string_expr mp ^ ")"
  | EMapErase (k, mp) ->
      "EMapErase(" ^ to_string_expr k ^ ", " ^ to_string_expr mp ^ ")"
  | EListLen e -> "EListLen(" ^ to_string_expr e ^ ")"
  | EListAccess (e, i) ->
      "EListAccess(" ^ to_string_expr e ^ ", " ^ string_of_int i ^ ")"
  | EPlus (e1, e2) ->
      "EPlus(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EMinus (e1, e2) ->
      "EMinus(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | ETimes (e1, e2) ->
      "ETimes(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EDiv (e1, e2) ->
      "EDiv(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EMod (e1, e2) ->
      "EMod(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EMin (e1, e2) ->
      "EMin(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | ETuple exprs ->
      "ETuple(" ^ String.concat ", " (List.map to_string_expr exprs) ^ ")"
  | ETupleAccess (e, i) ->
      "ETupleAccess(" ^ to_string_expr e ^ ", " ^ string_of_int i ^ ")"
  | EUnit -> "EUnit"
  | ENil -> "ENil"
  | EUnwrap e -> "EUnwrap(" ^ to_string_expr e ^ ")"
  | ECoalesce (e1, e2) ->
      "ECoalesce(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | ECreatePromise -> "ECreatePromise"
  | ECreateLock -> "ECreateLock"
  | ESome e -> "ESome(" ^ to_string_expr e ^ ")"
  | EIntToString e -> "EIntToString(" ^ to_string_expr e ^ ")"
  | EStore (e1, e2, e3) ->
      "EStore(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ", "
      ^ to_string_expr e3 ^ ")"

let to_string_lhs (l : lhs) : string =
  match l with LVar s -> s | LTuple lst -> "(" ^ String.concat ", " lst ^ ")"

let to_string (l : 'a label) : string =
  match l with
  | Instr (i, _) -> (
      match i with
      | Async (_, n, f, _) ->
          "instr async to " ^ to_string_expr n ^ " with " ^ f
      | Assign (lhs, expr) ->
          "Instr(Assign(" ^ to_string_lhs lhs ^ ", " ^ to_string_expr expr
          ^ "))"
      | Copy (_, _) -> "instr copy"
      | Resolve (lhs, expr) ->
          "Instr(Resolve(" ^ to_string_lhs lhs ^ ", " ^ to_string_expr expr
          ^ "))"
      | SyncCall (lhs, func_name, actual_exprs) ->
          "Instr(SyncCall(" ^ to_string_lhs lhs ^ ", " ^ func_name ^ ", "
          ^ String.concat ", " (List.map to_string_expr actual_exprs)
          ^ "))")
  | Pause _ -> "Pause"
  | Await (lhs, expr, _) ->
      "Await(" ^ to_string_lhs lhs ^ ", " ^ to_string_expr expr ^ ")"
  | SpinAwait (expr, _) -> "SpinAwait(" ^ to_string_expr expr ^ ")"
  | Return _ -> "Return"
  | Cond (expr, _, _) -> "Cond(" ^ to_string_expr expr ^ ", _, _)"
  | ForLoopIn _ -> "ForLoopIn"
  | Print _ -> "Print"
  | Break _ -> "Break"
  | Lock (e, _) -> "Lock(" ^ to_string_expr e ^ ")"
  | Unlock (e, _) -> "Unlock(" ^ to_string_expr e ^ ")"

let rec to_string_value (v : value) : string =
  match v with
  | VInt i -> string_of_int i
  | VBool b -> string_of_bool b
  | VMap m ->
      "VMap("
      ^ String.concat ", "
          (List.map
             (fun (k, v) ->
               "\t" ^ to_string_value k ^ ": " ^ to_string_value v ^ "\n")
             (ValueMap.fold (fun k v acc -> (k, v) :: acc) m []))
      ^ ")"
  | VList l -> "VList(" ^ String.concat ", " (List.map to_string_value l) ^ ")"
  | VOption o ->
      "VOption("
      ^ (match o with Some v -> to_string_value v | None -> "None")
      ^ ")"
  | VFuture fut -> (
      match fut.value with
      | None -> "Future(pending)"
      | Some v -> "Future(" ^ to_string_value v ^ ")")
  | VLock r -> "VLock(" ^ string_of_bool !r ^ ")"
  | VNode n -> "VNode(" ^ string_of_int n ^ ")"
  | VString s -> "" ^ s ^ ""
  | VUnit -> "VUnit"
  | VTuple arr ->
      "VTuple("
      ^ String.concat ", " (List.map to_string_value (Array.to_list arr))
      ^ ")"

module CFG : sig
  type vertex = int (* jenndbg: vertex of the control flow graph *)
  type t = vertex label DA.t (* jenndbg: control flow graph type *)

  val empty : unit -> t (* jenndbg: create an empty control flow graph *)

  val label :
    t -> vertex -> vertex label (* jenndbg: gets vertex label of vertex*)

  val set_label :
    t ->
    vertex ->
    vertex label ->
    unit (* jenndbg: sets vertex label of vertex *)

  val create_vertex :
    t -> vertex label -> vertex (* jenndbg: creates a vertex with label *)

  val fresh_vertex :
    t -> vertex (* jenndbg creates an empty vertex without passing in a label*)
end = struct
  type vertex = int
  type t = vertex label DA.t

  let empty = DA.create
  let label = DA.get (* jenndbg includes an instruction + PC *)
  let set_label = DA.set

  let create_vertex cfg label =
    let id = DA.length cfg in
    DA.add cfg label;
    id

  let fresh_vertex cfg =
    let id = DA.length cfg in
    DA.add cfg (Instr (Assign (LVar "Skip", EVar "skip"), id));
    (*jenndbg so this is how you create a vertex*)
    id
end

(* Activation records *)
(* jenndbg I think nodes are uniquely identified by their IDs *)
(* jenndbg I think a record is a type of vertex*)
type record = {
  mutable pc : CFG.vertex;
  node : int;
  origin_node : int;
  continuation : value -> unit;
      (* Called when activation record returns
                                For RPCs, this writes to the associate future;
                                For client operations it appends to the history 
*)
  env : value Env.t;
  id : int;
  mutable x : float;
  (* threshold for being chosen to be executed, when scheduler is implementing a delay. *)
  f : float -> float (* updates x every time this record is not chosen *);
}

type op_kind = Invocation | Response

type operation = {
  client_id : int;
  op_action : string;
  kind : op_kind;
  payload : value list;
  unique_id : int;
}

type crash_info = {
  mutable currently_crashed : BatSet.Int.t;
  mutable recovery_schedule : (int * int) list; (* (node_id, recover_at_step) *)
  mutable current_step : int;
  mutable queued_messages : (int * record) list;
}

(* Global state *)
type state = {
  nodes : value Env.t array;
  mutable runnable_records : record DA.t;
  mutable waiting_records : record list;
  history : operation DA.t;
  mutable free_clients :
    int list (* client ids should be valid indexes into nodes *);
  crash_info : crash_info;
}

(* Execution environment of an activation record: local variables +
   node-scoped variables. *)
type record_env = { local_env : value Env.t; node_env : value Env.t }

type function_info = {
  entry : CFG.vertex;
  name : string;
  formals : string list;
  locals : (string * expr) list;
  is_sync : bool;
}

(* Representation of program syntax *)
type program = {
  cfg : CFG.t;
  (* jenndbg this is its control flow *)
  rpc : function_info Env.t;
}
(* jenndbg why does an RPC handler need a list of function info *)

let load (var : string) (env : record_env) : value =
  try Env.find env.local_env var
  with Not_found -> (
    try Env.find env.node_env var
    with Not_found ->
      Log.err (fun m -> m "load fail: %s" var);
      failwith "Variable not found")

(* Helper to wake up records waiting on a future *)
let wake_waiters (fut : future_value) : unit =
  match fut.value with
  | Some value ->
      List.iter (fun callback -> callback value) fut.waiters;
      fut.waiters <- []
  | None -> failwith "Cannot wake waiters on unresolved future"

(* Helper to update a collection (map or list) with a new value *)
let update_collection (col : value) (key : value) (vl : value) : value =
  match col with
  | VMap m -> VMap (ValueMap.add key vl m)
  | VList l ->
      let idx = expect_int key in
      VList (List.mapi (fun i x -> if i = idx then vl else x) l)
  | _ -> failwith "Not a collection"

(* Evaluate an expression in the context of an environment. *)
let rec eval (env : record_env) (expr : expr) : value =
  match expr with
  | EInt i -> VInt i
  | EBool b -> VBool b
  | EVar v -> load v env
  | EFind (c, k) -> (
      match eval env c with
      | VMap map -> ValueMap.find (eval env k) map
      | VList l -> (
          match l with
          | [] -> failwith "Cannot index into empty list"
          | _ -> (
              match eval env k with
              | VInt i | VNode i ->
                  if i < 0 || i >= List.length l then (
                    Log.err (fun m -> m "idx %d, len %d" i (List.length l));
                    failwith "idx out of range of VList")
                  else List.nth l i
              | other ->
                  failwith
                    (Format.asprintf "Cannot index into a list with %s"
                       (type_name other))))
      | VString s -> (
          match load s env with
          | VMap map -> ValueMap.find (eval env k) map
          | _ ->
              failwith
                "EFind eval fail: cannot index into anything else but map with \
                 string")
      | other ->
          Log.err (fun m ->
              m "Collection %s is %s, cannot index into using %s"
                (to_string_expr c) (type_name other) (to_string_expr k));
          failwith
            (Format.asprintf "EFind eval fail: cannot index into %s"
               (type_name other)))
  | ENot e -> (
      match eval env e with
      | VBool b -> VBool (not b)
      | other -> failwith (Format.asprintf "Cannot negate %s" (type_name other))
      )
  | EAnd (e1, e2) ->
      VBool (expect_bool (eval env e1) && expect_bool (eval env e2))
  | EOr (e1, e2) ->
      VBool (expect_bool (eval env e1) || expect_bool (eval env e2))
  | EEqualsEquals (e1, e2) -> (
      match (eval env e1, eval env e2) with
      (* Same type comparisons *)
      | VInt i1, VInt i2 -> VBool (i1 = i2)
      | VBool b1, VBool b2 -> VBool (b1 = b2)
      | VString s1, VString s2 -> VBool (s1 = s2)
      | VNode n1, VNode n2 -> VBool (n1 = n2)
      | VUnit, VUnit -> VBool true
      | VOption o1, VOption o2 -> VBool (o1 = o2)
      | VTuple a1, VTuple a2 ->
          VBool
            (try Array.for_all2 ( = ) a1 a2 with Invalid_argument _ -> false)
      | VList l1, VList l2 ->
          let rec list_eq l1 l2 =
            match (l1, l2) with
            | [], [] -> true
            | [], _ | _, [] -> false
            | hd1 :: tl1, hd2 :: tl2 ->
                if hd1 = hd2 then list_eq tl1 tl2 else false
          in
          VBool (list_eq l1 l2)
      (* Special case: VNode and VInt are interchangeable *)
      | VNode n, VInt i | VInt i, VNode n -> VBool (n = i)
      (* Reference types that cannot be meaningfully compared *)
      | VMap _, _ | _, VMap _ ->
          failwith "Cannot compare maps for equality (reference type)"
      | VFuture _, _ | _, VFuture _ ->
          failwith "Cannot compare futures for equality (reference type)"
      | VLock _, _ | _, VLock _ ->
          failwith "Cannot compare locks for equality (reference type)"
      (* Different types return false *)
      | _ -> VBool false)
  | EMap kvp ->
      let rec makemap (kvpairs : (expr * expr) list) : value ValueMap.t =
        match kvpairs with
        | [] -> ValueMap.empty
        | (k, v) :: rest ->
            let tbl = makemap rest in
            ValueMap.add (eval env k) (eval env v) tbl
      in
      VMap (makemap kvp)
  | EList exprs ->
      let rec makelist (exprs : expr list) : value list =
        match exprs with [] -> [] | e :: rest -> eval env e :: makelist rest
      in
      VList (makelist exprs)
  | EListPrepend (item, ls) ->
      let v = eval env item in
      let l = expect_list (eval env ls) in
      VList (v :: l)
  | EListAppend (ls, item) ->
      let l = expect_list (eval env ls) in
      let v = eval env item in
      VList (l @ [ v ])
  | EListSubsequence (ls, start_idx, end_idx) -> (
      let l = expect_list (eval env ls) in
      let start_idx = expect_int (eval env start_idx) in
      let end_idx = expect_int (eval env end_idx) in
      match l with
      | [] -> failwith "EListSubsequence eval fail on empty list"
      | _ ->
          if
            start_idx < 0 || end_idx < 0
            || start_idx >= List.length l
            || end_idx > List.length l
          then failwith "EListSubsequence eval fail on out of bounds"
          else
            let rec subseq (lst : value list) (start_idx : int) (end_idx : int)
                : value list =
              match lst with
              | [] -> []
              | hd :: tl ->
                  if start_idx = 0 then
                    if end_idx = 0 then []
                    else hd :: subseq tl start_idx (end_idx - 1)
                  else subseq tl (start_idx - 1) (end_idx - 1)
            in
            VList (subseq l start_idx end_idx))
  | EString s -> VString s
  | ELessThan (e1, e2) ->
      VBool (expect_int (eval env e1) < expect_int (eval env e2))
  | ELessThanEquals (e1, e2) ->
      VBool (expect_int (eval env e1) <= expect_int (eval env e2))
  | EGreaterThan (e1, e2) ->
      VBool (expect_int (eval env e1) > expect_int (eval env e2))
  | EGreaterThanEquals (e1, e2) ->
      VBool (expect_int (eval env e1) >= expect_int (eval env e2))
  | EKeyExists (key, mp) ->
      let k = eval env key in
      let m = expect_map (eval env mp) in
      VBool (ValueMap.mem k m)
  | EMapErase (key, mp) ->
      let k = eval env key in
      let m = expect_map (eval env mp) in
      VMap (ValueMap.remove k m)
  | EListLen e -> (
      match eval env e with
      | VList l -> VInt (List.length l)
      | VMap m -> VInt (ValueMap.cardinal m)
      | other ->
          failwith
            (Format.asprintf "EListLen eval fail on %s (not a collection)"
               (type_name other)))
  | EListAccess (ls, idx) -> (
      let l = expect_list (eval env ls) in
      match l with
      | [] -> failwith "EListAccess eval fail on empty list"
      | _ ->
          if List.length l <= idx || idx < 0 then
            failwith "idx out of range in EListAccess"
          else List.nth l idx)
  | EPlus (e1, e2) -> VInt (expect_int (eval env e1) + expect_int (eval env e2))
  | EMinus (e1, e2) -> VInt (expect_int (eval env e1) - expect_int (eval env e2))
  | ETimes (e1, e2) -> VInt (expect_int (eval env e1) * expect_int (eval env e2))
  | EDiv (e1, e2) -> VInt (expect_int (eval env e1) / expect_int (eval env e2))
  | EMod (e1, e2) -> VInt (expect_int (eval env e1) mod expect_int (eval env e2))
  | EMin (e1, e2) ->
      VInt (min (expect_int (eval env e1)) (expect_int (eval env e2)))
  | EUnit -> VUnit
  | ENil -> VOption None (* nil literal evaluates to VOption None *)
  | ETuple exprs -> VTuple (Array.of_list (List.map (eval env) exprs))
  | ETupleAccess (e_tuple, idx) -> (
      let arr = expect_tuple (eval env e_tuple) in
      try arr.(idx)
      with Invalid_argument _ ->
        failwith
          ("Runtime error: Tuple index " ^ string_of_int idx ^ " out of bounds")
      )
  | EUnwrap e -> (
      match expect_option (eval env e) with
      | Some v -> v
      | None -> failwith "Runtime error: Attempted to unwrap nil")
  | EStore (col, key, value) ->
      let col_val = eval env col in
      let key_val = eval env key in
      let val_val = eval env value in
      update_collection col_val key_val val_val
  | ECoalesce (e_opt, e_default) -> (
      match expect_option (eval env e_opt) with
      | Some v -> v
      | None -> eval env e_default)
  | ECreatePromise -> VFuture { value = None; waiters = [] }
  | ECreateLock -> VLock (ref false)
  | ESome e -> VOption (Some (eval env e))
  | EIntToString e -> VString (string_of_int (expect_int (eval env e)))

let rec store (l : lhs) (vl : value) (env : record_env) : unit =
  match l with
  | LVar var ->
      if Env.mem env.local_env var then Env.replace env.local_env var vl
      else Env.replace env.node_env var vl
  | LTuple vars -> (
      match vl with
      | VTuple vals ->
          if List.length vars <> Array.length vals then
            failwith "Tuple assignment mismatch"
          else List.iteri (fun i var -> store (LVar var) vals.(i) env) vars
      | _ -> failwith "Cannot assign non-tuple to tuple lhs")

exception Halt
exception SyncReturn of value

let copy (lhs : lhs) (vl : value) (env : record_env) : unit =
  (* Since values are immutable, copy is just assignment *)
  store lhs vl env

let function_info name program =
  try Env.find program.rpc name
  with Not_found ->
    Log.err (fun m -> m "function %s is not defined in program.functions" name);
    failwith "Function not found"

let rec exec_sync (state : state) (program : program) (env : record_env)
    (start_pc : CFG.vertex) : value =
  let current_pc = ref start_pc in
  try
    while true do
      let label = CFG.label program.cfg !current_pc in
      match label with
      | Instr (instruction, next) -> (
          current_pc := next;
          match instruction with
          | Assign (lhs, rhs) -> store lhs (eval env rhs) env
          | Copy (lhs, rhs) -> copy lhs (eval env rhs) env
          | SyncCall (lhs, func_name, actual_exprs) ->
              let actual_values = List.map (eval env) actual_exprs in
              let { entry; formals; locals; is_sync; _ } =
                function_info func_name program
              in

              if not is_sync then
                failwith
                  ("Runtime Error: 'sync' function tried to call non-sync \
                    function '" ^ func_name ^ "' synchronously");

              let callee_env = Env.create 1024 in
              (try
                 List.iter2
                   (fun f a -> Env.add callee_env f a)
                   formals actual_values
               with Invalid_argument _ ->
                 failwith "Mismatched arguments in recursive sync call");

              List.iter
                (fun (var_name, default_expr) ->
                  Env.add callee_env var_name (eval env default_expr))
                locals;
              let callee_record_env =
                { local_env = callee_env; node_env = env.node_env }
              in
              Env.add callee_record_env.local_env "self" (load "self" env);
              let result = exec_sync state program callee_record_env entry in
              store lhs result env
          | Async (lhs, node, func, actuals) -> (
              match eval env node with
              | VNode node_id | VInt node_id ->
                  let new_future = { value = None; waiters = [] } in
                  let { entry; formals; locals; _ } =
                    function_info func program
                  in
                  let new_env = Env.create 1024 in
                  (try
                     List.iter2
                       (fun formal actual ->
                         Env.add new_env formal (eval env actual))
                       formals actuals
                   with Invalid_argument _ ->
                     failwith "Mismatched arguments in function call");
                  List.iter
                    (fun (var_name, expr) ->
                      Env.add new_env var_name (eval env expr))
                    locals;

                  (* We need to find the current node ID. 
                   In exec_sync, 'self' is loaded in the env. *)
                  let origin_node = expect_node (load "self" env) in

                  let new_record =
                    {
                      node = node_id;
                      origin_node;
                      pc = entry;
                      continuation =
                        (fun value ->
                          new_future.value <- Some value;
                          wake_waiters new_future);
                      env = new_env;
                      id = -1;
                      x = 0.5;
                      f = (fun x -> x);
                    }
                  in
                  store lhs (VFuture new_future) env;

                  if BatSet.Int.mem node_id state.crash_info.currently_crashed
                  then
                    state.crash_info.queued_messages <-
                      (node_id, new_record) :: state.crash_info.queued_messages
                  else DA.add state.runnable_records new_record
              | other ->
                  failwith
                    (Format.asprintf "Type error: expected node for RPC, got %s"
                       (type_name other)))
          | Resolve (lhs, rhs) -> (
              let value_to_resolve = eval env rhs in
              let promise_val =
                match lhs with
                | LVar var -> load var env
                | LTuple _ ->
                    failwith
                      ("Runtime error: Cannot resolve a tuple at "
                     ^ to_string_lhs lhs)
              in
              match promise_val with
              | VFuture r ->
                  if r.value = None then (
                    r.value <- Some value_to_resolve;
                    wake_waiters r)
                  else
                    failwith
                      ("Runtime error: Promise already resolved at "
                     ^ to_string_lhs lhs)
              | other ->
                  failwith
                    (Format.asprintf
                       "Type error: Attempted to resolve \n%s (not a promise)"
                       (type_name other))))
      | Cond (cond, bthen, belse) -> (
          match eval env cond with
          | VBool true -> current_pc := bthen
          | VBool false -> current_pc := belse
          | other ->
              failwith
                (Format.asprintf
                   "Type error in sync cond: expected \nbool, got %s"
                   (type_name other)))
      | Return expr -> raise (SyncReturn (eval env expr))
      | Print (expr, next) ->
          Log.app (fun m -> m "%s" (to_string_value (eval env expr)));
          current_pc := next
      | Break target_vertex -> current_pc := target_vertex
      | ForLoopIn (lhs, expr, body, next) -> (
          (* 1. Ensure we are iterating over a variable so we can update it *)
          let var_name =
            match expr with
            | EVar name -> name
            | _ -> failwith "ForLoopIn expression must be a variable"
          in

          match eval env expr with
          | VMap map -> (
              if ValueMap.is_empty map then current_pc := next
              else
                let k, v = ValueMap.min_binding map in
                let new_map = ValueMap.remove k map in

                store (LVar var_name) (VMap new_map) env;
                match lhs with
                | LTuple [ key; value ] ->
                    Env.add env.local_env key k;
                    Env.add env.local_env value v;
                    current_pc := body
                | _ ->
                    Log.err (fun m ->
                        m "failed to iterate map with lhs: %s"
                          (to_string_lhs lhs));
                    failwith
                      "Cannot iterate through map with anything other than a \
                       2-tuple")
          | VList list -> (
              match list with
              | [] -> current_pc := next
              | item :: rest -> (
                  store (LVar var_name) (VList rest) env;

                  match lhs with
                  | LVar var ->
                      Env.add env.local_env var item;
                      current_pc := body
                  | _ ->
                      Log.err (fun m ->
                          m "failed to iterate list with lhs %s"
                            (to_string_lhs lhs));
                      failwith
                        "Cannot iterate through list with anything other than \
                         a single variable"))
          | other ->
              Log.err (fun m ->
                  m "failed to iterate collection: %s" (to_string_expr expr));
              failwith
                (Format.asprintf "ForLoopIn on %s (not a collection)"
                   (type_name other)))
      | Lock (_, _) | Unlock (_, _) ->
          failwith
            "Runtime Error: 'lock'/'unlock' cannot be used in a 'sync' function"
      | Pause _ | Await (_, _, _) | SpinAwait (_, _) ->
          failwith
            "Runtime Error: Async operation (Pause, Await) in 'sync' function"
    done;
    VUnit
  with SyncReturn v -> v

(* Execute record until pause/return.*)
let exec (state : state) (program : program) (record : record) =
  let env = { local_env = record.env; node_env = state.nodes.(record.node) } in
  Env.add env.local_env "self" (VNode record.node);
  let rec loop () =
    match CFG.label program.cfg record.pc with
    | Instr (instruction, next) ->
        record.pc <- next;
        (match instruction with
        | Async (lhs, node, func, actuals) -> (
            match eval env node with
            | VNode node_id | VInt node_id ->
                let new_future = { value = None; waiters = [] } in
                let { entry; formals; locals; _ } =
                  function_info func program
                in
                let new_env = Env.create 1024 in
                (try
                   List.iter2
                     (fun formal actual ->
                       Env.add new_env formal (eval env actual))
                     formals actuals
                 with Invalid_argument _ ->
                   Log.err (fun m ->
                       m
                         "Func %s mismatches def and caller args\n\
                         \                    formals: %s\n\n\
                         \                               actuals: %s"
                         func
                         (String.concat ", " formals)
                         (String.concat ", \n"
                            (List.map to_string_expr actuals)));
                   failwith "Mismatched arguments in function call");
                List.iter
                  (fun (var_name, expr) ->
                    Env.add new_env var_name (eval env expr))
                  locals;
                let new_record =
                  {
                    node = node_id;
                    origin_node = record.node;
                    pc = entry;
                    continuation =
                      (fun value ->
                        new_future.value <- Some value;
                        wake_waiters new_future);
                    env = new_env;
                    id = record.id;
                    x = record.x;
                    f = record.f;
                  }
                in
                store lhs (VFuture new_future) env;
                if BatSet.Int.mem node_id state.crash_info.currently_crashed
                then (
                  Log.debug (fun m ->
                      m
                        "Queueing new message from %d to crashed node %d (from \
                         exec)"
                        record.node node_id);
                  state.crash_info.queued_messages <-
                    (node_id, new_record) :: state.crash_info.queued_messages)
                else DA.add state.runnable_records new_record
            | other ->
                failwith
                  (Format.asprintf "Type error: expected node for RPC, got %s"
                     (type_name other)))
        | SyncCall (lhs, func_name, actual_exprs) ->
            (* Evaluate args in the async env *)
            let actual_values = List.map (eval env) actual_exprs in

            let { entry; formals; locals; is_sync; _ } =
              function_info func_name program
            in

            if not is_sync then
              failwith
                ("Runtime error: 'exec' tried to SyncCall non-sync function: "
               ^ func_name);

            (* Create the callee's environment *)
            let callee_env = Env.create 1024 in
            (try
               List.iter2
                 (fun f a -> Env.add callee_env f a)
                 formals actual_values
             with Invalid_argument _ ->
               failwith "Mismatched arguments in sync call");

            List.iter
              (fun (var_name, default_expr) ->
                Env.add callee_env var_name (eval env default_expr)
                (* Eval defaults in caller's context *))
              locals;

            let callee_record_env =
              {
                local_env = callee_env;
                node_env = env.node_env (* Share node state *);
              }
            in
            Env.add callee_record_env.local_env "self" (VNode record.node);

            (* Run the sync call to completion *)
            let return_value =
              exec_sync state program callee_record_env entry
            in
            store lhs return_value env;
            loop ()
        | Assign (lhs, rhs) -> store lhs (eval env rhs) env
        | Copy (lhs, rhs) -> copy lhs (eval env rhs) env
        | Resolve (lhs, rhs) -> (
            let value_to_resolve = eval env rhs in
            let promise_val =
              match lhs with
              | LVar var -> load var env
              | LTuple _ ->
                  failwith
                    ("Runtime error: Cannot resolve a tuple at "
                   ^ to_string_lhs lhs)
            in
            match promise_val with
            | VFuture fut ->
                if fut.value = None then (
                  fut.value <- Some value_to_resolve;
                  wake_waiters fut)
                else
                  failwith
                    ("Runtime error: Promise already resolved at "
                   ^ to_string_lhs lhs)
            | other ->
                failwith
                  (Format.asprintf
                     "Type error: Attempted to resolve %s (not a promise)"
                     (type_name other))));
        loop ()
    | Cond (cond, bthen, belse) ->
        (match eval env cond with
        | VBool true -> record.pc <- bthen
        | VBool false -> record.pc <- belse
        | other ->
            failwith
              (Format.asprintf "Type error in condition: expected bool, got %s"
                 (type_name other)));
        loop ()
    | Await (lhs, expr, next) -> (
        match eval env expr with
        | VFuture fut -> (
            match fut.value with
            | Some value ->
                record.pc <- next;
                store lhs value env;
                loop ()
            | None ->
                let resume_callback value =
                  record.pc <- next;
                  store lhs value env;

                  if
                    BatSet.Int.mem record.node
                      state.crash_info.currently_crashed
                  then (
                    (* Node is crashed: Queue for recovery *)
                    Log.debug (fun m ->
                        m
                          "Queueing (from waiter) task from %d for crashed \
                           node %d"
                          record.origin_node record.node);
                    state.crash_info.queued_messages <-
                      (record.node, record) :: state.crash_info.queued_messages)
                  else
                    (* Node is alive: Add back to runnable records *)
                    DA.add state.runnable_records record
                in
                fut.waiters <- resume_callback :: fut.waiters;
                state.waiting_records <- record :: state.waiting_records)
        | other ->
            failwith
              (Format.asprintf "Type error in await: expected future, got %s"
                 (type_name other)))
    | SpinAwait (expr, next) -> (
        match eval env expr with
        | VBool b ->
            if b then (
              record.pc <- next;
              loop ())
            else
              (* Still waiting. *)
              DA.add state.runnable_records record
        | other ->
            failwith
              (Format.asprintf "Type error in SpinAwait: expected bool, got %s"
                 (type_name other)))
    | Return expr -> record.continuation (eval env expr)
    | Pause next ->
        record.pc <- next;
        DA.add state.runnable_records record
    | ForLoopIn (lhs, expr, body, next) -> (
        let var_name =
          match expr with
          | EVar name -> name
          | _ -> failwith "ForLoopIn expression must be a variable"
        in
        let collection = eval env expr in
        match collection with
        | VMap map -> (
            if ValueMap.is_empty map then (
              record.pc <- next;
              loop ())
            else
              (* Optimization: Direct access via min_binding *)
              let k, v = ValueMap.min_binding map in
              let new_map = ValueMap.remove k map in

              store (LVar var_name) (VMap new_map) env;

              match lhs with
              | LTuple [ key; value ] ->
                  Env.add env.local_env key k;
                  Env.add env.local_env value v;
                  record.pc <- body;
                  loop ()
              | _ ->
                  Log.err (fun m ->
                      m "failed to iterate map with lhs: %s" (to_string_lhs lhs));
                  failwith
                    "Cannot iterate through map with anything other than a \
                     2-tuple")
        | VList list -> (
            match list with
            | [] ->
                record.pc <- next;
                loop ()
            | removed_item :: new_list -> (
                (* Optimization: split list into head and tail *)
                store (LVar var_name) (VList new_list) env;
                match lhs with
                | LVar var ->
                    Env.add env.local_env var removed_item;
                    record.pc <- body;
                    loop ()
                | _ ->
                    Log.err (fun m ->
                        m "failed to iterate list with lhs %s"
                          (to_string_lhs lhs));
                    failwith
                      "Cannot iterate through list with anything other than a \
                       single variable"))
        | other ->
            Log.err (fun m ->
                m "failed to iterate collection: %s" (to_string_expr expr));
            failwith
              (Format.asprintf "ForLoopIn on %s (not a collection)"
                 (type_name other)))
    | Print (expr, next) ->
        Log.app (fun m -> m "%s" (to_string_value (eval env expr)));
        record.pc <- next;
        loop ()
    | Break target_vertex ->
        record.pc <- target_vertex;
        loop ()
    | Lock (lock_expr, next) ->
        let lock_ref = expect_lock (eval env lock_expr) in
        if !lock_ref = false then (
          (* Lock is free, acquire it *)
          lock_ref := true;
          record.pc <- next;
          loop ())
        else
          (* Lock is held, re-queue the record to wait *)
          DA.add state.runnable_records record
    | Unlock (lock_expr, next) ->
        let lock_ref = expect_lock (eval env lock_expr) in
        if !lock_ref = true then (
          (* Lock is held, release it *)
          lock_ref := false;
          record.pc <- next;
          loop ())
        else
          failwith
            "Runtime error: Attempted to 'Unlock' an already-unlocked lock"
  in
  loop ()

let schedule_record (state : state) (program : program)
    (randomly_drop_msgs : bool) (cut_tail_from_mid : bool)
    (sever_all_but_mid : bool) (partition_away_nodes : int list)
    (randomly_delay_msgs : bool) : unit =
  if false then
    Log.debug (fun m ->
        m "%b %b %b %b" randomly_drop_msgs cut_tail_from_mid sever_all_but_mid
          (List.length partition_away_nodes = 0));

  let len = DA.length state.runnable_records in
  if len = 0 then raise Halt;

  let idx =
    Random.self_init ();
    Random.int len
  in

  (* Swap-remove: move the chosen record to the end, then pop it *)
  let r = DA.get state.runnable_records idx in
  let last_idx = len - 1 in
  (if idx <> last_idx then
     let last = DA.get state.runnable_records last_idx in
     DA.set state.runnable_records idx last);
  DA.delete_last state.runnable_records;

  if BatSet.Int.mem r.node state.crash_info.currently_crashed then
    (* This should never happen *)
    let _ =
      Log.err (fun m -> m "Failure: source %d for dst %d " r.origin_node r.node)
    in
    () (* Record is already removed, just drop it *)
  else
    let env = { local_env = r.env; node_env = state.nodes.(r.node) } in
    match CFG.label program.cfg r.pc with
    | Instr (i, _) -> (
        match i with
        | Async (_, node, _, _) ->
            let node_id = expect_node (eval env node) in
            let src_node = r.node in
            let dest_node = node_id in

            (* If destination is crashed, queue the message *)
            if BatSet.Int.mem dest_node state.crash_info.currently_crashed then (
              Log.debug (fun m ->
                  m "  Queueing message from %d to crashed node %d (step %d)"
                    src_node dest_node state.crash_info.current_step);
              state.crash_info.queued_messages <-
                (dest_node, r) :: state.crash_info.queued_messages)
            else
              let should_execute = ref true in

              (* Only apply fault injection to non-local calls *)
              if src_node <> dest_node then (
                if
                  randomly_drop_msgs
                  &&
                  (Random.self_init ();
                   Random.float 1.0 < 0.3)
                then should_execute := false;

                if
                  cut_tail_from_mid
                  && ((src_node = 2 && dest_node = 1)
                     || (dest_node = 2 && src_node = 1))
                then should_execute := false;

                if sever_all_but_mid then
                  if dest_node = 2 && not (src_node = 1) then
                    should_execute := false
                  else if src_node = 2 && not (dest_node = 1) then
                    should_execute := false;

                if
                  List.mem src_node partition_away_nodes
                  || List.mem dest_node partition_away_nodes
                then should_execute := false;

                if randomly_delay_msgs then
                  if
                    Random.self_init ();
                    Random.float 1.0 < r.x
                  then (
                    r.x <- r.f r.x;
                    should_execute := false));

              if !should_execute then exec state program r
        | _ -> exec state program r)
    | _ -> exec state program r

(* Helper function to schedule a thread *)
let schedule_thread (state : state) (program : program) (func_name : string)
    (actuals : value list) (unique_id : int) (origin : int)
    (get_free_threads : unit -> int list)
    (update_free_threads : int list -> unit) : unit =
  let rec pick n before after =
    match after with
    | [] -> raise Halt
    | c :: cs ->
        if n == 0 then (
          let op = function_info func_name program in
          let env = Env.create 1024 in
          List.iter2
            (fun formal actual -> Env.add env formal actual)
            op.formals actuals;

          let temp_record_env =
            { local_env = env; node_env = state.nodes.(c) }
          in
          List.iter
            (fun (var_name, default_expr) ->
              Env.add env var_name (eval temp_record_env default_expr))
            op.locals;
          let invocation =
            {
              client_id = c;
              op_action = op.name;
              kind = Invocation;
              payload = actuals;
              unique_id;
            }
          in
          let continuation value =
            (* After completing the operation, add response to the history and
             allow thread to be scheduled again. *)
            let response =
              {
                client_id = c;
                op_action = op.name;
                kind = Response;
                payload = [ value ];
                unique_id;
              }
            in
            DA.add state.history response;
            let current_free_list = get_free_threads () in
            update_free_threads (c :: current_free_list)
          in
          let record =
            {
              pc = op.entry;
              node = c;
              origin_node = origin;
              continuation;
              env;
              id = unique_id;
              x = 0.4;
              f = (fun x -> x /. 2.0);
            }
          in
          update_free_threads (List.rev_append before cs);
          DA.add state.history invocation;
          DA.add state.runnable_records record)
        else pick (n - 1) (c :: before) cs
  in
  pick
    (Random.self_init ();
     Random.int (List.length (get_free_threads ())))
    [] (get_free_threads ())

(* Choose a client without a pending operation, create a new activation record
   to execute it, and append the invocation to the history *)
let schedule_client (state : state) (program : program) (func_name : string)
    (actuals : value list) (unique_id : int) (origin : int) : unit =
  schedule_thread state program func_name actuals unique_id origin
    (fun () -> state.free_clients)
    (fun threads -> state.free_clients <- threads)
