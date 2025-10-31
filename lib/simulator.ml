module DA = BatDynArray
(* A couple of features of the language in ast.ml make it difficult to
   implement directly:
   - RPCs and function calls in RHSs are complicated (e.g., consider an RPC as
     an argument of an RPC).  If there's a need for such constructs, the usual
     solution is to translate them away -- e.g.,
       y = f(g(x))
     gets translated to the IR
       tmp = g(x)
      
 y = f(tmp)
   - An RPC to a func that nominally returns (say) a string cannot evaluate to
     an string -- otherwise, the only way to execute the program is to preempt
     the current activation record and wait until the RPC returns to resume.  In
     particular, this makes it very difficult to implement fault-tolerant
     protocols -- if node A calls an RPC on node B and node B fails, then node
     A's activation record is blocked 
forever.  The proposed alternative is that
     an RPC that nomially returns a string evaluates to Future<string>.  We can
     manipulate a valute of type Future<string> by
   - checking whether its value is available (e.g.
fut.isAvailable() -> bool)
       (nonpreemptible)
   - retrieving its value (e.g. fut.isValue() -> string) (preemptible)
   - In general, I do not understand how "Roles" should be implemented
*)

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
  | EPollForResps of expr * expr
  | EPollForAnyResp of expr
  | ENextResp of expr
  | EMin of expr * expr
  | ETuple of expr list
  | ETupleAccess of expr * int
  | EUnit
  | ENil
  | EUnwrap of expr
  | ECoalesce of expr * expr
[@@deriving ord]

type lhs = LVar of string | LAccess of expr * expr | LTuple of string list
[@@deriving ord]

type instr =
  | Assign of lhs * expr (* jenndbg probably assigning map values? *)
  | Async of lhs * expr * string * expr list (* jenndbg RPC*)
  | Copy of lhs * expr
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
    | VList of t list ref
    | VOption of t option
    | VFuture of t option ref
    | VNode of int
    | VString of string
    | VUnit
    | VTuple of t array
end = struct
  type t =
    | VInt of int
    | VBool of bool
    | VMap of t ValueMap.t
    | VList of t list ref
    | VOption of t option
    | VFuture of t option ref
    | VNode of int
    | VString of string
    | VUnit
    | VTuple of t array
end

and ValueKey : sig
  type t = Value.t

  val equal : t -> t -> bool
  val hash : t -> int
end = struct
  open Value

  type t = Value.t

  let rec equal v1 v2 =
    match (v1, v2) with
    | VInt i1, VInt i2 -> i1 = i2
    | VBool b1, VBool b2 -> b1 = b2
    | VString s1, VString s2 -> s1 = s2
    | VNode n1, VNode n2 -> n1 = n2
    | VUnit, VUnit -> true
    | VOption o1, VOption o2 -> (
        match (o1, o2) with
        | None, None -> true
        | Some v1', Some v2' ->
            equal v1' v2' (* Options are immutable, so this is fine *)
        | _ -> false)
    | VTuple a1, VTuple a2 -> (
        try Array.for_all2 equal a1 a2 with Invalid_argument _ -> false)
    | VList l1, VList l2 -> l1 == l2
    | VFuture f1, VFuture f2 -> f1 == f2
    | VMap m1, VMap m2 -> m1 == m2
    | _ -> false

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
    | VMap m -> Hashtbl.hash m
end

and ValueMap : (Hashtbl.S with type key = Value.t) = Hashtbl.Make (ValueKey)

(* Type alias for convenience - expose constructors *)
type value = Value.t =
  | VInt of int
  | VBool of bool
  | VMap of value ValueMap.t
  | VList of value list ref
  | VOption of value option
  | VFuture of value option ref
  | VNode of int
  | VString of string
  | VUnit
  | VTuple of value array

(* Run-time value of a left-hand-side *)
type lvalue =
  | LVVar of string
  | LVAccess of (value * value ValueMap.t)
  | LVAccessList of (value * value list ref)
  | LVTuple of string list

module Env = Hashtbl.Make (struct
  type t = string

  let hash = Hashtbl.hash
  let equal = ( = )
end)

type 'a label =
  | Instr of instr * 'a (* jenndbg assignments or RPCs *)
  | Pause of 'a (* Insert pause to allow the scheduler to interrupt! *)
  | Await of lhs * expr * 'a
  | Return of expr (* jenndbg return...I guess? *)
  (*| Read (* jenndbg read a value *)*)
  | Cond of expr * 'a * 'a
  | ForLoopIn of lhs * expr * 'a * 'a
  | Print of expr * 'a
  | Break of 'a

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
  | EPollForResps (e1, e2) ->
      "EPollForResps(" ^ to_string_expr e1 ^ ", " ^ to_string_expr e2 ^ ")"
  | EPollForAnyResp e -> "EPollForAnyResp(" ^ to_string_expr e ^ ")"
  | ENextResp e -> "NextResp(" ^ to_string_expr e ^ ")"
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

let to_string_lhs (l : lhs) : string =
  match l with
  | LVar s -> s
  | LAccess (e1, e2) -> to_string_expr e1 ^ "[" ^ to_string_expr e2 ^ "]"
  | LTuple lst -> "(" ^ String.concat ", " lst ^ ")"

let to_string (l : 'a label) : string =
  match l with
  | Instr (i, _) -> (
      match i with
      | Async (_, n, f, _) ->
          "instr async to " ^ to_string_expr n ^ " with " ^ f
      | Assign (lhs, expr) ->
          "Instr(Assign(" ^ to_string_lhs lhs ^ ", " ^ to_string_expr expr
          ^ "))"
      | Copy (_, _) -> "instr copy")
  | Pause _ -> "Pause"
  | Await (lhs, expr, _) ->
      "Await(" ^ to_string_lhs lhs ^ ", " ^ to_string_expr expr ^ ")"
  | Return _ -> "Return"
  | Cond (expr, _, _) -> "Cond(" ^ to_string_expr expr ^ ", _, _)"
  | ForLoopIn _ -> "ForLoopIn"
  | Print _ -> "Print"
  | Break _ -> "Break"

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
  | VList l -> "VList(" ^ String.concat ", " (List.map to_string_value !l) ^ ")"
  | VOption o ->
      "VOption("
      ^ (match o with Some v -> to_string_value v | None -> "None")
      ^ ")"
  | VFuture f ->
      "VFuture("
      ^ (match !f with Some v -> to_string_value v | None -> "None")
      ^ ")"
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

(* Global state *)
type state = {
  nodes : value Env.t array;
  mutable records : record list;
  history : operation DA.t;
  mutable free_clients :
    int list (* client ids should be valid indexes into nodes *);
  mutable free_sys_threads : int list (* internal systems threads *);
}

(* Execution environment of an activation record: local variables +
   node-scoped variables. *)
type record_env = { local_env : value Env.t; node_env : value Env.t }

type function_info = {
  entry : CFG.vertex;
  name : string;
  formals : string list;
  locals : (string * expr) list;
}

(* Representation of program syntax *)
type program = {
  cfg : CFG.t;
  (* jenndbg this is its control flow *)
  rpc : function_info Env.t;
  client_ops : function_info Env.t;
}
(* jenndbg why does an RPC handler need a list of function info *)

let load (var : string) (env : record_env) : value =
  try Env.find env.local_env var
  with Not_found -> (
    try Env.find env.node_env var
    with Not_found ->
      Printf.printf "load fail: %s\n" var;
      failwith "Variable not found")

(* Evaluate an expression in the context of an environment. *)
let rec eval (env : record_env) (expr : expr) : value =
  match expr with
  | EInt i -> VInt i
  | EBool b -> VBool b
  | EVar v -> load v env
  | EFind (c, k) -> (
      match eval env c with
      | VMap map -> ValueMap.find map (eval env k)
      | VList l -> (
          match !l with
          | [] -> failwith "Cannot index into empty list"
          | _ -> (
              match eval env k with
              | VInt i | VNode i ->
                  if i < 0 || i >= List.length !l then (
                    Printf.printf "idx %d, len %d\n" i (List.length !l);

                    failwith "idx out of range of VList")
                  else List.nth !l i
              | _ -> failwith "Cannot index into a list with non-integer"))
      | VString s -> (
          match load s env with
          | VMap map -> ValueMap.find map (eval env k)
          | _ ->
              failwith
                "EFind eval fail: cannot index into anything else but map with \
                 string")
      | VBool _ ->
          Printf.printf "Collection %s is bool, cannot index into using %s\n"
            (to_string_expr c) (to_string_expr k);
          failwith "EFind eval fail: collection is neither map nor string"
      | VInt _ ->
          Printf.printf "Collection %s is int, cannot index into using %s\n"
            (to_string_expr c) (to_string_expr k);
          failwith "EFind eval fail: collection is neither map nor string"
      | VFuture _ ->
          Printf.printf "Collection %s is future, cannot index into using %s\n"
            (to_string_expr c) (to_string_expr k);
          failwith "EFind eval fail: collection is neither map nor string"
      | VNode _ ->
          Printf.printf "Collection %s is node, cannot index into using %s\n"
            (to_string_expr c) (to_string_expr k);
          failwith "EFind eval fail: collection is neither map nor string"
      | VOption _ ->
          Printf.printf "Collection %s is option, cannot index into using %s\n"
            (to_string_expr c) (to_string_expr k);
          failwith "EFind eval fail: collection is neither map nor string"
      | VUnit ->
          Printf.printf "Collection %s is unit, cannot index into using %s\n"
            (to_string_expr c) (to_string_expr k);
          failwith "EFind eval fail: collection is neither map nor string"
      | VTuple _ ->
          Printf.printf "Collection %s is tuple, cannot index into using %s\n"
            (to_string_expr c) (to_string_expr k);
          failwith "EFind eval fail: collection is neither map nor string")
  | ENot e -> (
      match eval env e with
      | VBool b -> VBool (not b)
      | VList _ -> failwith "Cannot negate a list"
      | VInt _ -> failwith "Cannot negate an int"
      | VString _ -> failwith "Cannot negate a string"
      | VNode _ -> failwith "Cannot negate a node"
      | VMap _ -> failwith "Cannot negate a map"
      | VOption _ -> failwith "Cannot negate an option"
      | _ -> failwith "ENot eval fail")
  | EAnd (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VBool b1, VBool b2 -> VBool (b1 && b2)
      | _ -> failwith "EAnd eval fail")
  | EOr (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VBool b1, VBool b2 -> VBool (b1 || b2)
      | _ -> failwith "EOr eval fail")
  | EEqualsEquals (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VBool (i1 = i2)
      | VBool b1, VBool b2 -> VBool (b1 = b2)
      | VString s1, VString s2 -> VBool (s1 = s2)
      | VNode n1, VNode n2 -> VBool (n1 = n2)
      | VNode n, VInt i -> VBool (n = i)
      | VInt i, VNode n -> VBool (i = n)
      | VUnit, VUnit -> VBool true
      | VOption o1, VOption o2 -> VBool (o1 = o2)
      | VMap _, _ -> failwith "EEqualsEquals fails with map"
      | VFuture _, _ -> failwith "EEqualsEquals fails with VFuture"
      | VList l1, VList l2 ->
          let rec list_eq (l1 : value list) (l2 : value list) : bool =
            match (l1, l2) with
            | [], [] -> true
            | [], _ -> false
            | _, [] -> false
            | hd1 :: tl1, hd2 :: tl2 ->
                if hd1 = hd2 then list_eq tl1 tl2 else false
          in
          VBool (list_eq !l1 !l2)
      | VList _, _ -> failwith "EEqualsEquals fails with VList, not VList"
      | VOption _, _ -> failwith "EEqualsEquals fails with option"
      | VInt i, other -> (
          match other with
          | VInt _ -> failwith "EEqualsEquals eval fail VInt, VInt"
          | VBool _ -> failwith "EEqualsEquals eval fail VInt, VBool"
          | VString _ -> failwith "EEqualsEquals eval fail VInt, VString"
          | VNode n ->
              Printf.printf "VInt %d, VNode %d\n" i n;
              failwith "EEqualsEquals eval fail VInt, VNode"
          | VMap _ -> failwith "EEqualsEquals eval fail VInt, VMap"
          | VFuture _ -> failwith "EEqualsEquals eval fail VInt, VFuture"
          | VList _ -> failwith "EEqualsEquals eval fail VInt, VList"
          | VOption _ -> VBool false
          | VUnit -> VBool false
          | VTuple _ -> VBool false)
      | VBool _, _ -> failwith "EEqualsEquals eval fail VBool"
      | VString _, _ -> failwith "EEqualsEquals eval fail VString"
      | VNode _, _ -> failwith "EEqualsEquals eval fail VNode"
      | VUnit, _ | _, VUnit -> VBool false
      | _, VOption _ -> VBool false
      | VTuple _, _ -> VBool false)
  | EMap kvp ->
      let rec makemap (kvpairs : (expr * expr) list) : value ValueMap.t =
        match kvpairs with
        | [] -> ValueMap.create 1024
        | (k, v) :: rest ->
            let tbl = makemap rest in
            ValueMap.add tbl (eval env k) (eval env v);
            tbl
      in
      VMap (makemap kvp)
  | EList exprs ->
      let rec makelist (exprs : expr list) : value list ref =
        match exprs with
        | [] -> ref []
        | e :: rest -> ref (eval env e :: !(makelist rest))
      in
      VList (makelist exprs)
  | EListPrepend (item, ls) -> (
      match (eval env item, eval env ls) with
      | v, VList l -> VList (ref (v :: !l))
      | _ -> failwith "EListPrepend eval fail")
  | EListAppend (ls, item) -> (
      match (eval env ls, eval env item) with
      | VList l, v -> VList (ref (!l @ [ v ]))
      | _ -> failwith "EListAppend eval fail")
  | EListSubsequence (ls, start_idx, end_idx) -> (
      match (eval env ls, eval env start_idx, eval env end_idx) with
      | VList l, VInt start_idx, VInt end_idx -> (
          match !l with
          | [] -> failwith "EListSubsequence eval fail on empty list"
          | _ ->
              if
                start_idx < 0 || end_idx < 0
                || start_idx >= List.length !l
                || end_idx > List.length !l
              then failwith "EListSubsequence eval fail on out of bounds"
              else
                let rec subseq (lst : value list) (start_idx : int)
                    (end_idx : int) : value list =
                  match lst with
                  | [] -> []
                  | hd :: tl ->
                      if start_idx = 0 then
                        if end_idx = 0 then []
                        else hd :: subseq tl start_idx (end_idx - 1)
                      else subseq tl (start_idx - 1) (end_idx - 1)
                in
                VList (ref (subseq !l start_idx end_idx)))
      | _ -> failwith "EListSubsequence eval fail")
  | EString s -> VString s
  | ELessThan (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VBool (i1 < i2)
      | _ -> failwith "ELessThan eval fail")
  | ELessThanEquals (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VBool (i1 <= i2)
      | _ -> failwith "ELessThanEquals eval fail")
  | EGreaterThan (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VBool (i1 > i2)
      | _ -> failwith "EGreaterThan eval fail")
  | EGreaterThanEquals (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VBool (i1 >= i2)
      | _ -> failwith "EGreaterThanEquals eval fail")
  | EKeyExists (key, mp) -> (
      match (eval env key, eval env mp) with
      | k, VMap m -> VBool (ValueMap.mem m k)
      | _ ->
          Printf.printf "%s is not a map, key %s may not exist\n"
            (to_string_expr mp) (to_string_expr key);
          failwith "EKeyExists eval fail")
  | EMapErase (key, mp) -> (
      match (eval env key, eval env mp) with
      | k, VMap m ->
          ValueMap.remove m k;
          VMap m
      | _ ->
          Printf.printf "%s is not a map, cannot erase key %s\n"
            (to_string_expr mp) (to_string_expr key);
          failwith "EMapErase eval fail")
  | EListLen e -> (
      match eval env e with
      | VList l -> VInt (List.length !l)
      | VMap m -> VInt (ValueMap.length m)
      | _ -> failwith "EListLen eval fail on non-collection")
  | EListAccess (ls, idx) -> (
      match eval env ls with
      | VList l -> (
          match !l with
          | [] -> failwith "EListAccess eval fail on empty list"
          | _ ->
              if List.length !l <= idx || idx < 0 then
                failwith "idx out of range in EListAccess"
              else List.nth !l idx)
      | _ -> failwith "Can't index into something that isn't a list")
  | EPlus (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VInt (i1 + i2)
      | _, VBool _ ->
          Printf.printf "e1 %s, e2 %s\n" (to_string_expr e1) (to_string_expr e2);
          failwith "EPlus eval fail _, VBool"
      | VBool _, _ ->
          Printf.printf "e1 %s, e2 %s\n" (to_string_expr e1) (to_string_expr e2);
          failwith "EPlus eval fail VBool _"
      | _ -> failwith "EPlus eval fail")
  | EMinus (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VInt (i1 - i2)
      | VInt _, _ -> failwith "EMinus VInt fail"
      | VBool _, _ -> failwith "EMinus VBool fail"
      | VFuture _, _ -> failwith "EMinus VFuture fail"
      | VList _, _ -> failwith "EMinus VList fail"
      | VMap _, _ -> failwith "EMinus VMap fail"
      | VOption _, _ -> failwith "EMinus VOption fail"
      | VNode _, _ -> failwith "EMinus VNode fail"
      | VString _, _ -> failwith "EMinus VString fail"
      | VUnit, _ -> failwith "EMinus VUnit fail"
      | VTuple _, _ -> failwith "EMinus VTuple fail")
  | ETimes (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VInt (i1 * i2)
      | _ -> failwith "ETimes eval fail")
  | EDiv (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VInt (i1 / i2)
      | _ -> failwith "EDiv eval fail")
  | EMod (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VInt (i1 mod i2)
      | _ -> failwith "EMod eval fail")
  | EPollForResps (e1, _) -> (
      match eval env e1 with
      | VList list_ref -> (
          match !list_ref with
          | [] -> failwith "Polling for response on empty list"
          | _ ->
              let rec poll_for_response (lst : value list) : int =
                match lst with
                | [] -> 0
                | hd :: tl -> (
                    match hd with
                    | VFuture fut -> (
                        match !fut with
                        (* | Some v -> if (v = eval env e2) then 1 + poll_for_response tl else poll_for_response tl *)
                        | Some _ -> 1 + poll_for_response tl
                        | None -> poll_for_response tl)
                    | VBool b -> (
                        match b with
                        | true -> 1 + poll_for_response tl
                        | false -> poll_for_response tl)
                    | _ ->
                        failwith
                          "Polling for response that isn't a future or bool")
              in
              VInt (poll_for_response !list_ref))
      | VMap m ->
          let lst = ValueMap.fold (fun _ v acc -> v :: acc) m [] in
          let rec poll_for_response (lst : value list) : int =
            match lst with
            | [] -> 0
            | hd :: tl -> (
                match hd with
                | VFuture fut -> (
                    match !fut with
                    (* | Some v -> if (v = eval env e2) then 1 + poll_for_response tl 
else poll_for_response tl *)
                    | Some _ -> 1 + poll_for_response tl
                    | None -> poll_for_response tl)
                | VBool b -> (
                    match b with
                    | true -> 1 + poll_for_response tl
                    | false -> poll_for_response tl)
                | _ ->
                    failwith
                      "Polling for response that isn't a future or bool in map")
          in
          VInt (poll_for_response lst)
      | _ -> failwith "Polling for response on non-collection")
  | EPollForAnyResp rhs -> (
      match eval env rhs with
      | VList list_ref -> (
          match !list_ref with
          | [] -> VBool false
          | _ ->
              let rec poll_for_response (lst : value list) : bool =
                match lst with
                | [] -> failwith "Polling for response on empty list"
                | hd :: tl -> (
                    match hd with
                    | VFuture fut -> (
                        match !fut with
                        | Some _ -> true
                        | None -> poll_for_response tl)
                    | VBool b -> (
                        match b with
                        | true -> true
                        | false -> poll_for_response tl)
                    | _ ->
                        failwith
                          "Polling for response that isn't a future or bool")
              in
              VBool (poll_for_response !list_ref))
      | VMap m ->
          let folded_map = ValueMap.fold (fun _ v acc -> v :: acc) m [] in
          let rec poll_for_response (lst : value list) : bool =
            match lst with
            | [] -> false
            | hd :: tl -> (
                match hd with
                | VFuture fut -> (
                    match !fut with
                    | Some _ -> true
                    | None -> poll_for_response tl)
                | VBool b -> (
                    match b with true -> true | false -> poll_for_response tl)
                | _ ->
                    failwith
                      "Polling for response that isn't a future or bool in map")
          in
          VBool (poll_for_response folded_map)
      | _ -> failwith "Polling for response on non-collection")
  | ENextResp e -> (
      match eval env e with
      | VMap m ->
          let folded_map = ValueMap.fold (fun k v acc -> (k, v) :: acc) m [] in
          let rec nxt_resp (lst : (value * value) list) : value =
            match lst with
            | [] -> failwith "No responses \nin map"
            | hd :: tl -> (
                let key, value = hd in
                match value with
                | VFuture fut -> (
                    match !fut with
                    | Some v ->
                        ValueMap.replace m key (VFuture (ref None));
                        v
                    | None -> nxt_resp tl)
                | _ -> failwith "ENextResp on non-future")
          in
          nxt_resp folded_map
      | _ -> failwith "ENextResp on non-collection")
  | EMin (e1, e2) -> (
      match (eval env e1, eval env e2) with
      | VInt i1, VInt i2 -> VInt (min i1 i2)
      | _ -> failwith "EMin eval fail")
  | EUnit -> VUnit
  | ENil -> VOption None (* nil literal evaluates to VOption None *)
  | ETuple exprs -> VTuple (Array.of_list (List.map (eval env) exprs))
  | ETupleAccess (e_tuple, idx) -> (
      match eval env e_tuple with
      | VTuple arr -> (
          try arr.(idx)
          with Invalid_argument _ ->
            failwith
              ("Runtime error: Tuple index " ^ string_of_int idx
             ^ " out of bounds"))
      | _ -> failwith "Type error: Attempted tuple access on non-tuple")
  | EUnwrap e -> (
      match eval env e with
      | VOption (Some v) -> v
      | VOption None -> failwith "Runtime error: Attempted to unwrap nil"
      | v ->
          Printf.printf "Unwrap on: %s\n" (to_string_value v);
          failwith "Type error: Attempted to unwrap non-optional")
  | ECoalesce (e_opt, e_default) -> (
      match eval env e_opt with
      | VOption (Some v) -> v
      | VOption None -> eval env e_default
      | _ -> failwith "Type error: Coalesce on non-optional")

let eval_lhs (env : record_env) (lhs : lhs) : lvalue =
  match lhs with
  | LVar var -> LVVar var
  | LAccess (collection, exp) -> (
      match eval env collection with
      | VMap map -> LVAccess (eval env exp, map)
      | VList l -> LVAccessList (eval env exp, l)
      | _ -> failwith "LAccess can't index into non-collection types")
  | LTuple strs -> LVTuple strs

let store (lhs : lhs) (vl : value) (env : record_env) : unit =
  match eval_lhs env lhs with
  | LVVar var ->
      if Env.mem env.local_env var then Env.replace env.local_env var vl
      else Env.replace env.node_env var vl
  | LVAccess (key, table) -> ValueMap.replace table key vl
  | LVAccessList (idx, ref_l) -> (
      match idx with
      | VInt i | VNode i ->
          if i < 0 || i >= List.length !ref_l then
            failwith "LVAccess idx out of range"
          else if List.length !ref_l == 0 then failwith "LVAccess empty list"
          else
            let lst = !ref_l in
            ref_l := List.mapi (fun j x -> if j = i then vl else x) lst
      | _ ->
          Printf.printf "failed to index into %s\n" (to_string_lhs lhs);
          failwith "Can't index into a list with non-integer")
  | LVTuple _ -> failwith "how to store LVTuples?"

exception Halt

let copy (lhs : lhs) (vl : value) (env : record_env) : unit =
  match eval_lhs env lhs with
  | LVVar var -> (
      match vl with
      | VMap m ->
          let temp = ValueMap.copy m in
          Env.replace env.local_env var (VMap temp)
      | VList l ->
          let temp = ref (List.map (fun x -> x) !l) in
          Env.replace env.local_env var (VList temp)
      | _ -> failwith "no copying non-collections")
  | _ -> failwith "copying only to local_copy"

let function_info name program =
  try Env.find program.rpc name
  with Not_found ->
    Printf.printf "function %s is not defined\n" name;
    failwith "Function not found"

(* Execute record until pause/return.  Invariant: record does *not* belong to
   state.records *)
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
                let new_future = ref None in

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
                   Printf.printf
                     "Func %s mismatches def and caller args\n\
                     \                    formals: %s\n\n\
                     \                               actuals: %s\n"
                     func
                     (String.concat ", " formals)
                     (String.concat ", \n" (List.map to_string_expr actuals));
                   failwith "Mismatched arguments in function call");
                List.iter
                  (fun (var_name, expr) ->
                    Env.add new_env var_name (eval env expr))
                  locals;
                let new_record =
                  {
                    node = node_id;
                    pc = entry;
                    continuation = (fun value -> new_future := Some value);
                    env = new_env;
                    id = record.id;
                    x = record.x;
                    f = record.f;
                  }
                in
                store lhs (VFuture new_future) env;
                state.records <- new_record :: state.records
            | VBool _ -> failwith "Type error bool"
            | VMap _ -> failwith "Type error map"
            | VList _ -> failwith "Type error list"
            | VOption _ -> failwith "Type error option"
            | VFuture _ -> failwith "Type error future"
            | VUnit -> failwith "Type error unit"
            | VTuple _ -> failwith "Type error tuple"
            | VString role -> (
                match load role env with
                | VNode node_id ->
                    let new_future = ref None in
                    let { entry; formals; _ } = function_info func program in

                    let new_env = Env.create 1024 in
                    List.iter2
                      (fun formal actual ->
                        Env.add new_env formal (eval env actual))
                      formals actuals;
                    let new_record =
                      {
                        node = node_id;
                        pc = entry;
                        continuation = (fun value -> new_future := Some value);
                        env = new_env;
                        id = record.id;
                        x = record.x;
                        f = record.f;
                      }
                    in

                    store lhs (VFuture new_future) env;
                    state.records <- new_record :: state.records
                | _ -> failwith "Type error idk what you are anymore"))
        | Assign (lhs, rhs) -> store lhs (eval env rhs) env
        | Copy (lhs, rhs) -> copy lhs (eval env rhs) env);
        loop ()
    | Cond (cond, bthen, belse) ->
        (match eval env cond with
        | VBool true -> record.pc <- bthen
        | VBool false -> record.pc <- belse
        | VInt _ -> failwith "Type error int"
        | VFuture _ -> failwith "Type error future"
        | VMap _ -> failwith "Type error map"
        | VList _ -> failwith "Type error list"
        | VOption _ -> failwith "Type error option"
        | VString _ -> failwith "Type error string"
        | VNode _ -> failwith "Type error node"
        | VUnit -> failwith "Type error unit"
        | VTuple _ -> failwith "Type error tuple");
        loop ()
    | Await (lhs, expr, next) -> (
        match eval env expr with
        | VFuture fut -> (
            match !fut with
            | Some value ->
                record.pc <- next;
                store lhs value env;
                loop ()
            | None ->
                (* Still waiting.  TODO: should keep blocked records in a
                 different data structure to avoid scheduling records that
                 can't 
do any work. *)
                state.records <- record :: state.records)
        | VBool b ->
            if b then (
              record.pc <- next;
              loop ())
            else state.records <- record :: state.records
        | VInt _ -> failwith "Type error int"
        | VMap _ -> failwith "Type error map"
        | VList _ -> failwith "Type error list"
        | VOption _ -> failwith "Type error option"
        | VString _ -> failwith "Type error string"
        | VNode _ -> failwith "Type error node"
        | VUnit -> failwith "Type \nerror unit"
        | VTuple _ -> failwith "Type error tuple")
    | Return expr -> record.continuation (eval env expr)
    | Pause next ->
        record.pc <- next;
        state.records <- record :: state.records
    | ForLoopIn (lhs, expr, body, next) -> (
        match eval env expr with
        | VMap map -> (
            if
              (* First remove the pair being processed from the map. *)
              ValueMap.length map == 0
            then (
              record.pc <- next;
              loop ())
            else
              let single_pair =
                let result_ref = ref None in

                ValueMap.iter
                  (fun key value ->
                    match !result_ref with
                    | Some _ -> () (* We already found a pair, so do nothing *)
                    | None -> result_ref := Some (key, value))
                  map;
                !result_ref
              in
              ValueMap.remove map (fst (Option.get single_pair));
              store (LVar "local_copy") (VMap map) env;

              match lhs with
              | LTuple [ key; value ] ->
                  let k, v = Option.get single_pair in
                  Env.add env.local_env key k;
                  Env.add env.local_env value v;
                  record.pc <- body;
                  loop ()
              | _ ->
                  Printf.printf "failed to iterate map with lhs: %s\n"
                    (to_string_lhs lhs);
                  failwith
                    "Cannot iterate through map with anything other than a \
                     2-tuple")
        | VList list_ref -> (
            if
              (* First remove the pair being processed from the map. *)
              List.length !list_ref == 0
            then (
              record.pc <- next;
              loop ())
            else
              let removed_item =
                let result_ref = ref None in
                List.iter
                  (fun item ->
                    match !result_ref with
                    | Some _ -> () (* We already found an item, so do nothing *)
                    | None -> result_ref := Some item)
                  !list_ref;

                !result_ref
              in
              list_ref :=
                List.filter (fun x -> x <> Option.get removed_item) !list_ref;
              store (LVar "local_copy") (VList list_ref) env;
              match lhs with
              | LVar var ->
                  Env.add env.local_env var (Option.get removed_item);
                  record.pc <- body;
                  loop ()
              | LAccess _ ->
                  failwith
                    "Cannot iterate through list with anything other than a \
                     variable LAccess"
              | LTuple _ ->
                  failwith
                    "Cannot iterate through list with anything other than a \
                     variable LTuple")
        | VInt _ -> failwith "Type error VInt"
        | VBool _ -> failwith "Type error VBool"
        | VString _ -> failwith "Type error VString"
        | VNode _ -> failwith "Type error VNode"
        | VOption _ -> failwith "Type error VOption"
        | VFuture _ -> failwith "Type error VFuture"
        | VUnit -> failwith "Type error VUnit"
        | VTuple _ -> failwith "Type error VTuple")
    | Print (expr, next) ->
        let v = eval env expr in
        Printf.printf "%s" (to_string_value v);
        record.pc <- next;
        loop ()
    | Break _ -> raise Halt
  in
  loop ()

let schedule_record (state : state) (program : program)
    (randomly_drop_msgs : bool) (cut_tail_from_mid : bool)
    (sever_all_but_mid : bool) (partition_away_nodes : int list)
    (randomly_delay_msgs : bool) : unit =
  if false then
    Printf.printf "%b %b %b %b\n" randomly_drop_msgs cut_tail_from_mid
      sever_all_but_mid
      (List.length partition_away_nodes = 0);
  let rec pick n before after =
    match after with
    | [] -> raise Halt
    | r :: rs ->
        if n == 0 then (
          let env = { local_env = r.env; node_env = state.nodes.(r.node) } in
          state.records <- List.rev_append before rs;
          match CFG.label program.cfg r.pc with
          | Instr (i, _) -> (
              match i with
              | Async (_, node, _, _) ->
                  let node_id =
                    match eval env node with
                    | VNode n_id | VInt n_id -> n_id
                    | VBool _ -> failwith "Type error VBool!"
                    | VString _ -> failwith "Type error VString!"
                    | VMap _ -> failwith "Type error VMap!"
                    | VList _ -> failwith "Type error VList!"
                    | VOption _ -> failwith "Type error VOption!"
                    | VFuture _ -> failwith "Type error VFuture!"
                    | VUnit -> failwith "Type error VUnit!"
                    | VTuple _ -> failwith "Type error VTuple!"
                  in
                  let src_node = r.node in
                  let dest_node = node_id in
                  let should_execute = ref true in

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
                      should_execute := false);
                  if !should_execute then exec state program r
              | _ -> exec state program r)
          | _ -> exec state program r)
        else pick (n - 1) (r :: before) rs
  in
  (* let idx = 0 in *)
  let idx =
    Random.self_init ();
    Random.int (List.length state.records)
  in
  (* let chosen_record = List.nth state.records idx in *)
  (* let vert = chosen_record.pc in  *)
  let chosen_idx =
    idx
    (* let chosen_idx =  *)
    (* match (CFG.label program.cfg vert) with *)
    (* | Pause _ *)
    (* | Await (_, _, _) -> if (List.length state.records) == 1 then idx else 1 *)
    (* | _ -> idx *)
  in
  pick chosen_idx [] state.records

(* Choose a client without a 
pending operation, create a new activation record
   to execute it, and append the invocation to the history *)
let schedule_client (state : state) (program : program) (func_name : string)
    (actuals : value list) (unique_id : int) : unit =
  let rec pick n before after =
    match after with
    | [] -> raise Halt
    | c :: cs ->
        if n == 0 then (
          let op = Env.find program.client_ops func_name in
          let env = Env.create 1024 in
          List.iter2
            (fun formal actual -> Env.add env formal actual)
            op.formals actuals;

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
             allow client to be scheduled again. *)
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
            state.free_clients <- c :: state.free_clients
          in
          let record =
            {
              pc = op.entry;
              node = c;
              continuation;
              env;
              id = unique_id;
              x = 0.4;
              f = (fun x -> x /. 2.0);
            }
          in
          state.free_clients <- List.rev_append before cs;
          DA.add state.history invocation;
          state.records <- record :: state.records)
        else pick (n - 1) (c :: before) cs
  in
  pick
    (Random.self_init ();
     Random.int (List.length state.free_clients))
    [] state.free_clients

let schedule_sys_thread (state : state) (program : program) (func_name : string)
    (actuals : value list) (unique_id : int) : unit =
  let rec pick n before after =
    match after with
    | [] -> raise Halt
    | c :: cs ->
        if n == 0 then (
          let op = Env.find program.client_ops func_name in
          let env = Env.create 1024 in
          List.iter2
            (fun formal actual -> Env.add env formal actual)
            op.formals actuals;

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
             allow client to be scheduled again. *)
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
            state.free_sys_threads <- c :: state.free_sys_threads
          in
          let record =
            {
              pc = op.entry;
              node = c;
              continuation;
              env;
              id = unique_id;
              x = 0.4;
              f = (fun x -> x /. 2.0);
            }
          in
          state.free_sys_threads <- List.rev_append before cs;
          DA.add state.history invocation;
          state.records <- record :: state.records)
        else pick (n - 1) (c :: before) cs
  in
  pick
    (Random.self_init ();
     Random.int (List.length state.free_sys_threads))
    [] state.free_sys_threads
