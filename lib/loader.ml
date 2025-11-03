open Simulator
open Yojson.Safe.Util

(*
  Helper to parse JSON of the form: {"VariantName": ...payload...}
  This is the default format serde_json uses for enums.
*)
let get_variant json =
  match to_assoc json with
  | [ (key, value) ] -> (key, value)
  | _ -> failwith "Expected JSON object with a single key for variant"

(* Recursively parses a JSON object into an `expr` *)
let rec expr_of_yojson json : expr =
  match json with
  (* Handle Serde's default for unit variants *)
  | `String "EUnit" -> EUnit
  | `String "ENil" -> ENil
  | `String "ECreatePromise" -> ECreatePromise
  | `String "ECreateLock" -> ECreateLock
  (* Handle adjacently tagged variants*)
  | `Assoc [ (key, value) ] -> (
      match key with
      | "EVar" -> EVar (to_string value)
      | "EFind" -> (
          match to_list value with
          | [ e1; e2 ] -> EFind (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EFind expects 2 args")
      | "EInt" -> EInt (to_int value)
      | "EBool" -> EBool (to_bool value)
      | "ENot" -> ENot (expr_of_yojson value)
      | "EAnd" -> (
          match to_list value with
          | [ e1; e2 ] -> EAnd (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EAnd expects 2 args")
      | "EOr" -> (
          match to_list value with
          | [ e1; e2 ] -> EOr (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EOr expects 2 args")
      | "EEqualsEquals" -> (
          match to_list value with
          | [ e1; e2 ] -> EEqualsEquals (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EEqualsEquals expects 2 args")
      | "EMap" ->
          EMap
            (List.map
               (fun pair ->
                 match to_list pair with
                 | [ k; v ] -> (expr_of_yojson k, expr_of_yojson v)
                 | _ -> failwith "EMap pair expects 2 items")
               (to_list value))
      | "EList" -> EList (List.map expr_of_yojson (to_list value))
      | "EListPrepend" -> (
          match to_list value with
          | [ e1; e2 ] -> EListPrepend (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EListPrepend expects 2 args")
      | "EListAppend" -> (
          match to_list value with
          | [ e1; e2 ] -> EListAppend (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EListAppend expects 2 args")
      | "EListSubsequence" -> (
          match to_list value with
          | [ e1; e2; e3 ] ->
              EListSubsequence
                (expr_of_yojson e1, expr_of_yojson e2, expr_of_yojson e3)
          | _ -> failwith "EListSubsequence expects 3 args")
      | "EString" -> EString (to_string value)
      | "ELessThan" -> (
          match to_list value with
          | [ e1; e2 ] -> ELessThan (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "ELessThan expects 2 args")
      | "ELessThanEquals" -> (
          match to_list value with
          | [ e1; e2 ] -> ELessThanEquals (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "ELessThanEquals expects 2 args")
      | "EGreaterThan" -> (
          match to_list value with
          | [ e1; e2 ] -> EGreaterThan (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EGreaterThan expects 2 args")
      | "EGreaterThanEquals" -> (
          match to_list value with
          | [ e1; e2 ] ->
              EGreaterThanEquals (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EGreaterThanEquals expects 2 args")
      | "EKeyExists" -> (
          match to_list value with
          | [ e1; e2 ] -> EKeyExists (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EKeyExists expects 2 args")
      | "EMapErase" -> (
          match to_list value with
          | [ e1; e2 ] -> EMapErase (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EKeyExists expects 2 args")
      | "EListLen" -> EListLen (expr_of_yojson value)
      | "EListAccess" -> (
          match to_list value with
          | [ e; i ] -> EListAccess (expr_of_yojson e, to_int i)
          | _ -> failwith "EListAccess expects 2 args")
      | "EPlus" -> (
          match to_list value with
          | [ e1; e2 ] -> EPlus (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EPlus expects 2 args")
      | "EMinus" -> (
          match to_list value with
          | [ e1; e2 ] -> EMinus (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EMinus expects 2 args")
      | "ETimes" -> (
          match to_list value with
          | [ e1; e2 ] -> ETimes (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "ETimes expects 2 args")
      | "EDiv" -> (
          match to_list value with
          | [ e1; e2 ] -> EDiv (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EDiv expects 2 args")
      | "EMod" -> (
          match to_list value with
          | [ e1; e2 ] -> EMod (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EMod expects 2 args")
      | "EPollForResps" -> (
          match to_list value with
          | [ e1; e2 ] -> EPollForResps (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EPollForResps expects 2 args")
      | "EPollForAnyResp" -> EPollForAnyResp (expr_of_yojson value)
      | "ENextResp" -> ENextResp (expr_of_yojson value)
      | "EMin" -> (
          match to_list value with
          | [ e1; e2 ] -> EMin (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "EMin expects 2 args")
      | "ETuple" -> ETuple (List.map expr_of_yojson (to_list value))
      | "ETupleAccess" -> (
          match to_list value with
          | [ e; i ] -> ETupleAccess (expr_of_yojson e, to_int i)
          | _ -> failwith "ETupleAccess expects 2 args")
      (* These two are now technically redundant, but harmless to keep.
       They would handle {"EUnit": null} if you ever generated that. *)
      | "EUnit" -> EUnit
      | "ENil" -> ENil
      | "EUnwrap" -> EUnwrap (expr_of_yojson value)
      | "ECoalesce" -> (
          match to_list value with
          | [ e1; e2 ] -> ECoalesce (expr_of_yojson e1, expr_of_yojson e2)
          | _ -> failwith "ECoalesce expects 2 args")
      | "ESome" -> ESome (expr_of_yojson value)
      | _ -> failwith ("Unknown expr key: " ^ key))
  (* Handle malformed JSON *)
  | _ ->
      failwith
        ("Expected (string) or (object with one key) for expr. Got: "
       ^ Yojson.Safe.to_string json)

let lhs_of_yojson json : lhs =
  let key, value = get_variant json in
  match key with
  | "Var" -> LVar (to_string value)
  | "Access" -> (
      match to_list value with
      | [ e1; e2 ] -> LAccess (expr_of_yojson e1, expr_of_yojson e2)
      | _ -> failwith "Lhs::Access expects 2 args")
  | "Tuple" -> LTuple (List.map to_string (to_list value))
  | _ -> failwith ("Unknown lhs key: " ^ key)

let instr_of_yojson json : instr =
  let key, value = get_variant json in
  match key with
  | "Assign" -> (
      match to_list value with
      | [ l; r ] -> Assign (lhs_of_yojson l, expr_of_yojson r)
      | _ -> failwith "Instr::Assign expects 2 args")
  | "Async" -> (
      match to_list value with
      | [ l; n; f; args ] ->
          Async
            ( lhs_of_yojson l,
              expr_of_yojson n,
              to_string f,
              List.map expr_of_yojson (to_list args) )
      | _ -> failwith "Instr::Async expects 4 args")
  | "Copy" -> (
      match to_list value with
      | [ l; r ] -> Copy (lhs_of_yojson l, expr_of_yojson r)
      | _ -> failwith "Instr::Copy expects 2 args")
  | "Resolve" -> (
      match to_list value with
      | [ l; r ] -> Resolve (lhs_of_yojson l, expr_of_yojson r)
      | _ -> failwith "Instr::Resolve expects 2 args")
  | _ -> failwith ("Unknown instr key: " ^ key)

let label_of_yojson json : CFG.vertex label =
  let key, value = get_variant json in
  match key with
  | "Instr" -> (
      match to_list value with
      | [ i; v ] -> Instr (instr_of_yojson i, to_int v)
      | _ -> failwith "Label::Instr expects 2 args")
  | "Pause" -> Pause (to_int value)
  | "Await" -> (
      match to_list value with
      | [ l; e; v ] -> Await (lhs_of_yojson l, expr_of_yojson e, to_int v)
      | _ -> failwith "Label::Await expects 3 args")
  | "SpinAwait" -> (
      match to_list value with
      | [ e; v ] -> SpinAwait (expr_of_yojson e, to_int v)
      | _ -> failwith "Label::SpinAwait expects 2 args")
  | "Return" -> Return (expr_of_yojson value)
  | "Cond" -> (
      match to_list value with
      | [ e; v1; v2 ] -> Cond (expr_of_yojson e, to_int v1, to_int v2)
      | _ -> failwith "Label::Cond expects 3 args")
  | "ForLoopIn" -> (
      match to_list value with
      | [ l; e; vb; vn ] ->
          ForLoopIn (lhs_of_yojson l, expr_of_yojson e, to_int vb, to_int vn)
      | _ -> failwith "Label::ForLoopIn expects 4 args")
  | "Print" -> (
      match to_list value with
      | [ e; v ] -> Print (expr_of_yojson e, to_int v)
      | _ -> failwith "Label::Print expects 2 args")
  | "Break" -> Break (to_int value)
  | "Lock" -> (
      match to_list value with
      | [ e; v ] -> Lock (expr_of_yojson e, to_int v)
      | _ -> failwith "Label::Lock expects 2 args")
  | "Unlock" -> (
      match to_list value with
      | [ e; v ] -> Unlock (expr_of_yojson e, to_int v)
      | _ -> failwith "Label::Unlock expects 2 args")
  | _ -> failwith ("Unknown label key: " ^ key)

let function_info_of_yojson json : function_info =
  {
    entry = member "entry" json |> to_int;
    name = member "name" json |> to_string;
    formals = member "formals" json |> to_list |> List.map to_string;
    locals =
      member "locals" json |> to_list
      |> List.map (fun pair ->
             match to_list pair with
             | [ name_json; expr_json ] ->
                 (to_string name_json, expr_of_yojson expr_json)
             | _ -> failwith "locals entry must be a [name, expr] pair");
  }

let program_of_yojson json : program =
  (* 1. Parse the CFG (a list of labels) into a BatDynArray *)
  let cfg_list = member "cfg" json |> to_list |> List.map label_of_yojson in
  let cfg_array = BatDynArray.of_list cfg_list in

  (* 2. Parse the RPC map (a JSON object) into an OCaml Hashtbl *)
  let rpc_hashtbl = Env.create (List.length (to_assoc (member "rpc" json))) in
  List.iter
    (fun (key, info_json) ->
      Env.add rpc_hashtbl key (function_info_of_yojson info_json))
    (to_assoc (member "rpc" json));

  (* 3. Parse the client_ops map (a JSON object) into an OCaml Hashtbl *)
  let client_ops_hashtbl =
    Env.create (List.length (to_assoc (member "client_ops" json)))
  in
  List.iter
    (fun (key, info_json) ->
      Env.add client_ops_hashtbl key (function_info_of_yojson info_json))
    (to_assoc (member "client_ops" json));

  { cfg = cfg_array; rpc = rpc_hashtbl; client_ops = client_ops_hashtbl }

(**
 * Loads a compiled program from a JSON file.
 *
 * @param file_path The path to the .json file.
 * @return The parsed `program` record.
 *)
let load_program_from_file (file_path : string) : program =
  let json = Yojson.Safe.from_file file_path in
  program_of_yojson json
