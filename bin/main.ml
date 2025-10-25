open Mylib.Ast
open Mylib
open Mylib.Simulator
open Yojson.Basic.Util

type config = {
  randomly_drop_msgs : bool;
  cut_tail_from_mid : bool;
  sever_all_to_tail_but_mid : bool;
  partition_away_nodes : int list;
  randomly_delay_msgs : bool;
}

let read_config_file (filename : string) : config =
  let json = Yojson.Basic.from_file filename in
  {
    randomly_drop_msgs = json |> member "randomly_drop_msgs" |> to_bool;
    cut_tail_from_mid = json |> member "cut_tail_from_mid" |> to_bool;
    sever_all_to_tail_but_mid =
      json |> member "sever_all_to_tail_but_mid" |> to_bool;
    partition_away_nodes =
      json |> member "partition_away_nodes" |> to_list |> filter_int;
    randomly_delay_msgs = json |> member "randomly_delay_msgs" |> to_bool;
  }

(*Parametrize first*)
(* TOPOLOGIES = ["LINEAR"; "STAR"; "RING"; "FULL"] *)

let num_servers = 3
let num_clients = 3
let num_sys_threads = num_servers * 3
let chain_len = 3
let head_idx = 0
let tail_idx = chain_len - 1
let topology = "FULL"

let data () : value ValueMap.t =
  let tbl = ValueMap.create 91 in
  ValueMap.add tbl (VString "birthday") (VInt 214);
  ValueMap.add tbl (VString "epoch") (VInt 1980);
  ValueMap.add tbl (VString "name") (VString "Jennifer");
  tbl

let mod_op (i : int) (m : int) : int = if i < 0 then i + m else i mod m

let global_state =
  {
    nodes =
      Array.init
        (num_servers + num_clients + num_sys_threads)
        (fun _ -> Env.create 91);
    records = [];
    history = DA.create ();
    free_clients = List.init num_clients (fun i -> num_servers + i);
    free_sys_threads =
      List.init num_sys_threads (fun i -> num_servers + num_clients + i);
  }

let rec convert_rhs (rhs : rhs) : Simulator.expr =
  match rhs with
  | VarRHS var -> EVar var
  | CollectionAccessRHS collection_access -> (
      match collection_access with
      | CollectionAccess (collection, key) ->
          EFind (convert_rhs collection, convert_rhs key))
  | FuncCallRHS func_call -> (
      match func_call with
      | FuncCall _ -> failwith "Already implemented FuncCallRHS in top level")
  | LiteralRHS literal -> (
      match literal with
      | Options _ -> failwith "Didn't implement Options yet"
      | String s -> EString s
      | Int i -> EInt i)
  | FieldAccessRHS _ -> failwith "Didn't implement FieldAccessRHS yet"
  | Bool b -> EBool b
  | Not rhs -> ENot (convert_rhs rhs)
  | Or (rhs1, rhs2) -> EOr (convert_rhs rhs1, convert_rhs rhs2)
  | And (b1, b2) -> EAnd (convert_rhs b1, convert_rhs b2)
  | EqualsEquals (rhs1, rhs2) ->
      EEqualsEquals (convert_rhs rhs1, convert_rhs rhs2)
  | NotEquals (b1, b2) -> ENot (EEqualsEquals (convert_rhs b1, convert_rhs b2))
  | LessThan (rhs1, rhs2) -> ELessThan (convert_rhs rhs1, convert_rhs rhs2)
  | LessThanEquals (rhs1, rhs2) ->
      ELessThanEquals (convert_rhs rhs1, convert_rhs rhs2)
  | GreaterThan (rhs1, rhs2) -> EGreaterThan (convert_rhs rhs1, convert_rhs rhs2)
  | GreaterThanEquals (rhs1, rhs2) ->
      EGreaterThanEquals (convert_rhs rhs1, convert_rhs rhs2)
  | KeyExists (key, mp) -> EKeyExists (convert_rhs key, convert_rhs mp)
  | CollectionRHS collection_lit -> (
      match collection_lit with
      | MapLit kvpairs ->
          EMap (List.map (fun (k, v) -> (convert_rhs k, convert_rhs v)) kvpairs)
      | ListLit items -> EList (List.map (fun v -> convert_rhs v) items)
      | ListPrepend (rhs, ls) -> EListPrepend (convert_rhs rhs, convert_rhs ls)
      | ListAppend (ls, rhs) -> EListAppend (convert_rhs ls, convert_rhs rhs)
      | ListSubsequence (ls, start_idx, end_idx) ->
          EListSubsequence
            (convert_rhs ls, convert_rhs start_idx, convert_rhs end_idx))
  | RpcCallRHS _ -> failwith "Already implemented RpcCallRHS in top level"
  | Head ls -> EListAccess (convert_rhs ls, 0)
  | Tail _ -> failwith "Didn't implement Tail yet"
  | Len ls -> EListLen (convert_rhs ls)
  | ListAccess (ls, idx) -> EListAccess (convert_rhs ls, idx)
  | Plus (rhs1, rhs2) -> EPlus (convert_rhs rhs1, convert_rhs rhs2)
  | Minus (rhs1, rhs2) -> EMinus (convert_rhs rhs1, convert_rhs rhs2)
  | Times (rhs1, rhs2) -> ETimes (convert_rhs rhs1, convert_rhs rhs2)
  | Div (rhs1, rhs2) -> EDiv (convert_rhs rhs1, convert_rhs rhs2)
  | Mod (rhs1, rhs2) -> EMod (convert_rhs rhs1, convert_rhs rhs2)
  | PollForResps (collection, rhs2) ->
      EPollForResps (convert_rhs collection, convert_rhs rhs2)
  | PollForAnyResp collection -> EPollForAnyResp (convert_rhs collection)
  | NextResp collection -> ENextResp (convert_rhs collection)
  | Min (first, second) -> EMin (convert_rhs first, convert_rhs second)
  | SetTimeout -> failwith "Already implemented SetTimeout in top level"

let convert_lhs (lhs : Ast.lhs) : Simulator.lhs =
  match lhs with
  | VarLHS var_name -> LVar var_name
  | CollectionAccessLHS collection_access -> (
      match collection_access with
      | CollectionAccess (collection, key) ->
          LAccess (convert_rhs collection, convert_rhs key))
  | FieldAccessLHS (_, _) ->
      failwith "TODO what on earth is FieldAccessLHS again?"
  | TupleLHS lefts -> LTuple lefts

let rec generate_cfg_from_stmts (stmts : statement list) (cfg : CFG.t)
    (last_vert : CFG.vertex) : CFG.vertex =
  match stmts with
  | [] -> last_vert
  | stmt :: rest -> (
      let next_vert = generate_cfg_from_stmts rest cfg last_vert in
      match stmt with
      | CondList cond_stmts ->
          let vert = generate_cfg_from_cond_stmts cond_stmts cfg next_vert in
          vert
      | VarDeclInit (var_name, rhs) -> (
          match rhs with
          | FuncCallRHS _ ->
              (* Handle function calls by first executing the call (result goes to "ret"),
                 then assigning "ret" to the variable *)
              let assign_vert =
                CFG.create_vertex cfg
                  (Instr (Assign (LVar var_name, EVar "ret"), next_vert))
              in
              generate_cfg_from_stmts [ Expr rhs ] cfg assign_vert
          | _ ->
              let value = convert_rhs rhs in
              let vert =
                CFG.create_vertex cfg
                  (Instr (Assign (LVar var_name, value), next_vert))
              in
              vert)
      | AssignmentStmt a ->
          let (Assignment (lhs, exp)) = a in
          let lhs_vert =
            CFG.create_vertex cfg
              (Instr (Assign (convert_lhs lhs, EVar "ret"), next_vert))
          in
          let vert = generate_cfg_from_stmts [ Expr exp ] cfg lhs_vert in
          vert
      | Expr expr -> (
          match expr with
          | RpcCallRHS rpc_call -> (
              match rpc_call with
              | RpcCall (node, func_call) -> (
                  match func_call with
                  | FuncCall (func_name, actuals) ->
                      let actuals =
                        List.map
                          (fun actual ->
                            match actual with Param rhs -> convert_rhs rhs)
                          actuals
                      in
                      let assign_vert =
                        CFG.create_vertex cfg
                          (Instr
                             (Assign (LVar "ret", EVar "dontcare"), next_vert))
                      in
                      let await_vertex =
                        CFG.create_vertex cfg
                          (Await
                             (LVar "dontcare", EVar "async_future", assign_vert))
                      in
                      let async_vertex =
                        CFG.create_vertex cfg
                          (Instr
                             ( Async
                                 ( LVar "async_future",
                                   convert_rhs node,
                                   func_name,
                                   actuals ),
                               await_vertex ))
                      in
                      CFG.create_vertex cfg (Pause async_vertex))
              | RpcAsyncCall (node, func_call) -> (
                  match func_call with
                  | FuncCall (func_name, actuals) ->
                      let actuals =
                        List.map
                          (fun actual ->
                            match actual with Param rhs -> convert_rhs rhs)
                          actuals
                      in
                      let async_vertex =
                        CFG.create_vertex cfg
                          (Instr
                             ( Async
                                 ( LVar "ret",
                                   convert_rhs node,
                                   func_name,
                                   actuals ),
                               next_vert ))
                      in
                      CFG.create_vertex cfg (Pause async_vertex)))
          | FuncCallRHS func_call -> (
              match func_call with
              | FuncCall (func_name, actuals) ->
                  let actuals =
                    List.map
                      (fun actual ->
                        match actual with Param rhs -> convert_rhs rhs)
                      actuals
                  in
                  let assign_vert =
                    CFG.create_vertex cfg
                      (Instr (Assign (LVar "ret", EVar "dontcare"), next_vert))
                  in
                  let await_vertex =
                    CFG.create_vertex cfg
                      (Await (LVar "dontcare", EVar "async_future", assign_vert))
                  in
                  let async_vertex =
                    CFG.create_vertex cfg
                      (Instr
                         ( Async
                             ( LVar "async_future",
                               EVar "self",
                               func_name,
                               actuals ),
                           await_vertex ))
                  in
                  CFG.create_vertex cfg (Pause async_vertex))
          | SetTimeout ->
              CFG.create_vertex cfg
                (Instr
                   (Async (LVar "ret", EVar "self", "TIMEOUT", []), next_vert))
          | rhs ->
              CFG.create_vertex cfg
                (Instr (Assign (LVar "ret", convert_rhs rhs), next_vert)))
      | Return exp ->
          let ret_vert = CFG.create_vertex cfg (Return (EVar "ret")) in
          generate_cfg_from_stmts [ Expr exp ] cfg ret_vert
      | ForLoop (init_stmt, cond, update, body) ->
          let cond_vert = CFG.fresh_vertex cfg in
          let update_vert =
            generate_cfg_from_stmts [ AssignmentStmt update ] cfg cond_vert
          in
          let body_vert = generate_cfg_from_stmts body cfg update_vert in
          CFG.set_label cfg cond_vert
            (Cond (convert_rhs cond, body_vert, next_vert));
          let init_vert =
            generate_cfg_from_stmts [ AssignmentStmt init_stmt ] cfg cond_vert
          in
          init_vert
      | ForLoopIn (idx, collection, body) ->
          let for_vert = CFG.fresh_vertex cfg in
          (* let ret_vert = CFG.create_vertex cfg (Return(EBool true)) in *)
          let body_vert = generate_cfg_from_stmts body cfg for_vert in
          CFG.set_label cfg for_vert
            (ForLoopIn (convert_lhs idx, EVar "local_copy", body_vert, next_vert));
          let local_copy_vert =
            CFG.create_vertex cfg
              (Instr (Copy (LVar "local_copy", convert_rhs collection), for_vert))
          in
          local_copy_vert
      | Comment -> generate_cfg_from_stmts rest cfg last_vert
      | Await exp ->
          CFG.create_vertex cfg (Await (LVar "ret", convert_rhs exp, next_vert))
      | Print exp -> CFG.create_vertex cfg (Print (convert_rhs exp, next_vert))
      | Match (exp, cases) ->
          let vert, _ = generate_cfg_from_match_stmt exp cases cfg last_vert in
          vert
      | BreakStmt -> CFG.create_vertex cfg (Break last_vert))

and generate_cfg_from_cond_stmts (cond_stmts : cond_stmt list) (cfg : CFG.t)
    (next : CFG.vertex) : CFG.vertex =
  match cond_stmts with
  | [] -> next
  | cond_stmt :: rest -> (
      let elseif_vert = generate_cfg_from_cond_stmts rest cfg next in
      match cond_stmt with
      | IfElseIf (cond, stmts) ->
          let body_vert = generate_cfg_from_stmts stmts cfg next in
          CFG.create_vertex cfg
            (Cond (convert_rhs cond, body_vert, elseif_vert)))

and generate_cfg_from_match_stmt (match_exp : rhs) (cases : case_stmt list)
    (cfg : CFG.t) (last : CFG.vertex) : CFG.vertex * CFG.vertex =
  match cases with
  | [] -> failwith "No cases in match statement"
  | DefaultStmt stmts :: _ ->
      let body_vert = generate_cfg_from_stmts stmts cfg last in
      (body_vert, body_vert)
  | CaseStmt (case_cond, stmts) :: rest ->
      let next_case_vert, next_body =
        generate_cfg_from_match_stmt match_exp rest cfg last
      in
      let body_vert =
        if List.length stmts = 0 then next_body
        else generate_cfg_from_stmts stmts cfg last
      in
      let case_vert =
        CFG.create_vertex cfg
          (Cond
             ( EEqualsEquals (convert_rhs match_exp, convert_rhs case_cond),
               body_vert,
               next_case_vert ))
      in
      (case_vert, body_vert)

let rec scan_ast_for_local_vars (stmts : statement list) : (string * expr) list
    =
  match stmts with
  | [] -> []
  | stmt :: rest -> (
      let rest_of_local_vars = scan_ast_for_local_vars rest in
      match stmt with
      | CondList cond_stmts ->
          let rec scan_cond_ast_for_local_vars (cond_stmts : cond_stmt list) :
              (string * expr) list =
            match cond_stmts with
            | [] -> []
            | IfElseIf (_, body) :: remaining_ifelses ->
                let local_vars = scan_ast_for_local_vars body in
                let remaining_local_vars =
                  scan_cond_ast_for_local_vars remaining_ifelses
                in
                local_vars @ remaining_local_vars
          in
          scan_cond_ast_for_local_vars cond_stmts @ rest_of_local_vars
      | VarDeclInit (var_name, rhs) ->
          let init_value =
            match rhs with
            | FuncCallRHS _ ->
                (* Placeholder - actual value will be set by CFG execution.
                   Using EInt 0 instead of evaluating in caller's environment. *)
                EInt 0
            | _ -> convert_rhs rhs
          in
          (var_name, init_value) :: rest_of_local_vars
      | ForLoop (init, _, _, body) ->
          let init_var =
            match init with
            | Assignment (lhs, rhs) -> (
                match lhs with
                | VarLHS var_name -> [ (var_name, convert_rhs rhs) ]
                | _ -> failwith "For loop init must be a variable assignment")
          and body_vars = scan_ast_for_local_vars body in
          init_var @ body_vars @ rest_of_local_vars
      | ForLoopIn (lhs, _, body) ->
          let init_vars =
            match lhs with
            | TupleLHS vars -> List.map (fun var -> (var, EInt 214)) vars
            | VarLHS var -> [ (var, EInt 214) ]
            | _ -> failwith "For loop in must be a variable assignment"
          and body_vars = scan_ast_for_local_vars body in
          init_vars @ body_vars @ rest_of_local_vars
      | Match (_, case_stmts) ->
          let rec scan_case_stmts_for_local_vars (case_stmts : case_stmt list) :
              (string * expr) list =
            match case_stmts with
            | [] -> []
            | DefaultStmt stmts :: _ -> scan_ast_for_local_vars stmts
            | CaseStmt (_, stmts) :: remaining_case_stmts ->
                scan_ast_for_local_vars stmts
                @ scan_case_stmts_for_local_vars remaining_case_stmts
          in
          scan_case_stmts_for_local_vars case_stmts @ rest_of_local_vars
      | AssignmentStmt _ | Expr _ | Comment | Await _ | Print _ | BreakStmt
      | Return _ ->
          rest_of_local_vars)

let process_func_def (func_def : func_def) (cfg : CFG.t) : function_info =
  match func_def with
  | FuncDef (func_call, _, body) -> (
      let last_vertex = CFG.create_vertex cfg (Return (EBool true)) in
      let entry = generate_cfg_from_stmts body cfg last_vertex in
      match func_call with
      | FuncCall (func_name, params) ->
          let formals =
            List.map
              (fun param ->
                match param with
                | Param rhs -> (
                    match rhs with
                    | VarRHS formal -> formal
                    | _ -> failwith "what param is this?"))
              params
          and locals = scan_ast_for_local_vars body in
          { entry; name = func_name; formals; locals })

let rec process_inits (inits : var_init list) (cfg : CFG.t) : CFG.vertex =
  match inits with
  | [] -> CFG.create_vertex cfg (Return (EBool true))
  | init :: rest -> (
      let next_vert = process_inits rest cfg in
      match init with
      | VarInit (_, var_name, rhs) ->
          CFG.create_vertex cfg
            (Instr (Assign (LVar var_name, convert_rhs rhs), next_vert)))
(* 
let timeout_func (cfg : CFG.t) : CFG.vertex = 
  CFG.create_vertex cfg (Return(EBool true)) *)

let process_role (role_def : role_def) (cfg : CFG.t) : function_info list =
  match role_def with
  | RoleDef (_, _, inits, func_defs) ->
      let init_vert = process_inits inits cfg
      and init_func_name = "BASE_NODE_INIT" in
      let init_func_info =
        { entry = init_vert; name = init_func_name; formals = []; locals = [] }
      in
      (*let timeout_vert = timeout_func cfg in
      let timeout_func_name = "TIMEOUT" in
      let timeout_info = 
      { entry = timeout_vert
      ; name = timeout_func_name
      ; formals = []
      ; locals = []
      } in*)
      let func_infos =
        List.map (fun func_def -> process_func_def func_def cfg) func_defs
      in
      (*timeout_info::*) init_func_info :: func_infos

let process_client_intf (client_intf : client_def) (cfg : CFG.t) :
    function_info list =
  match client_intf with
  | ClientDef (var_inits, func_defs) ->
      let init_vert = process_inits var_inits cfg
      and init_func_name = "BASE_CLIENT_INIT" in
      let init_func_info =
        { entry = init_vert; name = init_func_name; formals = []; locals = [] }
      in
      let func_infos =
        List.map (fun func_def -> process_func_def func_def cfg) func_defs
      in
      init_func_info :: func_infos

let process_program (prog : prog) : program =
  match prog with
  | Prog (single_role, clientIntf) ->
      let cfg = CFG.empty ()
      and rpc_calls = Env.create 91
      and client_calls = Env.create 91 in
      let func_infos = process_role single_role cfg
      and client_func_infos = process_client_intf clientIntf cfg in
      List.iter
        (fun func_info -> Env.add rpc_calls func_info.name func_info)
        func_infos;
      List.iter
        (fun func_info -> Env.add client_calls func_info.name func_info)
        client_func_infos;
      { cfg; rpc = rpc_calls; client_ops = client_calls }

let sync_exec (global_state : state) (prog : program)
    (randomly_drop_msgs : bool) (cut_tail_from_mid : bool)
    (sever_all_to_tail_but_mid : bool) (partition_away_nodes : int list)
    (randomly_delay_msgs : bool) : unit =
  while not (List.length global_state.records = 0) do
    schedule_record global_state prog randomly_drop_msgs cut_tail_from_mid
      sever_all_to_tail_but_mid partition_away_nodes randomly_delay_msgs
  done

let bootlegged_sync_exec (global_state : state) (prog : program)
    (randomly_drop_msgs : bool) (cut_tail_from_mid : bool)
    (sever_all_to_tail_but_mid : bool) (partition_away_nodes : int list)
    (randomly_delay_msgs : bool) : unit =
  let count = ref 0 in
  for _ = 0 to 100000 do
    if not (List.length global_state.records = 0) then (
      count := !count + 1;
      schedule_record global_state prog randomly_drop_msgs cut_tail_from_mid
        sever_all_to_tail_but_mid partition_away_nodes randomly_delay_msgs)
  done;
  Printf.printf "Executed %d record scheduling iterations\n" !count;
  Printf.printf "Remaining records: %d\n" (List.length global_state.records)

let parse_file (filename : string) : prog =
  let ic = open_in filename in
  let lexbuf = Lexing.from_channel ic in
  let ast = Parser.program Lexer.token lexbuf in
  close_in ic;
  ast

let print_single_node (node : value Env.t) =
  Env.iter
    (fun key value -> Printf.printf "%s: %s\n" key (to_string_value value))
    node

let print_global_nodes (nodes : value Env.t array) =
  Array.iter
    (fun node ->
      print_endline "Node has:";
      print_single_node node;
      print_endline "")
    nodes

let init_topology (topology : string) (global_state : state) (prog : program) :
    unit =
  match topology with
  | "LINEAR" ->
      for i = 0 to num_servers - 1 do
        schedule_client global_state prog "init"
          [
            VNode i (* dest *);
            VString "Mid" (* name *);
            VNode (mod_op (i - 1) num_servers) (* pred *);
            VNode (mod_op (i - 2) num_servers) (* pred_pred *);
            VNode (mod_op (i + 1) num_servers) (* succ *);
            VNode (mod_op (i + 2) num_servers) (* succ_succ *);
            VNode head_idx;
            VNode tail_idx;
            VNode i;
            VMap (data ());
          ]
          0;
        sync_exec global_state prog false false false [] false;
        (* Hashtbl.iter (fun _ _ -> print_endline "+1") data; *)
        print_endline "init mid"
      done;
      schedule_client global_state prog "init"
        [
          VNode head_idx (* dest *);
          VString "Head" (* name *);
          VNode (mod_op (head_idx - 1) num_servers) (* pred *);
          VNode (mod_op (head_idx - 2) num_servers) (* pred_pred *);
          VNode (mod_op (head_idx + 1) num_servers) (* succ *);
          VNode (mod_op (head_idx + 2) num_servers) (* succ_succ *);
          VNode head_idx (* head *);
          VNode tail_idx (* tail *);
          VNode head_idx;
          VMap (data ());
        ]
        0;
      print_endline "init head";
      (* Hashtbl.iter (fun _ _ -> print_endline "+1") data; *)
      sync_exec global_state prog false false false [] false;
      schedule_client global_state prog "init"
        [
          VNode tail_idx (* dest *);
          VString "Tail" (* name *);
          VNode (mod_op (tail_idx - 1) num_servers) (* pred *);
          VNode (mod_op (tail_idx - 2) num_servers) (* pred_pred *);
          VNode (mod_op (tail_idx + 1) num_servers) (* succ *);
          VNode (mod_op (tail_idx + 2) num_servers) (* succ_succ *);
          VNode head_idx;
          VNode tail_idx;
          VNode tail_idx;
          VMap (data ());
        ]
        0;
      print_endline "init tail";
      (* Hashtbl.iter (fun _ _ -> print_endline "+1") data; *)
      sync_exec global_state prog false false false [] false
  | "STAR" -> raise (Failure "Not implemented STAR topology")
  | "RING" -> raise (Failure "Not implemented RING topology")
  | "FULL" ->
      for i = 0 to num_servers - 1 do
        schedule_client global_state prog "init"
          [
            VNode i;
            VList
              (ref
                 (List.init num_servers (fun j -> VNode j)
                 |> List.filter (fun node ->
                        match node with VNode n -> n <> i | _ -> true)));
          ]
          0;
        sync_exec global_state prog false false false [] false
      done
  | _ -> raise (Failure "Invalid topology")

let schedule_vr_executions (global_state : state) (prog : program) : unit =
  let scheduler = schedule_client global_state prog in
  scheduler "newEntry" [ VInt 1; VString "Hello world" ] 0;
  scheduler "newEntry" [ VNode 0; VString "WON_ELECTION" ] 0;
  scheduler "newEntry" [ VNode 0; VString "WRITE KEY" ] 0

let init_clients (global_state : state) (prog : program) : unit =
  for i = 0 to num_clients - 1 do
    let client_id = num_servers + i in
    let init_fn = Env.find prog.client_ops "BASE_CLIENT_INIT" in
    let env = Env.create 91 in
    let record =
      {
        pc = init_fn.entry;
        node = client_id;
        continuation = (fun _ -> ());
        env;
        id = -1;
        x = 0.0;
        f = (fun x -> x);
      }
    in
    global_state.records <- [ record ];
    sync_exec global_state prog false false false [] false
  done

let interp (spec : string) (intermediate_output : string)
    (scheduler_config_json : string) : unit =
  let config = read_config_file scheduler_config_json in
  let randomly_drop_msgs = config.randomly_drop_msgs in
  let cut_tail_from_mid = config.cut_tail_from_mid in
  let sever_all_to_tail_but_mid = config.sever_all_to_tail_but_mid in
  let partition_away_nodes = config.partition_away_nodes in
  let randomly_delay_msgs = config.randomly_delay_msgs in

  (* Load the program into the simulator *)
  let _ = parse_file spec in
  ();
  let prog =
    let ast = parse_file spec in
    process_program ast
  in
  init_clients global_state prog;
  init_topology topology global_state prog;
  schedule_vr_executions global_state prog;

  bootlegged_sync_exec global_state prog randomly_drop_msgs cut_tail_from_mid
    sever_all_to_tail_but_mid partition_away_nodes randomly_delay_msgs;

  let oc = open_out intermediate_output in
  Printf.fprintf oc "ClientID,Kind,Action,Node,Payload,Value\n";
  DA.iter
    (fun op ->
      Printf.fprintf oc "%d," op.client_id;
      (match op.kind with
      | Response -> Printf.fprintf oc "Response,"
      | Invocation -> Printf.fprintf oc "Invocation,");
      Printf.fprintf oc "%s," op.op_action;
      List.iter
        (fun v ->
          match v with
          | VInt i -> Printf.fprintf oc "%d," i
          | VBool b -> Printf.fprintf oc "%s," (string_of_bool b)
          | VString s -> Printf.fprintf oc "%s," s
          | VNode n -> Printf.fprintf oc "%d," n
          | VFuture _ -> Printf.fprintf oc "TODO implement VFuture"
          | VMap _ -> Printf.fprintf oc "TODO implement VMap"
          | VOption _ -> Printf.fprintf oc "TODO implement VOptions"
          | VList _ -> Printf.fprintf oc "TODO implement VList"
          | VUnit -> Printf.fprintf oc "TODO implement VUnit"
          | VTuple _ -> Printf.fprintf oc "TODO implement VTuple")
        op.payload;
      Printf.fprintf oc "\n")
    global_state.history;
  print_global_nodes global_state.nodes

let handle_arguments () : string * string * string =
  if Array.length Sys.argv < 4 then (
    Printf.printf
      "Usage: %s <spec> <intermediate_output> <scheduler_config.json>\n"
      Sys.argv.(0);
    exit 1)
  else
    let spec = Sys.argv.(1) in
    let intermediate_output = Sys.argv.(2) in
    let scheduler_config_json = Sys.argv.(3) in
    Printf.printf
      "Input spec: %s, intermediate output: %s, scheduler_config_json: %s\n"
      spec intermediate_output scheduler_config_json;
    (spec, intermediate_output, scheduler_config_json)

let () =
  let spec, intermediate_output, scheduler_config_json = handle_arguments () in
  interp spec intermediate_output scheduler_config_json;
  print_endline "Program recognized as valid!";
  print_endline "Program ran successfully!"
