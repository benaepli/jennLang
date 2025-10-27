open Mylib.Simulator
open Mylib.Loader
open Mylib.History
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
  let tbl = ValueMap.create 1024 in
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
        (fun _ -> Env.create 1024);
    records = [];
    history = DA.create ();
    free_clients = List.init num_clients (fun i -> num_servers + i);
    free_sys_threads =
      List.init num_sys_threads (fun i -> num_servers + num_clients + i);
  }

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

let find_client_op_by_suffix (prog : program) (suffix : string) : string =
  let op_name =
    Env.fold
      (fun name _ acc ->
        if BatString.ends_with name suffix then Some name else acc)
      prog.client_ops None
  in
  match op_name with
  | Some name -> name
  | None -> failwith ("Could not find a client operation with suffix: " ^ suffix)

let init_topology (topology : string) (global_state : state) (prog : program)
    (operation_id_counter : int ref) : unit =
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
          (operation_id_counter := !operation_id_counter + 1;
           !operation_id_counter);
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
        (operation_id_counter := !operation_id_counter + 1;
         !operation_id_counter);
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
        (operation_id_counter := !operation_id_counter + 1;
         !operation_id_counter);
      print_endline "init tail";
      (* Hashtbl.iter (fun _ _ -> print_endline "+1") data; *)
      sync_exec global_state prog false false false [] false
  | "STAR" -> raise (Failure "Not implemented STAR topology")
  | "RING" -> raise (Failure "Not implemented RING topology")
  | "FULL" ->
      let init_fn_name = find_client_op_by_suffix prog "_init" in
      for i = 0 to num_servers - 1 do
        schedule_client global_state prog init_fn_name
          [ VNode i; VList (ref (List.init num_servers (fun j -> VNode j))) ]
          (operation_id_counter := !operation_id_counter + 1;
           !operation_id_counter);
        sync_exec global_state prog false false false [] false
      done
  | _ -> raise (Failure "Invalid topology")

let _schedule_vr_executions (global_state : state) (prog : program)
    (operation_id_counter : int ref) : unit =
  let scheduler prog_name actuals =
    schedule_client global_state prog
      (find_client_op_by_suffix prog prog_name)
      actuals
      (operation_id_counter := !operation_id_counter + 1;
       !operation_id_counter)
  in
  scheduler "newEntry" [ VNode 1; VInt 101 ];
  scheduler "newEntry" [ VNode 0; VInt 201 ];
  scheduler "getCommittedLog" [ VNode 0 ]

let rec schedule_random_op (global_state : state) (prog : program)
    (operation_id_counter : int ref) (client_id : int) : unit =
  Random.self_init ();
  let r = Random.float 1.0 in

  let op_name_suffix =
    if r < 0.01 then "simulateTimeout"
      (* 1% chance of triggering a view change *)
    else if r < 0.50 then "newEntry" (* 49% chance of newEntry *)
    else "getCommittedLog" (* 50% chance of getCommittedLog *)
  in
  let op_name = find_client_op_by_suffix prog op_name_suffix in

  let target_node = Random.int num_servers in

  let actuals =
    if op_name_suffix = "newEntry" then
      [ VNode target_node; VInt (Random.int 1000) ]
    else
      (* Both getCommittedLog and simulateTimeout just take a target node *)
      [ VNode target_node ]
  in
  let op = Env.find prog.client_ops op_name in
  let env = Env.create 1024 in
  List.iter2 (fun formal actual -> Env.add env formal actual) op.formals actuals;

  let new_op_id =
    operation_id_counter := !operation_id_counter + 1;
    !operation_id_counter
  in

  let invocation =
    {
      client_id;
      op_action = op.name;
      kind = Invocation;
      payload = actuals;
      unique_id = new_op_id;
    }
  in
  DA.add global_state.history invocation;
  let new_continuation value =
    let response =
      {
        client_id;
        op_action = op.name;
        kind = Response;
        payload = [ value ];
        unique_id = new_op_id;
      }
    in
    DA.add global_state.history response;
    schedule_random_op global_state prog operation_id_counter client_id
  in

  let record =
    {
      pc = op.entry;
      node = client_id;
      continuation = new_continuation;
      env;
      id = new_op_id;
      x = 0.4;
      f = (fun x -> x /. 2.0);
    }
  in
  global_state.records <- record :: global_state.records

let start_random_client_loops (global_state : state) (prog : program)
    (operation_id_counter : int ref) : unit =
  let clients_to_start = global_state.free_clients in
  global_state.free_clients <- [];

  (* Empty the list, as they will be permanently busy *)
  Printf.printf "Starting random work loops for %d clients...\n"
    (List.length clients_to_start);
  List.iter
    (fun client_id ->
      schedule_random_op global_state prog operation_id_counter client_id)
    clients_to_start

let init_clients (global_state : state) (prog : program) : unit =
  for i = 0 to num_clients - 1 do
    let client_id = num_servers + i in
    let init_fn = Env.find prog.client_ops "BASE_CLIENT_INIT" in
    let env = Env.create 1024 in
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

let init_nodes (global_state : state) (prog : program) : unit =
  (* Find the role init function by its suffix, which the compiler guarantees *)
  let init_fn_name =
    Env.fold
      (fun name _ acc ->
        if BatString.ends_with name "BASE_NODE_INIT" then Some name else acc)
      prog.rpc None
  in
  match init_fn_name with
  | None -> failwith "Could not find Role BASE_NODE_INIT function in prog.rpc"
  | Some name ->
      Printf.printf "Found Node init function: %s\n" name;
      let init_fn = Env.find prog.rpc name in
      for i = 0 to num_servers - 1 do
        let node_id = i in
        let env = Env.create 1024 in
        let record =
          {
            pc = init_fn.entry;
            node = node_id;
            continuation = (fun _ -> ());
            (* Fire and forget *)
            env;
            id = -1;
            (* System init *)
            x = 0.0;
            f = (fun x -> x);
          }
        in
        (* Add the record and run it to initialize the node's state *)
        global_state.records <- record :: global_state.records;
        sync_exec global_state prog false false false [] false
      done

let interp (compiled_json : string) (intermediate_output : string)
    (scheduler_config_json : string) : unit =
  let config = read_config_file scheduler_config_json in
  let randomly_drop_msgs = config.randomly_drop_msgs in
  let cut_tail_from_mid = config.cut_tail_from_mid in
  let sever_all_to_tail_but_mid = config.sever_all_to_tail_but_mid in
  let partition_away_nodes = config.partition_away_nodes in
  let randomly_delay_msgs = config.randomly_delay_msgs in

  (* Load the program from the compiled JSON file *)
  let prog = load_program_from_file compiled_json in
  let operation_id_counter = ref 0 in
  init_clients global_state prog;
  init_nodes global_state prog;
  init_topology topology global_state prog operation_id_counter;
  (* schedule_vr_executions global_state prog operation_id_counter; *)
  start_random_client_loops global_state prog operation_id_counter;

  bootlegged_sync_exec global_state prog randomly_drop_msgs cut_tail_from_mid
    sever_all_to_tail_but_mid partition_away_nodes randomly_delay_msgs;
  save_history_to_csv global_state.history intermediate_output;
  print_global_nodes global_state.nodes

let handle_arguments () : string * string * string =
  if Array.length Sys.argv < 4 then (
    Printf.printf
      "Usage: %s <compiled_json> <intermediate_output> <scheduler_config.json>\n"
      Sys.argv.(0);
    exit 1)
  else
    let compiled_json = Sys.argv.(1) in
    let intermediate_output = Sys.argv.(2) in
    let scheduler_config_json = Sys.argv.(3) in
    Printf.printf
      "Input JSON: %s, intermediate output: %s, scheduler_config_json: %s\n"
      compiled_json intermediate_output scheduler_config_json;
    (compiled_json, intermediate_output, scheduler_config_json)

let () =
  let compiled_json, intermediate_output, scheduler_config_json =
    handle_arguments ()
  in
  interp compiled_json intermediate_output scheduler_config_json;
  print_endline "Compiled program loaded successfully!";
  print_endline "Program ran successfully!"
