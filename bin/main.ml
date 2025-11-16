open Mylib.Simulator
open Mylib.Loader
open Mylib.History

(* Include the benchmark module *)
module Benchmark = Benchmark
open Config

let num_servers = 3
let num_clients = 3
let num_sys_threads = num_servers * 3
let topology = "FULL"

(** Create a fresh global state for each benchmark run *)
let create_fresh_global_state () : state =
  {
    nodes =
      Array.init
        (num_servers + num_clients + num_sys_threads)
        (fun _ -> Env.create 1024);
    runnable_records = [];
    waiting_records = [];
    history = DA.create ();
    free_clients = List.init num_clients (fun i -> num_servers + i);
    free_sys_threads =
      List.init num_sys_threads (fun i -> num_servers + num_clients + i);
    crash_info =
      {
        currently_crashed = [];
        recovery_schedule = [];
        current_step = 0;
        queued_messages = [];
      };
  }

let sync_exec (global_state : state) (prog : program)
    (randomly_drop_msgs : bool) (cut_tail_from_mid : bool)
    (sever_all_to_tail_but_mid : bool) (partition_away_nodes : int list)
    (randomly_delay_msgs : bool) : unit =
  while not (List.length global_state.runnable_records = 0) do
    schedule_record global_state prog randomly_drop_msgs cut_tail_from_mid
      sever_all_to_tail_but_mid partition_away_nodes randomly_delay_msgs
  done

let print_single_node (node : value Env.t) =
  Env.iter
    (fun key value ->
      (* Only print if the key does not start with "_tmp" *)
      if not (BatString.starts_with key "_tmp") then
        Printf.printf "%s: %s\n" key (to_string_value value))
    node

let print_global_nodes (nodes : value Env.t array) =
  Array.iter
    (fun node ->
      print_endline "Node has:";
      print_single_node node;
      print_endline "")
    nodes

let init_topology (topology : string) (global_state : state) (prog : program)
    (operation_id_counter : int ref) : unit =
  match topology with
  | "LINEAR" -> raise (Failure "Not implemented LINEAR topology")
  | "STAR" -> raise (Failure "Not implemented STAR topology")
  | "RING" -> raise (Failure "Not implemented RING topology")
  | "FULL" ->
      let init_fn_name = "ClientInterface.Init" in
      for i = 0 to num_servers - 1 do
        schedule_client global_state prog init_fn_name
          [ VInt i; VList (ref (List.init num_servers (fun j -> VNode j))) ]
          (operation_id_counter := !operation_id_counter + 1;
           !operation_id_counter);
        sync_exec global_state prog false false false [] false
      done
  | _ -> raise (Failure "Invalid topology")

let recover_node_in_topology (topology : string) (state : state)
    (prog : program) (node_id : int) continuation : unit =
  let recover_fn_name = "ClientInterface.Recover" in

  if Env.mem prog.rpc recover_fn_name then (
    Printf.printf "  Found recover function: %s\n" recover_fn_name;
    let recover_fn = Env.find prog.rpc recover_fn_name in

    (* Prepare actuals based on topology *)
    let actuals =
      match topology with
      | "FULL" -> [ VList (ref (List.init num_servers (fun j -> VNode j))) ]
      | _ -> failwith "Recovery not implemented for this topology"
    in

    (* Create activation record to execute recover on the node *)
    let env = Env.create 1024 in
    List.iter2
      (fun formal actual -> Env.add env formal actual)
      recover_fn.formals actuals;

    let temp_record_env =
      { local_env = env; node_env = state.nodes.(node_id) }
    in
    List.iter
      (fun (var_name, default_expr) ->
        Env.add env var_name (eval temp_record_env default_expr))
      recover_fn.locals;

    let record =
      {
        pc = recover_fn.entry;
        node = node_id;
        continuation = (fun _ -> continuation ());
        env;
        id = -1;
        (* System recovery *)
        x = 0.0;
        f = (fun x -> x);
      }
    in
    Printf.printf "here";

    (* Execute recovery directly on the node *)
    state.runnable_records <- record :: state.runnable_records)

let reinit_single_node (topology : string) (state : state) (prog : program)
    (node_id : int) continuation : unit =
  (* Re-run BASE_NODE_INIT *)
  let init_fn_name = "ClientInterface.BASE_NODE_INIT" in
  let init_fn = Env.find prog.rpc init_fn_name in
  let env = Env.create 1024 in
  let record =
    {
      pc = init_fn.entry;
      node = node_id;
      continuation =
        (fun _ ->
          recover_node_in_topology topology state prog node_id continuation);
      env;
      id = -1;
      x = 0.0;
      f = (fun x -> x);
    }
  in
  state.runnable_records <- record :: state.runnable_records

let check_and_apply_crashes (state : state) (prog : program) (topology : string)
    (crash_config : crash_config) (current_step : int) : unit =
  let ci = state.crash_info in
  ci.current_step <- current_step;

  if not crash_config.enable_random_crashes then ()
  else
    (* Check for scheduled recoveries *)
    let to_recover, still_scheduled =
      List.partition
        (fun (_, recover_step) -> recover_step <= current_step)
        ci.recovery_schedule
    in
    ci.recovery_schedule <- still_scheduled;

    List.iter
      (fun (node_id, _) ->
        Printf.printf "RECOVER: Node %d at step %d\n" node_id current_step;
        (* Reset node to fresh state *)
        state.nodes.(node_id) <- Env.create 1024;

        let reinstate () =
          ci.currently_crashed <-
            List.filter (fun n -> n <> node_id) ci.currently_crashed;
          let queued_for_node, remaining_queue =
            List.partition (fun (dest, _) -> dest = node_id) ci.queued_messages
          in
          ci.queued_messages <- remaining_queue;
          Printf.printf "  Delivering %d queued messages to node %d\n"
            (List.length queued_for_node)
            node_id;
          List.iter
            (fun (_, record) ->
              state.runnable_records <- record :: state.runnable_records)
            queued_for_node
        in
        (* Re-run BASE_NODE_INIT *)
        reinit_single_node topology state prog node_id reinstate)
      to_recover;

    (* Randomly crash nodes *)
    let crashable = List.init num_servers (fun i -> i) in

    (* Filter out already crashed nodes *)
    let eligible =
      List.filter (fun n -> not (List.mem n ci.currently_crashed)) crashable
    in
    List.iter
      (fun node_id ->
        Random.self_init ();
        if Random.float 1.0 < crash_config.crash_probability then (
          (* This node crashes. *)
          let recovery_time =
            crash_config.min_recovery_time
            + Random.int
                (crash_config.max_recovery_time - crash_config.min_recovery_time
               + 1)
          in
          let recover_at = current_step + recovery_time in

          Printf.printf "CRASH: Node %d at step %d (will recover at step %d)\n"
            node_id current_step recover_at;

          ci.currently_crashed <- node_id :: ci.currently_crashed;
          ci.recovery_schedule <- (node_id, recover_at) :: ci.recovery_schedule;

          (* Drop all pending work for this crashed node *)
          state.runnable_records <-
            List.filter (fun r -> r.node <> node_id) state.runnable_records;
          state.waiting_records <-
            List.filter (fun r -> r.node <> node_id) state.waiting_records))
      eligible

let rec schedule_random_op (global_state : state) (prog : program)
    (operation_id_counter : int ref) (client_id : int)
    (written_keys : string list ref) : unit =
  Random.self_init ();
  let r = Random.float 1.0 in

  let new_op_id =
    operation_id_counter := !operation_id_counter + 1;
    !operation_id_counter
  in

  let op_name, actuals, on_response_hook =
    if r < 0.01 then
      (* 1% chance of triggering a view change *)
      let target_node = Random.int num_servers in
      ("ClientInterface.SimulateTimeout", [ VNode target_node ], fun () -> ())
    else if r < 0.49 || List.length !written_keys = 0 then
      (* 49% chance of write, OR force write if no keys exist for reading *)
      let target_node = Random.int num_servers in
      let new_key = "key_" ^ string_of_int new_op_id in
      let new_val = "val_" ^ string_of_int (Random.int 1000) in

      (* Create a hook to add the key to written_keys upon receiving response *)
      let hook () =
        (* let _ = Printf.printf "Written key: %s\n" new_key in *)
        written_keys := new_key :: !written_keys
      in

      ( "ClientInterface.Write",
        [ VNode target_node; VString new_key; VString new_val ],
        hook )
    else
      (* 50% chance of read (and keys exist) *)
      let target_node = Random.int num_servers in

      (* Pick a random key from the ones we've written *)
      let key_to_read =
        List.nth !written_keys (Random.int (List.length !written_keys))
      in

      ( "ClientInterface.Read",
        [ VNode target_node; VString key_to_read ],
        fun () -> () )
  in
  let op = function_info op_name prog in
  let env = Env.create 1024 in
  List.iter2 (fun formal actual -> Env.add env formal actual) op.formals actuals;

  let temp_record_env =
    { local_env = env; node_env = global_state.nodes.(client_id) }
  in
  List.iter
    (fun (var_name, default_expr) ->
      Env.add env var_name (eval temp_record_env default_expr))
    op.locals;

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
    (* Execute the hook after receiving the response *)
    on_response_hook ();
    schedule_random_op global_state prog operation_id_counter client_id
      written_keys
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
  global_state.runnable_records <- record :: global_state.runnable_records

let start_random_client_loops (global_state : state) (prog : program)
    (operation_id_counter : int ref) : unit =
  let clients_to_start = global_state.free_clients in
  global_state.free_clients <- [];

  let written_keys = ref [] in

  Printf.printf "Starting random work loops for %d clients...\n"
    (List.length clients_to_start);
  List.iter
    (fun client_id ->
      schedule_random_op global_state prog operation_id_counter client_id
        written_keys)
    clients_to_start

let init_clients (global_state : state) (prog : program) : unit =
  for i = 0 to num_clients - 1 do
    let client_id = num_servers + i in
    let init_fn = function_info "ClientInterface.BASE_NODE_INIT" prog in
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
    global_state.runnable_records <- [ record ];
    sync_exec global_state prog false false false [] false
  done

let init_nodes (global_state : state) (prog : program) : unit =
  let init_fn_name = "Node.BASE_NODE_INIT" in
  Printf.printf "Found Node init function: %s\n" init_fn_name;
  let init_fn = Env.find prog.rpc init_fn_name in
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
    global_state.runnable_records <- record :: global_state.runnable_records;
    sync_exec global_state prog false false false [] false
  done

let bootlegged_sync_exec (global_state : state) (prog : program)
    (randomly_drop_msgs : bool) (cut_tail_from_mid : bool)
    (sever_all_to_tail_but_mid : bool) (partition_away_nodes : int list)
    (randomly_delay_msgs : bool) (max_iterations : int)
    (crash_conf : crash_config) : unit =
  let count = ref 0 in
  for _ = 0 to max_iterations do
    if not (List.length global_state.runnable_records = 0) then (
      count := !count + 1;
      check_and_apply_crashes global_state prog topology crash_conf !count;
      schedule_record global_state prog randomly_drop_msgs cut_tail_from_mid
        sever_all_to_tail_but_mid partition_away_nodes randomly_delay_msgs)
  done;
  Printf.printf "Executed %d record scheduling iterations\n" !count;
  Printf.printf "Remaining runnable records: %d\n"
    (List.length global_state.runnable_records);
  Printf.printf "Waiting records: %d\n"
    (List.length global_state.waiting_records)

(** Main interpreter function - runs a single simulation *)
let interp (compiled_json : string) (intermediate_output : string)
    (scheduler_config_json : string) (max_iterations : int) : unit =
  let config = read_config_file scheduler_config_json in
  let randomly_drop_msgs = config.randomly_drop_msgs in
  let cut_tail_from_mid = config.cut_tail_from_mid in
  let sever_all_to_tail_but_mid = config.sever_all_to_tail_but_mid in
  let partition_away_nodes = config.partition_away_nodes in
  let randomly_delay_msgs = config.randomly_delay_msgs in

  (* Create a fresh global state for this run *)
  let fresh_state = create_fresh_global_state () in

  (* Load the program from the compiled JSON file *)
  let prog = load_program_from_file compiled_json in
  let operation_id_counter = ref 0 in
  init_clients fresh_state prog;
  init_nodes fresh_state prog;
  init_topology topology fresh_state prog operation_id_counter;
  (* schedule_vr_executions fresh_state prog operation_id_counter; *)
  start_random_client_loops fresh_state prog operation_id_counter;

  bootlegged_sync_exec fresh_state prog randomly_drop_msgs cut_tail_from_mid
    sever_all_to_tail_but_mid partition_away_nodes randomly_delay_msgs
    max_iterations config.crash_config;
  save_history_to_csv fresh_state.history intermediate_output;
  print_global_nodes fresh_state.nodes

(** Handle normal execution mode arguments *)
let handle_normal_arguments () : string * string * string * int =
  if Array.length Sys.argv < 5 then (
    Printf.printf
      "Usage: %s <compiled_json> <intermediate_output> <scheduler_config.json> \
       <max_iterations>\n"
      Sys.argv.(0);
    exit 1)
  else
    let compiled_json = Sys.argv.(1) in
    let intermediate_output = Sys.argv.(2) in
    let scheduler_config_json = Sys.argv.(3) in
    let max_iterations = int_of_string Sys.argv.(4) in
    Printf.printf
      "Input JSON: %s, intermediate output: %s, scheduler_config_json: %s, \
       max_iterations: %d\n"
      compiled_json intermediate_output scheduler_config_json max_iterations;
    (compiled_json, intermediate_output, scheduler_config_json, max_iterations)

(** Print usage information *)
let print_usage () : unit =
  Printf.printf "Usage:\n\n";
  Printf.printf "Normal Mode:\n";
  Printf.printf
    "  %s <compiled_json> <intermediate_output> <scheduler_config.json> \
     <max_iterations>\n\n"
    Sys.argv.(0);
  Benchmark.print_usage ()

(** Main entry point *)
let () =
  (* Check if we're in benchmark mode *)
  if Array.length Sys.argv >= 2 && Sys.argv.(1) = "benchmark" then
    (* Benchmark mode *)
    match Benchmark.parse_benchmark_args () with
    | Some config ->
        let _stats = Benchmark.run_benchmark config interp in
        ()
    | None -> exit 1
  else if
    Array.length Sys.argv >= 2
    && (Sys.argv.(1) = "-h" || Sys.argv.(1) = "--help")
  then (
    print_usage ();
    exit 0)
  else
    (* Normal execution mode *)
    let ( compiled_json,
          intermediate_output,
          scheduler_config_json,
          max_iterations ) =
      handle_normal_arguments ()
    in
    interp compiled_json intermediate_output scheduler_config_json
      max_iterations;
    print_endline "Compiled program loaded successfully!";
    print_endline "Program ran successfully!"
