open Mylib.Simulator
open Mylib.Loader
open Mylib.History

(* Include the benchmark module *)
module Benchmark = Benchmark
open Plan
open Config
open Execution

let num_servers = 3
let num_clients = 3
let num_sys_threads = num_servers * 3

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

let init_topology (topology : topology_info) (global_state : state)
    (prog : program) : unit =
  match topology.topology with
  | "LINEAR" -> raise (Failure "Not implemented LINEAR topology")
  | "STAR" -> raise (Failure "Not implemented STAR topology")
  | "RING" -> raise (Failure "Not implemented RING topology")
  | "FULL" ->
      let init_fn_name = "Node.Init" in
      let init_fn = Env.find prog.rpc init_fn_name in
      for i = 0 to topology.num_servers - 1 do
        let node_id = i in
        let peers_list =
          VList (ref (List.init topology.num_servers (fun j -> VNode j)))
        in
        let actuals = [ VInt node_id; peers_list ] in
        let env =
          create_initialized_env default_env_size
            global_state.nodes.(node_id)
            init_fn.formals actuals init_fn.locals ~context_name:init_fn_name ()
        in
        let record =
          {
            pc = init_fn.entry;
            node = node_id;
            origin_node = node_id;
            continuation = (fun _ -> ());
            env;
            id = -1;
            x = 0.0;
            f = (fun x -> x);
          }
        in
        global_state.runnable_records <- record :: global_state.runnable_records;
        sync_exec global_state prog false false false [] false
      done
  | _ -> raise (Failure "Invalid topology")

let init_clients (global_state : state) (prog : program) : unit =
  for i = 0 to num_clients - 1 do
    let client_id = num_servers + i in
    let init_fn = function_info "ClientInterface.BASE_NODE_INIT" prog in
    let env =
      create_initialized_env default_env_size
        global_state.nodes.(client_id)
        [] [] init_fn.locals ~context_name:"ClientInterface.BASE_NODE_INIT" ()
    in
    Env.add env "self" (VNode client_id);

    let record_env =
      { local_env = env; node_env = global_state.nodes.(client_id) }
    in
    let _ = exec_sync prog record_env init_fn.entry in
    ()
  done

let init_nodes (global_state : state) (prog : program) : unit =
  let init_fn_name = "Node.BASE_NODE_INIT" in
  Printf.printf "Found Node init function: %s\n" init_fn_name;
  let init_fn = Env.find prog.rpc init_fn_name in
  for i = 0 to num_servers - 1 do
    let node_id = i in
    let env =
      create_initialized_env default_env_size
        global_state.nodes.(node_id)
        [] [] init_fn.locals ~context_name:init_fn_name ()
    in
    Env.add env "self" (VNode node_id);

    let record_env =
      { local_env = env; node_env = global_state.nodes.(node_id) }
    in
    let _ = exec_sync prog record_env init_fn.entry in
    ()
  done

(** Main interpreter function - runs a single simulation *)
let interp (compiled_json : string) (intermediate_output : string)
    (scheduler_config_json : string) (max_iterations : int) : unit =
  let _config = read_config_file scheduler_config_json in
  (* let randomly_drop_msgs = config.randomly_drop_msgs in
  let cut_tail_from_mid = config.cut_tail_from_mid in
  let sever_all_to_tail_but_mid = config.sever_all_to_tail_but_mid in
  let partition_away_nodes = config.partition_away_nodes in
  let randomly_delay_msgs = config.randomly_delay_msgs in *)

  (* Create a fresh global state for this run *)
  let fresh_state = create_fresh_global_state () in

  (* Load the program from the compiled JSON file *)
  let prog = load_program_from_file compiled_json in
  let operation_id_counter = ref 0 in
  init_clients fresh_state prog;
  init_nodes fresh_state prog;

  let topology = { topology = "FULL"; num_servers } in
  init_topology topology fresh_state prog;

  let test_plan : execution_plan =
    [
      {
        id = "event_A_write";
        action = ClientRequest (Write (0, "key1", "val1"));
        dependencies = [];
      };
      {
        id = "event_B_read_after_A";
        action = ClientRequest (Read (1, "key1"));
        dependencies = [ EventCompleted "event_A_write" ];
      };
      {
        id = "event_C_crash_node_2";
        action = CrashNode 2;
        dependencies = [];
        (* Crashes immediately *)
      };
      {
        id = "event_D_recover_node_2";
        action = RecoverNode 2;
        (* This event will only run after event_B (the read) completes *)
        dependencies = [ EventCompleted "event_B_read_after_A" ];
      };
    ]
  in

  exec_plan fresh_state prog test_plan max_iterations topology
    operation_id_counter;

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
