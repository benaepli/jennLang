open Mylib.Simulator
open Mylib.Loader
open Mylib.History
module Benchmark = Benchmark
open Config
open Execution
open PlanGenerator
open Sqlite3

let setup_log ?(style_renderer = `Ansi_tty) level =
  Fmt_tty.setup_std_outputs ~style_renderer ();
  Logs.set_level level;
  Logs.set_reporter (Logs_fmt.reporter ())

let num_sys_threads (num_servers : int) = num_servers * 3
let src = Logs.Src.create "main" ~doc:"Main logs"

module Log = (val Logs.src_log src : Logs.LOG)

(** Create a fresh global state for each benchmark run *)
let create_fresh_global_state (num_servers : int) (num_clients : int) : state =
  {
    nodes =
      Array.init
        (num_servers + num_clients + num_sys_threads num_servers)
        (fun _ -> Env.create 1024);
    runnable_records = DA.create ();
    waiting_records = [];
    channels = Hashtbl.create 1024;
    history = DA.create ();
    free_clients = List.init num_clients (fun i -> num_servers + i);
    crash_info =
      {
        currently_crashed = BatSet.Int.empty;
        recovery_schedule = [];
        current_step = 0;
        queued_messages = [];
      };
    delivery_pc = None;
  }

let sync_exec (global_state : state) (prog : program)
    (randomly_delay_msgs : bool) : unit =
  while not (DA.length global_state.runnable_records = 0) do
    schedule_record global_state prog false false false [] randomly_delay_msgs
  done

let print_single_node (node : value Env.t) =
  Env.iter
    (fun key value ->
      if not (BatString.starts_with key "_tmp") then
        Log.info (fun m -> m "%s: %s" key (to_string_value value)))
    node

let print_global_nodes (nodes : value Env.t array) =
  Array.iter
    (fun node ->
      Log.info (fun m -> m "Node has:");
      print_single_node node;
      Log.info (fun m -> m ""))
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
        let env = Env.create 1024 in
        let peers_list =
          VList (List.init topology.num_servers (fun j -> VNode j))
        in
        let actuals = [ VInt node_id; peers_list ] in

        (try
           List.iter2
             (fun formal actual -> Env.add env formal actual)
             init_fn.formals actuals
         with Invalid_argument _ ->
           failwith ("Mismatched arguments for " ^ init_fn_name));
        let temp_record_env =
          { local_env = env; node_env = global_state.nodes.(node_id) }
        in
        List.iter
          (fun (var_name, default_expr) ->
            Env.add env var_name (eval temp_record_env default_expr))
          init_fn.locals;
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
        DA.add global_state.runnable_records record;
        sync_exec global_state prog false
      done
  | _ -> raise (Failure "Invalid topology")

let init_clients (global_state : state) (prog : program) (num_servers : int)
    (num_clients : int) : unit =
  for i = 0 to num_clients - 1 do
    let client_id = num_servers + i in
    let init_fn = function_info "ClientInterface.BASE_NODE_INIT" prog in
    let env = Env.create 1024 in
    let record_env =
      { local_env = env; node_env = global_state.nodes.(client_id) }
    in
    List.iter
      (fun (var_name, default_expr) ->
        Env.add env var_name (eval record_env default_expr))
      init_fn.locals;
    Env.add record_env.local_env "self" (VNode client_id);

    let _ = exec_sync global_state prog record_env init_fn.entry in
    ()
  done

let init_nodes (global_state : state) (prog : program) (num_servers : int) :
    unit =
  let init_fn_name = "Node.BASE_NODE_INIT" in
  Log.info (fun m -> m "Found Node init function: %s" init_fn_name);
  let init_fn = Env.find prog.rpc init_fn_name in

  for i = 0 to num_servers - 1 do
    let node_id = i in
    let env = Env.create 1024 in
    let record_env =
      { local_env = env; node_env = global_state.nodes.(node_id) }
    in
    List.iter
      (fun (var_name, default_expr) ->
        Env.add env var_name (eval record_env default_expr))
      init_fn.locals;
    Env.add record_env.local_env "self" (VNode node_id);

    let _ = exec_sync global_state prog record_env init_fn.entry in
    ()
  done

(** Main interpreter function that runs a single simulation *)
let interp (compiled_json : string) (db : db) (run_id : int)
    (run_config : single_run_config) (max_iterations : int) : unit =
  let gen_config = run_config.plan_gen_config in
  let randomly_delay_msgs = run_config.randomly_delay_msgs in

  (* Create a fresh global state for this run *)
  let fresh_state =
    create_fresh_global_state gen_config.num_servers gen_config.num_clients
  in

  (* Load the program from the compiled JSON file *)
  let prog = load_program_from_file compiled_json in
  let operation_id_counter = ref 0 in

  (* Initialize nodes and clients *)
  init_clients fresh_state prog gen_config.num_servers gen_config.num_clients;
  init_nodes fresh_state prog gen_config.num_servers;

  let topology = { topology = "FULL"; num_servers = gen_config.num_servers } in
  init_topology topology fresh_state prog;

  let test_plan = PlanGenerator.generate_plan gen_config in

  exec_plan fresh_state prog test_plan max_iterations topology
    operation_id_counter randomly_delay_msgs;

  save_history db run_id fresh_state.history;
  print_global_nodes fresh_state.nodes

(** Helper to create a list of ints from a range *)
let expand_range (r : range) : int list =
  let rec aux current =
    if current > r.max then [] else current :: aux (current + r.step)
  in
  aux r.min

(** Create a directory, ignoring if it already exists *)
let ensure_dir_exists (dir : string) : unit =
  try Unix.mkdir dir 0o755 with Unix.Unix_error (Unix.EEXIST, _, _) -> ()

(** Main entry point *)
let () =
  if
    Array.length Sys.argv >= 2
    && (Sys.argv.(1) = "-h" || Sys.argv.(1) = "--help")
  then (
    Printf.printf
      "Usage: %s <compiled_json> <explorer_config.json> <output_dir>\n"
      Sys.argv.(0);
    exit 0);

  if Array.length Sys.argv <> 4 then (
    Printf.eprintf
      "Usage: %s <compiled_json> <explorer_config.json> <output_dir>\n"
      Sys.argv.(0);
    exit 1);

  let compiled_json = Sys.argv.(1) in
  let explorer_config_json = Sys.argv.(2) in
  let output_dir = Sys.argv.(3) in

  setup_log (Some Logs.Warning);
  Log.app (fun m -> m "🚀 Starting Execution Explorer...");
  Log.app (fun m -> m "Program: %s" compiled_json);
  Log.app (fun m -> m "Config: %s" explorer_config_json);
  Log.app (fun m -> m "Output directory: %s" output_dir);

  (* Read the main explorer config *)
  let config = read_config_file explorer_config_json in
  ensure_dir_exists output_dir;

  (* Expand all parameter ranges into lists *)
  let all_servers = expand_range config.num_servers_range in
  let all_clients = expand_range config.num_clients_range in
  let all_writes = expand_range config.num_write_ops_range in
  let all_reads = expand_range config.num_read_ops_range in
  let all_timeouts = expand_range config.num_timeouts_range in
  let all_crashes = expand_range config.num_crashes_range in
  let all_densities = config.dependency_density_values in

  let total_configs =
    List.length all_servers * List.length all_clients * List.length all_writes
    * List.length all_reads * List.length all_timeouts * List.length all_crashes
    * List.length all_densities
  in
  Log.info (fun m -> m "Total unique configurations to test: %d" total_configs);
  Log.info (fun m -> m "Runs per configuration: %d" config.num_runs_per_config);
  Log.info (fun m ->
      m "Total simulations: %d\n" (total_configs * config.num_runs_per_config));

  let config_counter = ref 0 in
  let run_counter = ref 0 in

  let db_file = Filename.concat output_dir "output.db" in
  let db = db_open db_file in
  init_sqlite db;

  (* Start the exploration loops (nested) *)
  List.iter
    (fun num_servers ->
      List.iter
        (fun num_clients ->
          List.iter
            (fun num_writes ->
              List.iter
                (fun num_reads ->
                  List.iter
                    (fun num_timeouts ->
                      List.iter
                        (fun num_crashes ->
                          List.iter
                            (fun density ->
                              incr config_counter;
                              let config_name =
                                Printf.sprintf
                                  "s%d_c%d_w%d_r%d_t%d_crash%d_d%.2f"
                                  num_servers num_clients num_writes num_reads
                                  num_timeouts num_crashes density
                              in
                              Log.app (fun m -> m "%s" (String.make 70 '='));
                              Log.app (fun m ->
                                  m "Running Config %d / %d: %s" !config_counter
                                    total_configs config_name);
                              Log.app (fun m -> m "%s" (String.make 70 '='));

                              (* This is the specific config for this single run *)
                              let run_config : single_run_config =
                                {
                                  plan_gen_config =
                                    {
                                      num_servers;
                                      num_clients;
                                      num_write_ops = num_writes;
                                      num_read_ops = num_reads;
                                      num_timeouts;
                                      num_crashes;
                                      dependency_density = density;
                                    };
                                  randomly_delay_msgs =
                                    config.randomly_delay_msgs;
                                }
                              in

                              for i = 1 to config.num_runs_per_config do
                                incr run_counter;
                                Log.app (fun m ->
                                    m " Run %d/%d (overall #%d) %!" i
                                      config.num_runs_per_config !run_counter);

                                let start_time = Unix.gettimeofday () in
                                try
                                  (* Run the simulation *)
                                  interp compiled_json db !run_counter
                                    run_config config.max_iterations;

                                  let end_time = Unix.gettimeofday () in
                                  Log.app (fun m ->
                                      m " Success (%.4fs)"
                                        (end_time -. start_time))
                                with
                                | Failure msg ->
                                    Log.err (fun m ->
                                        m " Failed (Error: %s)" msg)
                                | e ->
                                    Log.err (fun m ->
                                        m " Failed (Exception: %s)"
                                          (Printexc.to_string e))
                              done)
                            all_densities)
                        all_crashes)
                    all_timeouts)
                all_reads)
            all_writes)
        all_clients)
    all_servers;

  ignore (db_close db);
  Log.app (fun m -> m "\nExecution explorer finished.")
