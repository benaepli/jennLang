(** Benchmark module for running and timing simulations *)

type benchmark_config = {
  compiled_json : string;
  scheduler_config_json : string;
  max_iterations : int;
  num_runs : int;
  output_dir : string;
}

type run_result = {
  run_number : int;
  elapsed_time : float;
  output_file : string;
  success : bool;
}

type benchmark_stats = {
  total_runs : int;
  successful_runs : int;
  failed_runs : int;
  total_time : float;
  mean_time : float;
  min_time : float;
  max_time : float;
  std_dev : float;
  run_times : float list;
}

(** Calculate standard deviation of a list of floats *)
let std_dev (values : float list) : float =
  let n = float_of_int (List.length values) in
  if n <= 1.0 then 0.0
  else
    let mean = List.fold_left ( +. ) 0.0 values /. n in
    let variance =
      List.fold_left (fun acc x -> acc +. ((x -. mean) ** 2.0)) 0.0 values
      /. (n -. 1.0)
    in
    sqrt variance

(** Calculate statistics from run results *)
let calculate_stats (results : run_result list) : benchmark_stats =
  let successful_results = List.filter (fun r -> r.success) results in
  let run_times = List.map (fun r -> r.elapsed_time) successful_results in

  let total_runs = List.length results in
  let successful_runs = List.length successful_results in
  let failed_runs = total_runs - successful_runs in

  if successful_runs = 0 then
    {
      total_runs;
      successful_runs = 0;
      failed_runs;
      total_time = 0.0;
      mean_time = 0.0;
      min_time = 0.0;
      max_time = 0.0;
      std_dev = 0.0;
      run_times = [];
    }
  else
    let total_time = List.fold_left ( +. ) 0.0 run_times in
    let mean_time = total_time /. float_of_int successful_runs in
    let min_time = List.fold_left min (List.hd run_times) run_times in
    let max_time = List.fold_left max (List.hd run_times) run_times in
    let std_dev_val = std_dev run_times in

    {
      total_runs;
      successful_runs;
      failed_runs;
      total_time;
      mean_time;
      min_time;
      max_time;
      std_dev = std_dev_val;
      run_times;
    }

(** Print benchmark summary *)
let print_summary (stats : benchmark_stats) : unit =
  Printf.printf "\n%s\n" (String.make 60 '=');
  Printf.printf "📊 BENCHMARK SUMMARY\n";
  Printf.printf "%s\n" (String.make 60 '=');
  Printf.printf "Total Runs:       %d\n" stats.total_runs;
  Printf.printf "Successful Runs:  %d\n" stats.successful_runs;

  if stats.failed_runs > 0 then
    Printf.printf "Failed Runs:      %d\n" stats.failed_runs;

  if stats.successful_runs > 0 then (
    Printf.printf "\n%s\n" (String.make 60 '-');
    Printf.printf "TIMING STATISTICS\n";
    Printf.printf "%s\n" (String.make 60 '-');
    Printf.printf "Total Time:       %.4fs\n" stats.total_time;
    Printf.printf "Mean Time:        %.4fs\n" stats.mean_time;

    if stats.successful_runs > 1 then (
      Printf.printf "Min Time:         %.4fs\n" stats.min_time;
      Printf.printf "Max Time:         %.4fs\n" stats.max_time;
      Printf.printf "Std. Deviation:   %.4fs\n" stats.std_dev;
      Printf.printf "Coefficient of Variation: %.2f%%\n"
        (stats.std_dev /. stats.mean_time *. 100.0));

    Printf.printf "\n%s\n" (String.make 60 '-'))
  else Printf.printf "\n No successful runs to report.\n";
  Printf.printf "%s\n\n" (String.make 60 '=')

(** Run a single benchmark iteration *)
let run_single_iteration (config : benchmark_config) (run_number : int)
    (runner_fn : string -> string -> string -> int -> unit) : run_result =
  let output_file =
    Filename.concat config.output_dir
      (Printf.sprintf "history_run_%d.csv" run_number)
  in

  Printf.printf "\n\nRunning iteration %d/%d... %!\n" run_number config.num_runs;

  let start_time = Unix.gettimeofday () in

  let success =
    try
      runner_fn config.compiled_json output_file config.scheduler_config_json
        config.max_iterations;
      true
    with
    | Failure msg ->
        Printf.printf "FAILED (Error: %s)\n" msg;
        false
    | e ->
        Printf.printf "FAILED (Exception: %s)\n" (Printexc.to_string e);
        false
  in

  let end_time = Unix.gettimeofday () in
  let elapsed_time = end_time -. start_time in

  if success then Printf.printf "Success (%.4fs)\n" elapsed_time
  else Printf.printf "Failed after %.4fs\n" elapsed_time;

  { run_number; elapsed_time; output_file; success }

(** Main benchmark runner *)
let run_benchmark (config : benchmark_config)
    (runner_fn : string -> string -> string -> int -> unit) : benchmark_stats =
  Printf.printf "%s\n" (String.make 60 '=');
  Printf.printf "🏃 STARTING BENCHMARK\n";
  Printf.printf "%s\n" (String.make 60 '=');
  Printf.printf "Executable:        (embedded)\n";
  Printf.printf "Program JSON:      %s\n" config.compiled_json;
  Printf.printf "Config JSON:       %s\n" config.scheduler_config_json;
  Printf.printf "Max Iterations:    %d\n" config.max_iterations;
  Printf.printf "Number of Runs:    %d\n" config.num_runs;
  Printf.printf "Output Directory:  %s\n" config.output_dir;
  Printf.printf "%s\n\n" (String.make 60 '=');

  (* Create output directory if it doesn't exist *)
  (try Unix.mkdir config.output_dir 0o755
   with Unix.Unix_error (Unix.EEXIST, _, _) -> ());

  (* Run all iterations *)
  let results = ref [] in
  for i = 1 to config.num_runs do
    let result = run_single_iteration config i runner_fn in
    results := result :: !results
  done;

  let results = List.rev !results in
  let stats = calculate_stats results in

  (* Print summary *)
  print_summary stats;

  stats

(** Parse benchmark command-line arguments *)
let parse_benchmark_args () : benchmark_config option =
  let args = Array.to_list Sys.argv in

  (* Check if first arg is "benchmark" *)
  match args with
  | _ :: "benchmark" :: rest ->
      (* Default values *)
      let config_ref =
        ref
          {
            compiled_json = "";
            scheduler_config_json = "";
            max_iterations = 1000;
            num_runs = 10;
            output_dir = ".";
          }
      in

      let rec parse_args = function
        | [] -> ()
        | "-n" :: n :: rest ->
            config_ref := { !config_ref with num_runs = int_of_string n };
            parse_args rest
        | "--num-runs" :: n :: rest ->
            config_ref := { !config_ref with num_runs = int_of_string n };
            parse_args rest
        | "-i" :: i :: rest ->
            config_ref := { !config_ref with max_iterations = int_of_string i };
            parse_args rest
        | "--iterations" :: i :: rest ->
            config_ref := { !config_ref with max_iterations = int_of_string i };
            parse_args rest
        | "-o" :: dir :: rest ->
            config_ref := { !config_ref with output_dir = dir };
            parse_args rest
        | "--output-dir" :: dir :: rest ->
            config_ref := { !config_ref with output_dir = dir };
            parse_args rest
        | [ json; config ] ->
            config_ref :=
              {
                !config_ref with
                compiled_json = json;
                scheduler_config_json = config;
              };
            ()
        | _ :: rest -> parse_args rest
      in

      parse_args rest;

      if
        !config_ref.compiled_json = "" || !config_ref.scheduler_config_json = ""
      then (
        Printf.eprintf "Error: Missing required arguments\n";
        Printf.eprintf
          "Usage: %s benchmark [options] <compiled_json> <scheduler_config.json>\n"
          Sys.argv.(0);
        Printf.eprintf "\nOptions:\n";
        Printf.eprintf
          "  -n, --num-runs N       Number of benchmark runs (default: 10)\n";
        Printf.eprintf
          "  -i, --iterations N     Max iterations per run (default: 1000)\n";
        Printf.eprintf
          "  -o, --output-dir DIR   Output directory for CSV files (default: .)\n";
        None)
      else Some !config_ref
  | _ -> None

(** Print benchmark usage *)
let print_usage () : unit =
  Printf.printf "Benchmark Mode:\n";
  Printf.printf
    "  %s benchmark [options] <compiled_json> <scheduler_config.json>\n\n"
    Sys.argv.(0);
  Printf.printf "Options:\n";
  Printf.printf
    "  -n, --num-runs N       Number of benchmark runs (default: 10)\n";
  Printf.printf
    "  -i, --iterations N     Max iterations per run (default: 1000)\n";
  Printf.printf
    "  -o, --output-dir DIR   Output directory for CSV files (default: .)\n";
  Printf.printf "\nExample:\n";
  Printf.printf
    "  %s benchmark -n 20 -i 5000 -o ./bench_results program.json config.json\n"
    Sys.argv.(0)
