open Yojson.Basic.Util

type range = { min : int; max : int; step : int }

(** Helper to parse a range from JSON, defaulting step to 1 *)
let read_range json =
  let min = json |> member "min" |> to_int in
  let max = json |> member "max" |> to_int in
  let step =
    json |> member "step" |> to_int_option |> Option.value ~default:1
  in
  if min > max || step <= 0 then
    failwith "Invalid range: min must be <= max and step must be > 0"
  else { min; max; step }

type config = {
  num_servers_range : range;
  num_clients_range : range;
  num_write_ops_range : range;
  num_read_ops_range : range;
  num_timeouts_range : range;
  num_crashes_range : range;
  dependency_density_values : float list;
  randomly_delay_msgs : bool;
  num_runs_per_config : int;
  max_iterations : int;
}

type single_run_config = {
  plan_gen_config : PlanGenerator.generator_config;
  randomly_delay_msgs : bool;
}

let read_config_file (filename : string) : config =
  let json = Yojson.Basic.from_file filename in
  {
    num_servers_range = json |> member "num_servers" |> read_range;
    num_clients_range = json |> member "num_clients" |> read_range;
    num_write_ops_range = json |> member "num_write_ops" |> read_range;
    num_read_ops_range = json |> member "num_read_ops" |> read_range;
    num_timeouts_range = json |> member "num_timeouts" |> read_range;
    num_crashes_range = json |> member "num_crashes" |> read_range;
    dependency_density_values =
      json |> member "dependency_density" |> to_list |> filter_float;
    randomly_delay_msgs =
      json
      |> member "randomly_delay_msgs"
      |> to_bool_option
      |> Option.value ~default:false;
    num_runs_per_config = json |> member "num_runs_per_config" |> to_int;
    max_iterations = json |> member "max_iterations" |> to_int;
  }
