open Yojson.Basic.Util

type crash_config = {
  enable_random_crashes : bool;
  crash_probability : float;
      (* per step, per node (e.g., 0.001 = 0.1% chance) *)
  min_recovery_time : int; (* minimum steps before recovery *)
  max_recovery_time : int; (* maximum steps before recovery *)
}

type config = {
  randomly_drop_msgs : bool;
  cut_tail_from_mid : bool;
  sever_all_to_tail_but_mid : bool;
  partition_away_nodes : int list;
  randomly_delay_msgs : bool;
  crash_config : crash_config;
}

let read_config_file (filename : string) : config =
  let json = Yojson.Basic.from_file filename in
  let crash_json = json |> member "crash_config" in
  {
    randomly_drop_msgs = json |> member "randomly_drop_msgs" |> to_bool;
    cut_tail_from_mid = json |> member "cut_tail_from_mid" |> to_bool;
    sever_all_to_tail_but_mid =
      json |> member "sever_all_to_tail_but_mid" |> to_bool;
    partition_away_nodes =
      json |> member "partition_away_nodes" |> to_list |> filter_int;
    randomly_delay_msgs = json |> member "randomly_delay_msgs" |> to_bool;
    crash_config =
      {
        enable_random_crashes =
          crash_json |> member "enable_random_crashes" |> to_bool;
        crash_probability = crash_json |> member "crash_probability" |> to_float;
        min_recovery_time = crash_json |> member "min_recovery_time" |> to_int;
        max_recovery_time = crash_json |> member "max_recovery_time" |> to_int;
      };
  }
