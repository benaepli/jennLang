open Plan

type generator_config = {
  num_servers : int;
  num_clients : int;
  (* Client operations *)
  num_write_ops : int;
  num_read_ops : int;
  num_timeouts : int;
  (* Fault specs*)
  num_crashes : int; (* Number of crash/recover pairs *)
  (* Dependency specs *)
  (* Probability (0.0 to 1.0) that any event will
     depend on a random preceding event. *)
  dependency_density : float;
}

val generate_plan : generator_config -> execution_plan
