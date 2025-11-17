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

let () = Random.self_init ()

type action_stub =
  | Single of event_action
  | Paired of (event_action * event_action)
(* e.g., Crash followed by Recover *)

(** Generates a bag of action stubs based on the config *)
let generate_base_actions (config : generator_config) : action_stub list =
  let actions = ref [] in

  let rand_server () = Random.int config.num_servers in
  let rand_key () = Printf.sprintf "key%d" (Random.int 3 + 1) in
  (* e.g., key1, key2, key3 *)
  let rand_val () = Printf.sprintf "val%d" (Random.int 100) in

  (* Client Ops *)
  for _ = 1 to config.num_write_ops do
    let action = Write (rand_server (), rand_key (), rand_val ()) in
    actions := Single (ClientRequest action) :: !actions
  done;

  for _ = 1 to config.num_read_ops do
    let action = Read (rand_server (), rand_key ()) in
    actions := Single (ClientRequest action) :: !actions
  done;

  for _ = 1 to config.num_timeouts do
    let action = SimulateTimeout (rand_server ()) in
    actions :=
      Single (ClientRequest action)
      :: !actions (* A timeout is a client request *)
  done;

  (* Faults *)
  for _ = 1 to config.num_crashes do
    let s = rand_server () in
    actions := Paired (CrashNode s, RecoverNode s) :: !actions
  done;

  !actions

type intermediate_event = {
  id : event_id;
  action : event_action;
  (* What key does this op touch? (key, Read/Write) *)
  key_op : (string * [ `R | `W ]) option;
  (* Is this part of a pair? (group_id, First/Second) *)
  pair_group : (int * [ `First | `Second ]) option;
}

let shuffle_list l =
  let arr = Array.of_list l in
  let n = Array.length arr in
  for i = n - 1 downto 1 do
    let j = Random.int (i + 1) in
    let tmp = arr.(i) in
    arr.(i) <- arr.(j);
    arr.(j) <- tmp
  done;
  Array.to_list arr

(** * Converts the list of action stubs into a flat, shuffled list of *
    intermediate_events. *)
let create_intermediate_list (stubs : action_stub list) :
    intermediate_event list =
  let event_id_counter = ref 0 in
  let pair_group_counter = ref 0 in

  (* Helper to process a single stub and add its event(s) to the accumulator *)
  let process_stub acc stub =
    match stub with
    | Single action ->
        event_id_counter := !event_id_counter + 1;
        let id = Printf.sprintf "e%d" !event_id_counter in

        (* Extract key operation metadata *)
        let key_op =
          match action with
          | ClientRequest (Write (_, k, _)) -> Some (k, `W)
          | ClientRequest (Read (_, k)) -> Some (k, `R)
          | _ -> None
        in

        let event = { id; action; key_op; pair_group = None } in
        event :: acc (* Prepend to accumulator *)
    | Paired (action1, action2) ->
        (* Create two events for the pair *)
        event_id_counter := !event_id_counter + 1;
        let id1 = Printf.sprintf "e%d" !event_id_counter in

        event_id_counter := !event_id_counter + 1;
        let id2 = Printf.sprintf "e%d" !event_id_counter in

        pair_group_counter := !pair_group_counter + 1;
        let group_id = !pair_group_counter in

        let event1 =
          {
            id = id1;
            action = action1;
            key_op = None;
            pair_group = Some (group_id, `First);
          }
        in

        let event2 =
          {
            id = id2;
            action = action2;
            key_op = None;
            pair_group = Some (group_id, `Second);
          }
        in

        (* Add both to the accumulator *)
        event2 :: event1 :: acc
  in

  let intermediate_events = List.fold_left process_stub [] stubs in

  (*
   * Shuffle the list. This creates a randomized base ordering.
   *)
  shuffle_list intermediate_events

(* Iterates through the shuffled list of intermediate events,
 * adding both logical and probabilistic dependencies.
 *
 * @param config The generator config (for dependency_density).
 * @param events The shuffled list from create_intermediate_list.
 * @return A final, valid execution_plan.
 *)
let finalize_plan (config : generator_config) (events : intermediate_event list)
    : execution_plan =
  (* Build a map of {pair_group_id -> crash_event_id} *)
  let crash_pair_map =
    List.fold_left
      (fun map event ->
        match event.pair_group with
        | Some (group_id, `First) ->
            EventMap.add (string_of_int group_id) event.id map
        | _ -> map)
      EventMap.empty events
  in

  (* We iterate through the list, building our final plan. *)
  (* `seen_events`: A list of all events we've processed so far. *)
  (* `last_write_map`: A map of {key -> last_write_event_id} *)
  let final_plan_rev, _, _ =
    List.fold_left
      (fun (built_plan_rev, seen_events, last_write_map)
           (current_event : intermediate_event) ->
        let dependencies = ref [] in

        (* Enforcing logical dependencies *)
        (match current_event.pair_group with
        | Some (group_id, `Second) -> (
            try
              let crash_id =
                EventMap.find (string_of_int group_id) crash_pair_map
              in
              dependencies := EventCompleted crash_id :: !dependencies
            with Not_found ->
              failwith
                "PlanGenerator: Inconsistent state. Recover has no matching \
                 Crash.")
        | _ -> ());

        (* Read/write coherence *)
        (match current_event.key_op with
        | Some (key, `R) -> (
            match EventMap.find_opt key last_write_map with
            | Some write_id ->
                dependencies := EventCompleted write_id :: !dependencies
            | None -> ())
        | _ -> ());

        (* Probabilistic *)
        List.iter
          (fun (prev_event : intermediate_event) ->
            (* Roll the die for every preceding event *)
            let is_a_cycle =
              match (current_event.pair_group, prev_event.pair_group) with
              | Some (gid_curr, `First), Some (gid_prev, `Second) ->
                  gid_curr = gid_prev
              | Some (gid_curr, `Second), Some (gid_prev, `First) ->
                  gid_curr = gid_prev
              | _ -> false
            in

            if (not is_a_cycle) && Random.float 1.0 < config.dependency_density
            then
              let dep = EventCompleted prev_event.id in
              (* Avoid adding a duplicate dependency *)
              if not (List.mem dep !dependencies) then
                dependencies := dep :: !dependencies)
          seen_events;

        let new_planned_event =
          {
            id = current_event.id;
            action = current_event.action;
            dependencies = !dependencies;
          }
        in

        let new_seen_events = current_event :: seen_events in

        let new_last_write_map =
          match current_event.key_op with
          | Some (key, `W) -> EventMap.add key current_event.id last_write_map
          | _ -> last_write_map
        in

        ( new_planned_event :: built_plan_rev,
          new_seen_events,
          new_last_write_map ))
      ([], [], EventMap.empty) events
  in

  (* Un-reverse the list to get the original shuffled order *)
  List.rev final_plan_rev

(** Generates a single, randomized execution_plan based on the configuration. *)
let generate_plan (config : generator_config) : execution_plan =
  config |> generate_base_actions |> create_intermediate_list
  |> finalize_plan config
