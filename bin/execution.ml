open Plan
open Mylib.Simulator

let src = Logs.Src.create "execution" ~doc:"Execution logs"

module Log = (val Logs.src_log src : Logs.LOG)

let default_env_size = 1024

type topology_info = { topology : string; num_servers : int }

(**
 * Creates and initializes an environment with formals, actuals, and locals.
 * 
 * @param env_size The size of the environment to create
 * @param node_env The node's environment (from state.nodes)
 * @param formals List of formal parameter names
 * @param actuals List of actual parameter values
 * @param locals List of (name, default_expr) pairs for local variables
 * @param context_name Optional context for error messages
 * @return The initialized environment
 *)
let create_initialized_env (env_size : int) (node_env : 'a Env.t)
    (formals : string list) (actuals : value list)
    (locals : (string * expr) list) ?(context_name : string = "function") () :
    'a Env.t =
  let env = Env.create env_size in

  (* Add formal parameters *)
  (try
     List.iter2 (fun formal actual -> Env.add env formal actual) formals actuals
   with Invalid_argument _ ->
     failwith
       (Format.asprintf "Mismatched arguments for %s: expected %d, got %d"
          context_name (List.length formals) (List.length actuals)));

  (* Evaluate and add local variables *)
  let record_env = { local_env = env; node_env } in
  List.iter
    (fun (var_name, default_expr) ->
      Env.add env var_name (eval record_env default_expr))
    locals;

  env

let recover_node_in_topology (topology : topology_info) (state : state)
    (prog : program) (node_id : int) continuation : unit =
  let recover_fn_name = "Node.RecoverInit" in

  if Env.mem prog.rpc recover_fn_name then (
    Log.info (fun m -> m "Found recover function: %s" recover_fn_name);
    let recover_fn = Env.find prog.rpc recover_fn_name in

    let actuals =
      match topology.topology with
      | "FULL" ->
          [
            VInt node_id;
            VList (List.init topology.num_servers (fun j -> VNode j));
          ]
      | _ -> failwith "Recovery not implemented for this topology"
    in

    let env =
      create_initialized_env default_env_size state.nodes.(node_id)
        recover_fn.formals actuals recover_fn.locals
        ~context_name:"Node.RecoverInit" ()
    in

    let record =
      {
        pc = recover_fn.entry;
        node = node_id;
        origin_node = node_id;
        continuation = (fun _ -> continuation ());
        env;
        id = -1;
        x = 0.0;
        f = (fun x -> x);
      }
    in
    exec state prog record)
  else continuation ()

let reinit_single_node (topology : topology_info) (state : state)
    (prog : program) (node_id : int) continuation : unit =
  let init_fn_name = "Node.BASE_NODE_INIT" in
  let init_fn = Env.find prog.rpc init_fn_name in

  let env =
    create_initialized_env default_env_size state.nodes.(node_id) [] []
      init_fn.locals ~context_name:init_fn_name ()
  in
  Env.add env "self" (VNode node_id);

  let record_env = { local_env = env; node_env = state.nodes.(node_id) } in
  let _ = exec_sync state prog record_env init_fn.entry in
  recover_node_in_topology topology state prog node_id continuation

let in_progress_client_ops : (int, event_id) Hashtbl.t = Hashtbl.create 10

let force_crash_node (state : state) (node_id : int) : unit =
  let ci = state.crash_info in

  (* Don't crash a node that is already crashed *)
  if BatSet.Int.mem node_id ci.currently_crashed then (
    Log.warn (fun m ->
        m "Request to crash node %d, which is already crashed." node_id);
    ())
  else (
    Log.debug (fun m ->
        m "Crash: Forcing crash of node %d at step %d" node_id ci.current_step);
    ci.currently_crashed <- BatSet.Int.add node_id ci.currently_crashed;

    (* Partition runnable and waiting records for the crashed node *)
    (* For DA, we need to extract the ones to crash and keep the rest *)
    let all_tasks_on_crashed_node = ref [] in
    let new_runnable = DA.create () in
    DA.iter
      (fun r ->
        if r.node = node_id then
          all_tasks_on_crashed_node := r :: !all_tasks_on_crashed_node
        else DA.add new_runnable r)
      state.runnable_records;
    state.runnable_records <- new_runnable;

    let waiting_tasks_on_crashed_node, remaining_waiting =
      List.partition (fun r -> r.node = node_id) state.waiting_records
    in

    state.waiting_records <- remaining_waiting;

    let all_pending_tasks =
      !all_tasks_on_crashed_node @ waiting_tasks_on_crashed_node
    in

    (* Partition tasks: queue external RPCs, drop local tasks *)
    let tasks_to_queue, tasks_to_drop =
      List.partition
        (fun r ->
          let is_external = r.origin_node <> r.node in
          let origin_is_alive =
            not (BatSet.Int.mem r.origin_node ci.currently_crashed)
          in
          is_external && origin_is_alive)
        all_pending_tasks
    in

    (* Queue the external tasks for redelivery upon recovery *)
    let messages_to_queue = List.map (fun r -> (node_id, r)) tasks_to_queue in

    Log.info (fun m ->
        m
          "Dropping %d local tasks and queuing %d external tasks from crashed \
           node %d"
          (List.length tasks_to_drop)
          (List.length messages_to_queue)
          node_id);

    ci.queued_messages <- messages_to_queue @ ci.queued_messages)

let force_recover_node (state : state) (prog : program)
    (topology : topology_info) (node_id : int) : unit =
  let ci = state.crash_info in

  (* Only recover nodes that are actually crashed *)
  if not (BatSet.Int.mem node_id ci.currently_crashed) then (
    Log.warn (fun m ->
        m "Request to recover node %d, which is not crashed." node_id);
    () (* Do nothing *))
  else (
    Log.debug (fun m ->
        m "Recover: Forcing recovery of node %d at step %d" node_id
          ci.current_step);

    (* Reset node to fresh state *)
    state.nodes.(node_id) <- Env.create 1024;

    (* Call the re-initialization logic *)
    reinit_single_node topology state prog node_id (fun () -> ());

    (* Remove from crashed list *)
    ci.currently_crashed <- BatSet.Int.remove node_id ci.currently_crashed;

    (* Find all queued messages for this node *)
    let queued_for_node, remaining_queue =
      List.partition (fun (dest, _) -> dest = node_id) ci.queued_messages
    in
    ci.queued_messages <- remaining_queue;

    Log.info (fun m ->
        m "Delivering %d queued messages to recovered node %d"
          (List.length queued_for_node)
          node_id);

    (* Add all queued messages back to the runnable list *)
    List.iter
      (fun (_, record) -> DA.add state.runnable_records record)
      queued_for_node)

(**
* Schedules a pre-planned client operation.
*
* @param operation_id_counter A ref to the global counter for unique IDs.
* @param event_id The string ID from the execution_plan.
* @param op_spec The (Write/Read/Timeout) action to perform.
* @param op_map A hashtable mapping internal int IDs back to event_ids.
* @param plan_engine A ref to the main plan engine.
*)
let schedule_planned_op (state : state) (program : program)
    (operation_id_counter : int ref) (* Must be passed from interp/exec_plan *)
    (event_id : event_id) (op_spec : client_op_spec) (client_id : int)
    (op_map : (int, event_id) Hashtbl.t) (plan_engine : PlanEngine.t ref) : unit
    =
  (* Generate a new internal operation ID *)
  let new_op_id =
    operation_id_counter := !operation_id_counter + 1;
    !operation_id_counter
  in

  (* Link internal ID to plan event ID *)
  Hashtbl.add op_map new_op_id event_id;

  let op_name, actuals =
    match op_spec with
    | Write (target_node, key, value) ->
        ( "ClientInterface.Write",
          [ VNode target_node; VString key; VString value ] )
    | Read (target_node, key) ->
        ("ClientInterface.Read", [ VNode target_node; VString key ])
    | SimulateTimeout target_node ->
        ("ClientInterface.SimulateTimeout", [ VNode target_node ])
  in

  let op = function_info op_name program in
  let env =
    create_initialized_env default_env_size state.nodes.(client_id) op.formals
      actuals op.locals ~context_name:op_name ()
  in

  (* Log the invocation to history *)
  let invocation =
    {
      client_id;
      op_action = op.name;
      kind = Invocation;
      payload = actuals;
      unique_id = new_op_id;
    }
  in
  DA.add state.history invocation;

  (* Important: create the new continuation *)
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
    DA.add state.history response;

    (* Hook to plan engine *)
    (* Find the event_id (string) from the internal unique_id (int) *)
    let completed_event_id =
      try Hashtbl.find op_map new_op_id
      with Not_found ->
        failwith
          ("schedule_planned_op: Could not find event_id for op_id "
         ^ string_of_int new_op_id)
    in

    (* Mark the event as complete in the engine *)
    plan_engine :=
      PlanEngine.mark_event_completed !plan_engine completed_event_id;

    Log.debug (fun m ->
        m " Completed client op %s (ID: %d)" completed_event_id new_op_id);

    (* Return the client to the free pool *)
    state.free_clients <- client_id :: state.free_clients;

    (* Remove from in-progress map *)
    Hashtbl.remove op_map new_op_id;
    ()
  in

  (* Create and schedule the new record *)
  let record =
    {
      pc = op.entry;
      node = client_id;
      origin_node = client_id;
      continuation = new_continuation;
      env;
      id = new_op_id;
      x = 0.4;
      f = (fun x -> x);
    }
  in
  DA.add state.runnable_records record
(* Log.debug (fun m -> m "Dispatched client op %s (ID: %d) on client %d" event_id
    new_op_id client_id *)

let exec_plan (state : state) (program : program) (plan : execution_plan)
    (max_iterations : int) (topology : topology_info)
    (operation_id_counter : int ref) (randomly_delay_msgs : bool) : unit =
  Hashtbl.clear in_progress_client_ops;
  let plan_engine = ref (PlanEngine.create plan) in
  let current_step = ref 0 in
  let deferred_ops = ref [] in

  (* This is the main loop. It runs as long as the plan isn't
     complete, or until we hit the safety timeout. *)
  while
    (not (PlanEngine.is_complete !plan_engine))
    && !current_step < max_iterations
  do
    (* Dispatch all ready events from the plan *)
    let ready_events, new_engine = PlanEngine.get_ready_events !plan_engine in
    plan_engine := new_engine;

    List.iter
      (fun (event : planned_event) ->
        Log.debug (fun m ->
            m "Step %d: Dispatching event %s" !current_step event.id);
        match event.action with
        | ClientRequest op_spec -> (
            match state.free_clients with
            | [] ->
                (* defer this event *)
                Log.debug (fun m ->
                    m "Step %d: Deferring event %s (no free clients)"
                      !current_step event.id);
                deferred_ops := event :: !deferred_ops;

                (* Mark it back to Ready in the engine *)
                plan_engine := PlanEngine.mark_as_ready !plan_engine event.id
            | client_id :: rest_of_clients ->
                Log.debug (fun m ->
                    m "STEP %d: Dispatching event %s" !current_step event.id);
                (* Consume the client *)
                state.free_clients <- rest_of_clients;

                schedule_planned_op state program operation_id_counter event.id
                  op_spec client_id in_progress_client_ops plan_engine)
        | CrashNode node_id ->
            force_crash_node state node_id;
            (* This is an instantaneous event, so mark it complete immediately *)
            plan_engine := PlanEngine.mark_event_completed !plan_engine event.id
        | RecoverNode node_id ->
            force_recover_node state program topology node_id;
            plan_engine := PlanEngine.mark_event_completed !plan_engine event.id)
      ready_events;

    (* Run the Simulator's async "network" for one step *)
    if DA.length state.runnable_records > 0 then
      schedule_record state program false false false []
        randomly_delay_msgs (* Disable all random fault injection *)
    else
      (* If nothing is runnable, we just advance the plan.
         This can happen if we are waiting for a client op to be dispatched. *)
      ();

    current_step := !current_step + 1
  done;

  Log.info (fun m -> m "Execution plan completed in %d steps." !current_step);
  if !current_step >= max_iterations then
    Log.warn (fun m ->
        m "Hit max iterations (%d) before plan completion." max_iterations)
