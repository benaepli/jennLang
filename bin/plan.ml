type event_id = string

type client_op_spec =
  (* target_node, key, value *)
  | Write of (int * string * string)
  (* target_node, key *)
  | Read of (int * string)
  (* target_node *)
  | SimulateTimeout of int

type event_action =
  (* A client operation. This event is "complete"
     when the simulator receives the response. *)
  | ClientRequest of client_op_spec
  (* An instantaneous event. This is "complete"
     as soon as it's executed. *)
  | CrashNode of int
  | RecoverNode of int

type dependency = EventCompleted of event_id

(* The node in our dependency graph *)
type planned_event = {
  id : event_id;
  action : event_action;
  dependencies : dependency list;
}

(* The full plan is a list of nodes.
The execution order is determined by the dependencies. *)
type execution_plan = planned_event list

type event_status =
  | Pending (* Waiting on dependencies *)
  | Ready (* All dependencies met, ready to run *)
  | InProgress
  | Completed (* Done *)

module EventMap = Map.Make (String)

module PlanEngine = struct
  type t = {
    event_statuses : event_status EventMap.t;
    unmet_dependencies : int EventMap.t;
    reverse_dependencies : event_id list EventMap.t;
    event_lookup : planned_event EventMap.t;
  }

  let create (plan : execution_plan) : t =
    (* First pass: populate lookups and initial dependency counts *)
    let add_event acc event =
      let dep_count = List.length event.dependencies in
      let status = if dep_count = 0 then Ready else Pending in
      {
        acc with
        event_lookup = EventMap.add event.id event acc.event_lookup;
        unmet_dependencies =
          EventMap.add event.id dep_count acc.unmet_dependencies;
        event_statuses = EventMap.add event.id status acc.event_statuses;
      }
    in

    (* Second pass: Build the reverse dependency map *)
    let add_reverse_deps acc child_event =
      List.fold_left
        (fun acc (EventCompleted parent_id) ->
          (* Find existing children of the parent, or an empty list *)
          let children =
            EventMap.find_opt parent_id acc.reverse_dependencies
            |> Option.value ~default:[]
          in
          (* Add this child_event's ID to the parent's list of children *)
          {
            acc with
            reverse_dependencies =
              EventMap.add parent_id
                (child_event.id :: children)
                acc.reverse_dependencies;
          })
        acc child_event.dependencies
    in

    let empty_engine =
      {
        event_statuses = EventMap.empty;
        unmet_dependencies = EventMap.empty;
        reverse_dependencies = EventMap.empty;
        event_lookup = EventMap.empty;
      }
    in

    let with_events = List.fold_left add_event empty_engine plan in
    List.fold_left add_reverse_deps with_events plan

  (** Returns a list of all events that are currently ready, along with the
      updated engine with those events marked as in-progress. *)
  let get_ready_events (engine : t) : planned_event list * t =
    (* Mark all ready events as in progress and collect their IDs *)
    let ready_ids, new_statuses =
      EventMap.fold
        (fun id status (ids, statuses) ->
          if status = Ready then (id :: ids, EventMap.add id InProgress statuses)
          else (ids, statuses))
        engine.event_statuses
        ([], engine.event_statuses)
    in

    (* Use the lookup table to return the full planned_event records *)
    let events =
      List.map
        (fun id ->
          match EventMap.find_opt id engine.event_lookup with
          | Some event -> event
          | None ->
              failwith
                ("PlanEngine: Inconsistent state. Could not find event: " ^ id))
        ready_ids
    in

    (events, { engine with event_statuses = new_statuses })

  let mark_event_completed (engine : t) (id : event_id) : t =
    let new_statuses = EventMap.add id Completed engine.event_statuses in

    (* Update all children *)
    let children_ids =
      EventMap.find_opt id engine.reverse_dependencies
      |> Option.value ~default:[]
    in

    let new_deps, final_statuses =
      List.fold_left
        (fun (deps, statuses) child_id ->
          let current_count = EventMap.find child_id deps in
          let new_count = current_count - 1 in

          if new_count < 0 then
            failwith
              ("PlanEngine: Inconsistent state. Event " ^ child_id
             ^ " has negative dependencies.")
          else if new_count = 0 then
            (* Sanity check: ensure the child was pending *)
            match EventMap.find child_id statuses with
            | Pending ->
                ( EventMap.add child_id new_count deps,
                  EventMap.add child_id Ready statuses )
            | _ ->
                failwith
                  ("PlanEngine: Inconsistent state. Event " ^ child_id
                 ^ " became Ready but was not Pending.")
          else (EventMap.add child_id new_count deps, statuses))
        (engine.unmet_dependencies, new_statuses)
        children_ids
    in

    {
      engine with
      event_statuses = final_statuses;
      unmet_dependencies = new_deps;
    }

  let is_complete (engine : t) : bool =
    EventMap.for_all (fun _ status -> status = Completed) engine.event_statuses
end
