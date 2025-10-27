open Simulator

(* Helper to convert a value to its string representation for the CSV *)
let string_of_value_for_csv (v : value) : string =
  match v with
  | VInt i -> string_of_int i
  | VBool b -> string_of_bool b
  | VString s -> s (* The Csv library will handle any necessary quoting *)
  | VNode n -> string_of_int n
  | VFuture _ -> "TODO implement VFuture"
  | VMap _ -> "TODO implement VMap"
  | VOption _ -> "TODO implement VOptions"
  | VList _ -> "TODO implement VList"
  | VUnit -> "TODO implement VUnit"
  | VTuple _ -> "TODO implement VTuple"

(* Saves the simulation history to a CSV file *)
let save_history_to_csv (history : operation DA.t) (filename : string) : unit =
  (* Find the maximum payload length to create a non-ragged CSV *)
  let max_payload_len =
    DA.fold_left
      (fun max_len op -> max max_len (List.length op.payload))
      0 history
  in

  (* Create a dynamic header based on the max payload size *)
  let payload_headers =
    List.init max_payload_len (fun i -> "Payload" ^ string_of_int (i + 1))
  in
  let header = [ "ClientID"; "Kind"; "Action" ] @ payload_headers in

  (* Map all history operations to a list of string lists (rows) *)
  let rows =
    DA.fold_left
      (fun acc op ->
        let client_id = string_of_int op.client_id in
        let kind =
          match op.kind with
          | Response -> "Response"
          | Invocation -> "Invocation"
        in
        let action = op.op_action in
        let base_row = [ client_id; kind; action ] in

        (* Convert payload to list of strings using the helper *)
        let payload_strings = List.map string_of_value_for_csv op.payload in

        (* Pad the row with empty strings to match the header width *)
        let padding =
          List.init
            (max_payload_len - List.length payload_strings)
            (fun _ -> "")
        in
        let full_row = base_row @ payload_strings @ padding in

        (* Prepend the new row to the accumulator *)
        full_row :: acc)
      [] history
  in

  (* Combine header and data (and reverse data rows) into a single list *)
  (* Rows are in reverse order from fold_left, so List.rev is needed *)
  let all_rows = header :: List.rev rows in

  Csv.save filename all_rows
