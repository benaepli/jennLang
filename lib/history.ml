open Simulator

(* Helper to convert a value to its Yojson representation *)
let rec json_of_value (v : value) : Yojson.Basic.t =
  match v with
  | VInt i -> `Assoc [ ("type", `String "VInt"); ("value", `Int i) ]
  | VBool b -> `Assoc [ ("type", `String "VBool"); ("value", `Bool b) ]
  | VString s -> `Assoc [ ("type", `String "VString"); ("value", `String s) ]
  | VNode n -> `Assoc [ ("type", `String "VNode"); ("value", `Int n) ]
  | VFuture f ->
      let value_json =
        match !f with Some v -> json_of_value v | None -> `Null
      in
      `Assoc [ ("type", `String "VFuture"); ("value", value_json) ]
  | VMap m ->
      let pairs = ValueMap.fold (fun k v acc -> (k, v) :: acc) m [] in
      (* Represent map as an array of [key, value] pairs *)
      let json_pairs =
        List.map
          (fun (k, v) -> `List [ json_of_value k; json_of_value v ])
          pairs
      in
      `Assoc [ ("type", `String "VMap"); ("value", `List json_pairs) ]
  | VOption o ->
      let value_json =
        match o with Some v -> json_of_value v | None -> `Null
      in
      `Assoc [ ("type", `String "VOption"); ("value", value_json) ]
  | VList l ->
      let items_json = List.map json_of_value !l in
      `Assoc [ ("type", `String "VList"); ("value", `List items_json) ]
  | VUnit -> `Assoc [ ("type", `String "VUnit"); ("value", `Null) ]
  | VTuple t ->
      let items_json = Array.to_list t |> List.map json_of_value in
      `Assoc [ ("type", `String "VTuple"); ("value", `List items_json) ]
  | VLock r ->
      let value_json = `Bool !r in
      `Assoc [ ("type", `String "VLock"); ("value", value_json) ]

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
  let header = [ "UniqueID"; "ClientID"; "Kind"; "Action" ] @ payload_headers in
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
        let unique_id = string_of_int op.unique_id in
        let base_row = [ unique_id; client_id; kind; action ] in

        (* Convert payload to list of JSON strings using the new helper *)
        let payload_strings =
          List.map
            (fun v -> json_of_value v |> Yojson.Basic.to_string)
            op.payload
        in

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
