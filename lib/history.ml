open Simulator
open Sqlite3

(* Helper to convert a value to its Yojson representation *)
let rec json_of_value (v : value) : Yojson.Basic.t =
  match v with
  | VInt i -> `Assoc [ ("type", `String "VInt"); ("value", `Int i) ]
  | VBool b -> `Assoc [ ("type", `String "VBool"); ("value", `Bool b) ]
  | VString s -> `Assoc [ ("type", `String "VString"); ("value", `String s) ]
  | VNode n -> `Assoc [ ("type", `String "VNode"); ("value", `Int n) ]
  | VFuture f ->
      let value_json =
        match f.value with Some v -> json_of_value v | None -> `Null
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
  (* Create header with single Payload column *)
  let header = [ "UniqueID"; "ClientID"; "Kind"; "Action"; "Payload" ] in

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

        (* Convert entire payload to a single JSON array string *)
        let payload_json_array =
          List.map json_of_value op.payload |> fun json_list ->
          `List json_list |> Yojson.Basic.to_string
        in

        let full_row = base_row @ [ payload_json_array ] in

        (* Prepend the new row to the accumulator *)
        full_row :: acc)
      [] history
  in

  (* Combine header and data (and reverse data rows) into a single list *)
  (* Rows are in reverse order from fold_left, so List.rev is needed *)
  let all_rows = header :: List.rev rows in

  Csv.save filename all_rows

(* Initialize the SQLite database with the required tables *)
let init_sqlite (db : db) : unit =
  let create_runs =
    "CREATE TABLE IF NOT EXISTS runs ( run_id INTEGER PRIMARY KEY, start_time \
     DATETIME DEFAULT CURRENT_TIMESTAMP, meta_info TEXT );"
  in
  let create_executions =
    "CREATE TABLE IF NOT EXISTS executions ( run_id INTEGER REFERENCES \
     runs(run_id), unique_id INTEGER, client_id INTEGER, kind TEXT, action \
     TEXT, payload JSON );"
  in
  let create_index =
    "CREATE INDEX IF NOT EXISTS idx_run_execution ON executions(run_id, \
     unique_id);"
  in
  ignore (exec db create_runs);
  ignore (exec db create_executions);
  ignore (exec db create_index)

(* Saves the simulation history to the SQLite database *)
let save_history (db : db) (run_id : int) (history : operation DA.t) : unit =
  (* Insert run record *)
  let insert_run =
    Printf.sprintf "INSERT INTO runs (run_id) VALUES (%d);" run_id
  in
  ignore (exec db insert_run);

  (* Bulk insert executions *)
  ignore (exec db "BEGIN TRANSACTION;");
  let stmt =
    prepare db
      "INSERT INTO executions (run_id, unique_id, client_id, kind, action, \
       payload) VALUES (?, ?, ?, ?, ?, ?)"
  in

  DA.iter
    (fun op ->
      ignore (reset stmt);
      ignore (bind stmt 1 (Sqlite3.Data.INT (Int64.of_int run_id)));
      ignore (bind stmt 2 (Sqlite3.Data.INT (Int64.of_int op.unique_id)));
      ignore (bind stmt 3 (Sqlite3.Data.INT (Int64.of_int op.client_id)));

      let kind =
        match op.kind with Response -> "Response" | Invocation -> "Invocation"
      in
      ignore (bind stmt 4 (Sqlite3.Data.TEXT kind));

      ignore (bind stmt 5 (Sqlite3.Data.TEXT op.op_action));

      let payload_json_array =
        List.map json_of_value op.payload |> fun json_list ->
        `List json_list |> Yojson.Basic.to_string
      in
      ignore (bind stmt 6 (Sqlite3.Data.TEXT payload_json_array));

      ignore (step stmt))
    history;

  ignore (finalize stmt);
  ignore (exec db "COMMIT;")
