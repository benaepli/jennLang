{
  (* open Core *)
  open Lexing
  open Parser

  exception LexError of string

  let next_line lexbuf =
    let pos = lexbuf.lex_curr_p in
    lexbuf.lex_curr_p <-
      { pos with pos_bol = lexbuf.lex_curr_pos;
        pos_lnum = pos.pos_lnum + 1
      }
(*

  module StringMap = Map.Make(String)
  let keyword_table = StringMap.of_alist_exn [
    "func", FUNC;
    "if", IF;
    "elseif", ELSEIF;
    "else", ELSE;
    "map", MAP;
    "Options", OPTIONS;
    "return", RETURN;
    "rpc_call", RPC_CALL;
    (* "true", true;
    "false", false; *)
  ]


  let find_token s =
    match StringMap.find_opt s keyword_table with
    | Some kw -> kw
    | None -> ID s *)

}

let white = [' ' '\t']+
let newline = '\r' | '\n' | "\r\n"
let id = ['a'-'z' 'A'-'Z' '_'] ['a'-'z' 'A'-'Z' '0'-'9' '_']*
let digit = ['0'-'9']
let integer = digit+

rule comment = parse
  | "*/" { token lexbuf }
  | _ { comment lexbuf }

and read_string buf = parse
  | '"' { STRING (Buffer.contents buf) }
  | '\\' '"' { Buffer.add_char buf '"'; read_string buf lexbuf }
  | '\\' '\\' { Buffer.add_char buf '\\'; read_string buf lexbuf }
  | '\\' 'n' { Buffer.add_char buf '\n'; read_string buf lexbuf }
  | '\\' 't' { Buffer.add_char buf '\t'; read_string buf lexbuf }
  | newline { next_line lexbuf; Buffer.add_char buf '\n'; read_string buf lexbuf }
  | eof { raise (LexError "String is not terminated") }
  | _ as c { Buffer.add_char buf c; read_string buf lexbuf }

and token = parse
  | white { token lexbuf }
  | newline { next_line lexbuf; token lexbuf }
  | "//" [^ '\n']* { token lexbuf } (* ignore comments *)
  | "/*" { comment lexbuf }
  | '"' { read_string (Buffer.create 17) lexbuf }
  | "->" { ARROW }
  | ',' { COMMA }
  | '=' { EQUALS }
  | integer as i { INT (int_of_string i)}
  | '<' {LEFT_ANGLE_BRACKET}
  | "<=" {LEFT_ANGLE_BRACKET_EQUALS}
  | '>' {RIGHT_ANGLE_BRACKET}
  | ">=" {RIGHT_ANGLE_BRACKET_EQUALS}
  | '!' {BANG}
  | '{' {LEFT_CURLY_BRACE}
  | '}' {RIGHT_CURLY_BRACE}
  | '(' {LEFT_PAREN}
  | ')' {RIGHT_PAREN}
  | '[' {LEFT_SQUARE_BRACKET}
  | ']' {RIGHT_SQUARE_BRACKET}
  | '+' {PLUS}
  | '-' {MINUS}
  | "*" {STAR}
  | '/' {SLASH}
  | '%' {PERCENT}
  (*| '.' {DOT}*)
  | ':' {COLON}
  | ';' {SEMICOLON}
  | "==" {EQUALS_EQUALS}
  | "!=" {NOT_EQUALS}
  | "&&" {AND}
  | "||" {OR}
  | "await" { AWAIT }
  | "append" { APPEND }
  | "break" { BREAK }
  | "case" { CASE }
  | "default" { DEFAULT }
  | "else if" { ELSEIF }
  | "else" { ELSE }
  | "exists" { EXISTS }
  | "false" { FALSE false }
  | "for" { FOR }
  | "func" { FUNC }
  | "hd" { HEAD }
  | "if" { IF }
  | "in" { IN }
  | "len" { LEN }
  | "map" { MAP }
  | "match" { MATCH }
  | "min" { MIN }
  | "Options" { OPTIONS }
  | "prepend" { PREPEND }
  | "print" { PRINT }
  | "poll_for_resps" { POLL_FOR_RESPS }
  | "has_next_resp" { POLL_FOR_ANY_RESP }
  | "next_resp" { NEXT_RESP}
  | "set_timeout" { SET_TIMEOUT }
  | "return" { RETURN }
  | "rpc_async_call" { RPC_ASYNC_CALL}
  | "rpc_call" { RPC_CALL }
  | "tl" { TAIL }
  | "true" { TRUE true }
  | "var" { VAR }
  | "ClientInterface" { CLIENT_INTERFACE }
  | id as s { ID s }
  | eof { EOF }
  (* | _ as c
    { raise (SyntaxError ("Unexpected character: " ^ Char.escaped c)) } *)
