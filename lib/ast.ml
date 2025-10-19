(* open Core *)

type option = Option of string

type type_def =
  | CustomType of string
  | MapType of type_def * type_def
  (* 
type lhs =
  | VarLHS of string
  | MapAccessLHS of string * string *)

and collection_access = CollectionAccess of rhs * rhs

and rhs = 
  | VarRHS of string
  | Bool of bool
  | Not of rhs
  | And of rhs * rhs
  | Or of rhs * rhs
  | EqualsEquals of rhs * rhs
  | NotEquals of rhs * rhs
  | LessThan of rhs * rhs
  | LessThanEquals of rhs * rhs
  | GreaterThan of rhs * rhs
  | GreaterThanEquals of rhs * rhs
  | KeyExists of rhs * rhs
  | CollectionAccessRHS of collection_access
  | FuncCallRHS of func_call
  | LiteralRHS of literal 
  | FieldAccessRHS of rhs * string
  | CollectionRHS of collection
  | RpcCallRHS of rpc_call
  | Head of rhs
  | Tail of rhs
  | Len of rhs
  | ListAccess of rhs * int
  | Plus of rhs * rhs
  | Minus of rhs * rhs
  | Times of rhs * rhs
  | Div of rhs * rhs
  | Mod of rhs * rhs
  | PollForResps of rhs * rhs
  | PollForAnyResp of rhs
  | NextResp of rhs
  | Min of rhs * rhs
  | SetTimeout

and collection = 
  | MapLit of (rhs * rhs) list
  | ListLit of rhs list
  | ListPrepend of rhs * rhs
  | ListAppend of rhs * rhs
  | ListSubsequence of rhs * rhs * rhs

and param = Param of rhs 

and literal = 
  | Options of option list
  | String of string
  | Int of int

and func_call = FuncCall of string * param list

and rpc_call = 
  | RpcCall of string * func_call
  | RpcAsyncCall of string * func_call

and lhs = 
  | VarLHS of string
  | CollectionAccessLHS of collection_access 
  | FieldAccessLHS of rhs * string
  | TupleLHS of string list

(* list of condition to be evaluated * statement body, else condition is just true*)
type cond_stmt = IfElseIf of rhs * statement list 

and case_stmt = 
  | CaseStmt of rhs * statement list
  | DefaultStmt of statement list

and assignment = Assignment of lhs * rhs

and statement =
  | CondList of cond_stmt list 
  | VarDeclInit of string * rhs
  | AssignmentStmt of assignment
  | Expr of rhs
  | Return of rhs
  | ForLoop of assignment * rhs * assignment * statement list
  | ForLoopIn of lhs * rhs * statement list
  | Comment
  | Await of rhs
  | Print of rhs
  | Match of rhs * case_stmt list
  | BreakStmt 

type func_def = FuncDef of func_call * type_def * statement list

type var_init = VarInit of type_def * string * rhs

type role_def = RoleDef of string * param list * var_init list * func_def list

type client_def = ClientDef of var_init list * func_def list

type prog = Prog of role_def * client_def