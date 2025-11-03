import csv
import sys
import json
from enum import Enum
import argparse

from z3 import *


# TODO support inv calls without paired resp


class CallType(Enum):
    INV = True
    RESP = False


class Command(str):
    OK = "OK"
    WRITE = "WRITE"
    READ = "READ"


TimestampType = int
KeyType = int
ValueType = int
ProcessType = str

unique_id = 0


def get_unique_id():
    global unique_id
    unique_id += 1
    return unique_id


class ConstraintPair:
    def __init__(self, op1, op2):
        self.less_than = op1
        self.greater_than = op2


class Operation:
    def __init__(self, proc, cmd, inv, key, val=None):
        self.proc = proc
        self.cmd = cmd
        self.key = key
        self.inv = inv
        self.resp = None
        self.unique_id = unique_id
        self.val = val
        self.unique_id = get_unique_id()

    def set_resp(self, resp):
        self.resp = resp

    def __str__(self):
        return "{0}_{1}_{2}_proc{3}_{4}".format(
            self.cmd, self.key, self.val, self.proc, self.unique_id
        )
        # return "cmd: {0}, key: {1}, inv: {2}, resp: {3}, val: {4}" \
        #     .format(self.cmd, self.key, self.inv, self.resp, self.val)


class Action:
    def __init__(self, proc, call, cmd, k=None, val=None):
        self.proc = proc
        self.call = call
        self.cmd = cmd
        self.k = k
        self.val = val

    def __str__(self):
        return "Action(proc={0},call={1},cmd={2},k={3},val={4})".format(
            self.proc, self.call, self.cmd, self.k, self.val
        )


class ConstraintsGenerator:
    def __init__(self):
        self.predecessors = {}  # map of operations to set of predecessors
        self.successors = {}  # map of operations to set of successors
        self.matches = {}  # map of read operation to write operation
        self.alreadyUNSAT = False  # already found an unsatisfiable condition
        # self.already_matched = set()

    def __str__(self):
        result = ""
        for op in self.predecessors.keys():
            result += (
                str([str(p) for p in self.predecessors[op]])
                + " <-- "
                + str(op)
                + " --> "
                + str([str(s) for s in self.successors[op]])
                + "\n"
            )
        result += "\n"
        for read_op, write_op in self.matches.items():
            result += "Match(" + str(read_op) + ", " + str(write_op) + ")\n"

        return result

    def enforce_realtime_order(self, actions):
        """
        resp(op_i) < inv(op_j) -> op_i < op_j
        (op_i < op_j) /\ (op_j < op_k) -> op_i < op_k
        :param actions: list of invocations and responses
        :return: None
        """

        proc2Op = {}  # maps process to operation
        for i in range(len(actions)):
            print("i", i)
            timestamp = i
            action = actions[timestamp]
            print("action", action)
            if action.call == CallType.INV:
                if action.cmd == Command.WRITE:
                    proc2Op[action.proc] = Operation(
                        action.proc, action.cmd, timestamp, action.k, val=action.val
                    )
                elif action.cmd == Command.READ:
                    proc2Op[action.proc] = Operation(
                        action.proc, action.cmd, timestamp, action.k
                    )

            elif action.call == CallType.RESP:
                print("action.proc", action.proc)
                # Handle cases where a response might appear without a known invocation
                if action.proc not in proc2Op:
                    print(
                        f"Warning: Response from proc {action.proc} without matching invocation. Skipping."
                    )
                    continue

                op = proc2Op[action.proc]
                op.set_resp(timestamp)
                if op.cmd == Command.READ:
                    op.val = action.val
                del proc2Op[action.proc]

                self.successors[op] = set()
                for k in self.successors.keys():
                    if k == op:
                        continue

                    if k.resp < op.inv:
                        self.successors[k].add(op)

                self.predecessors[op] = set()
                for k in self.predecessors.keys():
                    if k == op:
                        continue

                    if k.resp < op.inv:
                        self.predecessors[op].add(k)

        # take care of operations that have invocations but not responses
        print(len(proc2Op))
        i = len(actions)
        for proc, op in proc2Op.items():

            print("proc", proc, "op", op)
            # a schedule with open reads is equivalent to the schedule
            # with the open reads removed.
            if op.cmd == Command.READ:
                continue

            timestamp = i
            i = i + 1
            if op.cmd == Command.WRITE:
                op.set_resp(timestamp)

            self.successors[op] = set()
            for k in self.successors.keys():
                if k == op:
                    continue

                if k.resp < op.inv:
                    self.successors[k].add(op)

            self.predecessors[op] = set()
            for k in self.predecessors.keys():
                if k == op:
                    continue

                if k.resp < op.inv:
                    self.predecessors[op].add(k)

    def match_read(self, read_op, ops):
        """
        Matches a read to a write operation.
        (op_i \in Writes) /\ (op_j \in Reads) /\
        (op_i.wval = op_j.rval) /\
        (op_i.inv <= op_j.resp) <-> match(op_i, op_j)
        :param read_op: the read to be matched
        :param ops: list of potential operators
        :return: bool, if there exists a match or not
        """

        for op in ops:
            # if op in self.already_matched:
            # continue
            if (
                op.cmd == Command.WRITE
                and op.val == read_op.val
                and op.inv <= read_op.resp
                and op.key == read_op.key
            ):
                self.matches[read_op] = op
                self.successors[op].add(read_op)
                self.predecessors[read_op].add(op)
                # self.already_matched.add(read_op)
                # self.already_matched.add(op)
                return True

        self.alreadyUNSAT = True
        print("UNSAT, no matching write for", str(read_op))
        return False

    def match_all_reads(self):
        """
        MatchAllReads (match(op_i, op_j) -> op_i < op_j) /\
        (every read matches some write)
        :return:
        """
        ops = self.successors.keys()

        for op in ops:
            if op.cmd == Command.READ and not self.match_read(op, ops):
                # some read has no match
                self.alreadyUNSAT = True
                return

    def order_concurrent_writes(self, op_rx, succs):
        """
        match(op_wa, op_rx) /\ match(op_wb, op_ry) /\ (op_rx < op_ry) -> op_wa < op_wb
        :param op_rx: starting point read operation
        :param succs: list of successor operations for op_rx
        :return: None
        """

        for op_ry in succs:
            if op_ry.cmd == Command.READ:
                if op_rx not in self.matches or op_ry not in self.matches:
                    continue

                op_wa = self.matches[op_rx]
                op_wb = self.matches[op_ry]

                # Don't add self-loop if both reads match the same write
                if op_wa == op_wb:
                    continue

                self.successors[op_wa].add(op_wb)
                self.predecessors[op_wb].add(op_wa)

    def concurrent_writes_ordered_by_reads(self):
        """
        match(op_wa, op_rx) /\ match(op_wb, op_ry) /\ (op_rx < op_ry) -> op_wa < op_wb
        :return: None
        """
        for op, succs in self.successors.items():
            if op.cmd == Command.READ:
                self.order_concurrent_writes(op, succs)

    def generate_constraints(self, actions):

        self.enforce_realtime_order(actions)
        self.match_all_reads()
        if self.alreadyUNSAT:
            return False

        self.concurrent_writes_ordered_by_reads()

        return True


def z3solver(cg):
    solver = Solver()
    solver.set(unsat_core=True)
    symbols = {}

    successors = cg.successors
    for op, succs in successors.items():
        print("op", op)
        if op not in symbols:
            symbols[op] = Int(str(op))
        for succ in succs:
            if succ not in symbols:
                symbols[succ] = Int(str(succ))

            op_sym = symbols[op]
            succ_sym = symbols[succ]
            solver.assert_and_track(
                And([op_sym < succ_sym]), "rto {0} < {1}".format(op_sym, succ_sym)
            )
            # solver.add(And([op_sym < succ_sym, op_sym > 0, succ_sym > 0]))
            # print("{0} < {1}".format(op_sym, succ_sym))

    # no intervening writes between matched reads and writes
    for read_op, write_op in cg.matches.items():
        if read_op not in symbols or write_op not in symbols:
            continue

        for op in successors.keys():
            if op.cmd is Command.WRITE and op.key == read_op.key and op != write_op:
                if op not in symbols:
                    continue

                read_sym = symbols[read_op]
                write_sym = symbols[write_op]
                op_sym = symbols[op]

                solver.assert_and_track(
                    Not(And([write_sym < op_sym, op_sym < read_sym])),
                    "intervening write ~({0} < {1} < {2})".format(
                        write_sym, op_sym, read_sym
                    ),
                )
                # solver.add(Not(
                #     And([write_sym < op_sym, op_sym < read_sym])
                # ))

    if symbols.values():
        solver.add(Distinct([sym for _, sym in symbols.items()]))
        solver.add(And([op_sym > 0 for op_sym in symbols.values()]))
    else:
        print("No symbols generated. Skipping solver.")
        return True, {}

    if solver.check() != sat:
        c = solver.unsat_core()
        print("UNSAT--z3", c)
        return False, None

    model = solver.model()
    values = {op: model.evaluate(s).as_long() for op, s in symbols.items()}
    return True, values


def parse_json_value(json_str):
    """
    Parse a JSON-encoded value from the CSV format.
    Returns the actual value from the JSON structure.
    """
    if not json_str or json_str.strip() == "":
        return None
    try:
        parsed = json.loads(json_str)
        if isinstance(parsed, dict) and "value" in parsed:
            return parsed["value"]
        return parsed
    except (json.JSONDecodeError, ValueError):
        return json_str


def parseTrace(outfile):
    """
    Parses the CSV trace file and converts it into a list of Action objects.
    """
    actions = []
    with open(outfile, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:

            client_id = str(row["ClientID"])
            kind = CallType.INV if row["Kind"] == "Invocation" else CallType.RESP
            action_name = row["Action"]  # e.g., "34_write", "35_read"

            action = Action(client_id, kind, None)

            # Get all payload columns (Payload1, Payload2, etc.)
            payload_values = []
            i = 1
            while f"Payload{i}" in row:
                val = parse_json_value(row[f"Payload{i}"])
                # We append None values as they are significant (e.g., VOption(None))
                # The check for `val is not None` in the original file was
                # likely to skip empty payload columns, which parse_json_value now does.
                # We'll re-add it to be safe, assuming empty strings parse to None.
                if val is not None:
                    payload_values.append(val)
                i += 1

            if "write" in action_name:
                if kind == CallType.INV:
                    action.cmd = Command.WRITE
                    # Payload1: Node ID (ignored)
                    # Payload2: Key
                    # Payload3: Value
                    action.k = payload_values[1] if len(payload_values) > 1 else None
                    action.val = payload_values[2] if len(payload_values) > 2 else None
                elif kind == CallType.RESP:
                    action.cmd = Command.OK
                    # No value payload on write response

            elif "read" in action_name:
                if kind == CallType.INV:
                    action.cmd = Command.READ
                    # Payload1: Node ID (ignored)
                    # Payload2: Key
                    action.k = payload_values[1] if len(payload_values) > 1 else None
                elif kind == CallType.RESP:
                    action.cmd = Command.OK
                    # Payload1 is the return value
                    parsed_val = payload_values[0] if len(payload_values) > 0 else None

                    # Handle VOption wrapper, e.g., {"type":"VOption","value":{"type":"VString","value":"val_546"}}
                    # or {"type":"VOption","value":null}
                    if (
                        isinstance(parsed_val, dict)
                        and "type" in parsed_val
                        and parsed_val["type"] == "VOption"
                    ):
                        parsed_val = parsed_val.get(
                            "value"
                        )  # This is now None or {"type":"VString","value":"val_546"}

                    # Handle VString, VInt, etc. wrapper
                    # e.g., {"type":"VString","value":"val_546"}
                    if isinstance(parsed_val, dict) and "value" in parsed_val:
                        action.val = parsed_val["value"]
                    else:
                        action.val = parsed_val  # Handles None

            else:
                # Skips "init" actions or others
                continue

            actions.append(action)
            print(action.__str__())
    return actions


def main():

    parser = argparse.ArgumentParser(description="Check linearizability.")
    parser.add_argument("intermediate_file", type=str)
    args = parser.parse_args()

    outfile = args.intermediate_file
    actions = parseTrace(outfile)

    cg = ConstraintsGenerator()
    if cg.generate_constraints(actions):
        print(cg)
        is_sat, solution = z3solver(cg)
        if is_sat:
            if solution:
                sorted_ops = [
                    str(op)
                    for op, _ in sorted(solution.items(), key=lambda item: item[1])
                ]
                print(" < ".join(sorted_ops))
            else:
                print("SAT (empty or no-op history)")
            return 0
    return -1


if __name__ == "__main__":
    sys.exit(main())
