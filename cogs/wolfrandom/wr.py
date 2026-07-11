#!/usr/bin/env python3
"""
API for selecting WolfRandom chess variants positions from a pre-built wolfrandom database (.wr).
The database doesn't know chess rules and can be used to store positions for any chess variant.
Currently .wr is merely an SQLite database, but it has changed in the past and may change in the future.

The API can be used in two ways: as a binary or as a Python library.

# Using as binary

This is the simplest method that can be used from any programming language or manually from terminal.
If you use it from another piece of code, pass --json argument to make its output parsing-friendly.
If you use it manually, don't pass --json - for human-friendly output.

Call `./wr.py --help` for full documentation.

# Using as Python library

To use it from Python without invoking a binary, first you need to construct an EvalRange object
describing filtering by evaluation that you want to request. There are several ways to do this.
Refer to the documentation of EvalRange class below.

Second, you can use the constructed EvalRange object together with other parameters, to pass
them to the wolfrandom() function for position selection or printing all positions in this range.
Refer to the documentation of wolfrandom() function below.

Here is a basic example showcasing the features:

```
from wr import wolfrandom, EvalRange
from pprint import pprint

# Select a random position between -3.45 and mate-in-10 (white)
eval_range = EvalRange(-3.45, 'M10', allow_inverse_evals=False)
result = wolfrandom('database.wr', eval_range, action='select', add_eval=True)
pprint(result)
'''Prints something like:
{
    'total': 676767,
    'filtered': 454545,
    'selected': '1. c3 d6 2. d3 f5 3. e3 a5',
    'eval': '+4.07'
}
'''

# Print all positions with evaluation between -5 and +5 (inclusive)
range_all = EvalRange('-5.0:5.0')
wolfrandom('database.wr', range_all, action='print',
           printsink=open('output.txt', 'r'), add_eval=False)
```
"""

import argparse
import json
import os
import secrets
import sqlite3
import sys

# ---------- Common helpers for building or reading the db ----------

def encode_eval(eval_float: float) -> int:
    if eval_float != eval_float:
        raise ValueError("NaN is not allowed as eval")
    if abs(eval_float) > 1000.0:
        eval_float = 1000.0 if eval_float > 0 else -1000.0
    return int(round(eval_float * 100))

def parse_eval(eval_str: str) -> int:
    s = eval_str.strip()
    if s.startswith('-M'):
        n = int(s[2:])
        if not (0 <= n <= 999):
            raise ValueError("invalid mate value")
        return -100000 + n
    if s.startswith('M'):
        n = int(s[1:])
        if not (0 <= n <= 999):
            raise ValueError("invalid mate value")
        return 100000 - n
    return encode_eval(float(s))

def eval_to_str(code: int) -> str:
    assert abs(code) <= 100000
    if code == 0:
        return '0.00'
    if abs(code) == 100000:
        sign = '-' if code < 0 else ''
        return sign + 'inf'
    if abs(code) > 99000:
        sign = '-' if code < 0 else ''
        return sign + 'M' + str(100000 - abs(code))
    sign = '-' if code < 0 else '+'
    return sign + str(abs(code) // 100) + '.' + '{:0>2}'.format(abs(code) % 100)

# ---------- Main API ----------

class EvalRange:
    """Class representing engine evaluation range.

    This can be either [low, high] (both ends included), or it can be
    the union of [-high,-low] and [low, high], to represent the positions
    that are not worse than `high`, but not closer to equality than `low`.
    For the latter, you will need to pass `allow_inverse_evals=True` to constructor.

    There are two ways you can construct this class. First, by a single string:
    ```
    EvalRange(':') # equivalent to EvalRange('-inf:inf')
    EvalRange('-8.0:8.0')
    EvalRange('-M12:M3')
    EvalRange('3.0:6.1', allow_inverse_evals=True)
    ```
    Another method is via two arguments, each of which can be either str, float or int:
    ```
    EvalRange(-8, 8)
    EvalRange('-M12', -4.5)
    EvalRange(3.12, '4.0', allow_inverse_evals=True)
    ```

    Note that the intervals [-high,-low] and [low, high] are intersecting if low <= 0 <= high,
    so their union is in fact a single interval, and using option `allow_inverse_evals` is not
    needed and can be confusing. You can call `eval_range.print_warning_if_needed(file=sys.stderr)`
    to warn about this situation, although it is not a hard error.

    If range is impossible to construct, constructor will raise ValueError describing the reason.
    """

    def __init__(self, first, second=None, allow_inverse_evals=False):
        # Stage 1: split the range if needed
        if second is None:
            split = first.split(':')
            if len(split) != 2:
                raise ValueError("Use EvalRange('low:high') or EvalRange(low, high)")
            first, second = split
            if not first:
                first = float('-inf')
            if not second:
                second = float('inf')

        # Stage 2: convert evals to encoded ints
        def encode_raw(value) -> int:
            if isinstance(value, float) or isinstance(value, int):
                return encode_eval(float(value))
            if isinstance(value, str):
                return parse_eval(value)
            raise ValueError("Unsupported type of evaluation (must be str, float or int)")
        self.low, self.high = encode_raw(first), encode_raw(second)
        if self.low > self.high:
            raise ValueError("Invalid range: low > high")
        self.allow_inverse_evals = allow_inverse_evals

    def print_warning_if_needed(self, file):
        if not self.allow_inverse_evals:
            return
        if self.low <= 0 and self.high >= 0:
            bound = max(abs(self.low), abs(self.high))
            print(f"Warning: no need to use 'Allow inverse evals' option; use `{eval_to_str(-bound)}:{eval_to_str(bound)}` range instead", file=file)


def wolfrandom(db_path: str, eval_range: EvalRange, action='select', printsink=None, add_eval=True):
    """Main routine to work with .wr database

    Args:
        db_path: Path to the .wr database.
        eval_range: Evaluation range to filter positions of the database.
        action: Action to perform with the interval, possible values:
            select: Select a random position among those satisfying the evaluation range.
            print: Print the list of positions to printsink if provided, or stdout by default.
        printsink: Destination to print list of positions by print(..., file=printsink) ; sys.stdout by default.
        add_eval: Whether to add eval to JSON or printed list of positions.

    Returns:
        A JSON object. Example for 'select' action:
            {
                'total': 44444, # total number of positions
                'filtered': 24, # positions satisfying the constraint
                'selected': '1. c3 d6 2. d3 f5 3. e3 a5',
                'eval': '+4.07',
            }
        If 'filtered' is 0 (no positions found), 'selected' and 'eval' fields are not included.
        Same happens if the requested action is 'print' instead of 'select'.
        If add_eval is False, then 'eval' field is never included.

    Raises:
        ValueError: on unsupported `action` argument value
        Other: on disk/SQL operations failure (e.g. malformed or missing database)
    """

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM main")
    result = {}
    result['total'] = cur.fetchone()[0]

    intervals = []
    low, high = eval_range.low, eval_range.high
    if eval_range.allow_inverse_evals:
        if low <= 0 <= high:
            bound = max(abs(low), abs(high))
            intervals = [(-bound, bound)]
        else:
            intervals = [(low, high), (-high, -low)]
            intervals.sort()
    else:
        intervals = [(low, high)]

    interval_data = []  # each: (count, seq_first, seq_last)
    total_filtered = 0

    for lo, hi in intervals:
        cur.execute(
            "SELECT seq FROM main WHERE eval_code >= ? ORDER BY eval_code, idx LIMIT 1",
            (lo,)
        )
        row = cur.fetchone()
        if row is None:
            continue
        seq_first = row[0]

        cur.execute(
            "SELECT seq FROM main WHERE eval_code <= ? ORDER BY eval_code DESC, idx DESC LIMIT 1",
            (hi,)
        )
        row = cur.fetchone()
        if row is None:
            continue
        seq_last = row[0]

        if seq_first > seq_last:
            continue

        cnt = seq_last - seq_first + 1
        interval_data.append((cnt, seq_first, seq_last))
        total_filtered += cnt

    result['filtered'] = total_filtered

    if total_filtered == 0:
        conn.close()
        return result

    if action == 'print':
        conditions = []
        params = []
        for _, first, last in interval_data:
            conditions.append("(seq BETWEEN ? AND ?)")
            params.extend([first, last])
        if not conditions:
            conn.close()
            return result
        where_clause = " WHERE " + " OR ".join(conditions)
        query = "SELECT eval_code, moves FROM main" + where_clause
        cur.execute(query, params)
        if printsink is None:
            printsink = sys.stdout
        for code, moves in cur.fetchall():
            if add_eval:
                to_print = f"{moves} | {eval_to_str(code)}"
            else:
                to_print = f"{moves}"
            print(to_print, file=printsink)
        conn.close()
        return result
    elif action == 'select':
        r = secrets.randbelow(total_filtered)
        chosen = None
        for cnt, seq_first, seq_last in interval_data:
            if r >= cnt:
                r -= cnt
                continue
            chosen = seq_first + r
            assert chosen <= seq_last
            break
        assert chosen is not None
        cur.execute("SELECT eval_code, moves FROM main WHERE seq = ?", (chosen,))
        eval_code, moves = cur.fetchone()

        result['selected'] = moves
        if add_eval:
            result['eval'] = eval_to_str(eval_code)

        conn.close()
        return result
    else:
        raise ValueError("Invalid action argument")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Select a random position from a wolfrandom database by evaluation range.",
        usage="%(prog)s <database.wr> <range> [options]",
        epilog=(
            "Range format: [low]:[high] (e.g., -8.0:8.0). Bounds are inclusive.\n"
            "Open ends: -8.0:  (all ≥ -8.0)  or  :8.0  (all ≤ 8.0). Use `:` for everything.\n"
            "Mates: M12:M2 or -M2:-M12 are valid. Internally mates are encoded above any numeric\n"
            "       score: M1 is highest, M999 is the lowest mate (still > any non‑mate).\n"
            "Examples:\n"
            "  %(prog)s db.wr -8.0:8.0\n"
            "  %(prog)s db.wr -8.0:8.0 --eval\n"
            "  %(prog)s db.wr -8.0:8.0 --eval --json\n"
            "  %(prog)s db.wr 3.0:6.0 --allow-inverse-evals # [-6;-3] and [3,6] ranges\n"
            "  %(prog)s db.wr M200:M1 --allow-inverse-evals # select random mate (white or black)\n"
            "  %(prog)s db.wr : --print # print all positions sorted by eval\n\n"
            "Note: --print and --json cannot be combined – JSON for all positions could be\n"
            "      humongous and likely exceed memory, so it's intentionally forbidden."
        ),
        add_help=True,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--eval", action="store_true",
                        help="Print the evaluation along with the selected position")
    parser.add_argument("--allow-inverse-evals", action="store_true",
                        help="Use union of [low,high] and [-high,-low]")
    parser.add_argument("--print", action="store_true",
                        help="Print all positions in the range (no random selection)")
    parser.add_argument("--json", action="store_true",
                        help="Print JSON instead of human-friendly output")

    known, remaining = parser.parse_known_args()

    if len(remaining) != 2:
        parser.error("Expected two positional arguments: <database.wr> <range>")
    db_path = remaining[0]
    range_str = remaining[1]

    if known.print and known.json:
        parser.error("Combining --print and --json is not supported")

    if not os.path.isfile(db_path):
        print(f'Error: `{db_path}` is not a file')
        exit(1)

    try:
        eval_range = EvalRange(range_str, allow_inverse_evals=known.allow_inverse_evals)
        eval_range.print_warning_if_needed(file=sys.stderr)
    except ValueError as e:
        print(e)
        exit(1)

    if known.print:
        action = 'print'
    else:
        action = 'select'

    result = wolfrandom(db_path, eval_range, action=action, add_eval=known.eval)
    if known.print:
        exit(0)

    if known.json:
        print(json.dumps(result))
        if 'selected' not in result:
            exit(2)
        exit(0)

    total, filtered = result['total'], result['filtered']
    print(f"Total positions: {total}")
    print(f"Filtered set contains {filtered} positions.")
    if filtered == 0:
        print("No position to select, exiting...")
        exit(2)

    print('Selected:\n' + result['selected'])
    if known.eval:
        print(f"Eval: {result['eval']}")
    exit(0)
