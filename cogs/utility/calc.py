"""Math engine and LaTeX rendering for the .calc command.

Parsing is locked down: expressions are evaluated against an explicit
whitelist of SymPy functions/constants (``global_dict``), unknown names
become free symbols, and there is no access to builtins or attributes.
Rendering uses matplotlib's mathtext so no LaTeX install is required.
"""
from __future__ import annotations

import io

import matplotlib

matplotlib.use("Agg")  # headless, no display required
import matplotlib.pyplot as plt  # noqa: E402

import sympy  # noqa: E402
from sympy.parsing.sympy_parser import (  # noqa: E402
    implicit_multiplication_application,
    parse_expr,
    standard_transformations,
)

_TRANSFORMS = standard_transformations + (implicit_multiplication_application,)

# Names the parser is allowed to resolve. Anything else is treated as a
# symbolic variable (safe) rather than executable code.
_ALLOWED = {
    # primitives the standard transformations emit into the namespace
    "Integer": sympy.Integer, "Float": sympy.Float, "Rational": sympy.Rational,
    "Symbol": sympy.Symbol,
    "Add": sympy.Add, "Mul": sympy.Mul, "Pow": sympy.Pow,
    # constants
    "pi": sympy.pi,
    "e": sympy.E,
    "E": sympy.E,
    "I": sympy.I,
    "oo": sympy.oo,
    "inf": sympy.oo,
    "infinity": sympy.oo,
    # functions
    "sin": sympy.sin, "cos": sympy.cos, "tan": sympy.tan,
    "asin": sympy.asin, "acos": sympy.acos, "atan": sympy.atan, "atan2": sympy.atan2,
    "sinh": sympy.sinh, "cosh": sympy.cosh, "tanh": sympy.tanh,
    "exp": sympy.exp, "log": sympy.log, "ln": sympy.log, "sqrt": sympy.sqrt,
    "cbrt": sympy.cbrt, "root": sympy.root, "Abs": sympy.Abs, "abs": sympy.Abs,
    "floor": sympy.floor, "ceiling": sympy.ceiling, "ceil": sympy.ceiling,
    "factorial": sympy.factorial, "gamma": sympy.gamma,
    "gcd": sympy.gcd, "lcm": sympy.lcm,
    "Min": sympy.Min, "Max": sympy.Max, "min": sympy.Min, "max": sympy.Max,
    "sign": sympy.sign, "deg": sympy.rad,  # deg(x): treat x as degrees -> radians
}

# Guard rails against expressions that are cheap to write but expensive
# (or impossible) to evaluate.
_MAX_POW_EXP = 1000
_MAX_FACTORIAL = 10000


class CalcError(Exception):
    """A user-facing calculation error with a friendly message."""


def _parse(expression: str) -> tuple[sympy.Expr, sympy.Expr]:
    """Parse an expression into ``(raw, evaluated)`` SymPy trees.

    ``raw`` preserves the structure the user typed (good for display);
    ``evaluated`` is the simplified/computed form used for math.
    """
    if len(expression) > 500:
        raise CalcError("Expression is too long.")
    expr = expression.replace("^", "**")

    def _run(evaluate: bool) -> sympy.Basic:
        try:
            result = parse_expr(
                expr,
                transformations=_TRANSFORMS,
                global_dict=_ALLOWED,
                evaluate=evaluate,
            )
        except (SyntaxError, TypeError, ValueError, AttributeError, RecursionError):
            raise CalcError("Invalid expression.")
        if not isinstance(result, sympy.Basic):
            raise CalcError("Invalid expression.")
        return result

    # Guard the *unevaluated* tree first so something like 9^9^9 can't be
    # computed during parsing. Only once the guard passes do we evaluate.
    raw = _run(evaluate=False)
    _guard(raw)
    return raw, _run(evaluate=True)


def _guard(expr: sympy.Basic) -> None:
    """Reject expressions whose evaluation would be unreasonably expensive."""
    for node in sympy.preorder_traversal(expr):
        if isinstance(node, sympy.Pow):
            exp = node.exp
            if exp.is_number and exp.is_real and abs(float(exp)) > _MAX_POW_EXP:
                raise CalcError("Exponent too large.")
        elif isinstance(node, sympy.factorial):
            arg = node.args[0]
            if arg.is_number and arg.is_real and float(arg) > _MAX_FACTORIAL:
                raise CalcError("Factorial argument too large.")


def _free_symbol(expr: sympy.Basic) -> sympy.Symbol:
    """Pick the variable to operate on for solve/diff/integrate."""
    symbols = sorted(expr.free_symbols, key=lambda s: s.name)
    if not symbols:
        return sympy.Symbol("x")
    for s in symbols:
        if s.name == "x":
            return s
    return symbols[0]


def _numeric(expr: sympy.Basic) -> str | None:
    """Return a decimal approximation string, or None if not a real number."""
    try:
        value = sympy.N(expr, 12)
    except Exception:
        return None
    if value.is_number and value.is_real and not value.has(sympy.zoo, sympy.nan):
        text = str(value)
        # Trim trailing zeros from the decimal representation.
        if "." in text and "e" not in text.lower():
            text = text.rstrip("0").rstrip(".")
        return text
    return None


def compute(mode: str, expression: str) -> tuple[str, str]:
    """Run a calculation.

    Returns ``(plain_text, latex)`` where ``plain_text`` is a copy-pasteable
    summary and ``latex`` is the body to render as an image.
    """
    raw, expr = _parse(expression)
    raw_latex = sympy.latex(raw)

    if mode == "solve":
        var = _free_symbol(expr)
        # `solve(expr)` solves expr == 0.
        try:
            solutions = sympy.solve(expr, var, dict=False)
        except Exception:
            raise CalcError("Could not solve that equation.")
        if not solutions:
            raise CalcError("No solutions found.")
        lhs = sympy.latex(var)
        body = r",\quad ".join(
            f"{lhs} = {sympy.latex(s)}" for s in solutions
        )
        plain = ", ".join(f"{var} = {s}" for s in solutions)
        return plain, body

    if mode == "diff":
        var = _free_symbol(expr)
        result = sympy.diff(expr, var)
        body = rf"\frac{{d}}{{d{sympy.latex(var)}}}\left({raw_latex}\right) = {sympy.latex(result)}"
        return f"d/d{var} ({expr}) = {result}", body

    if mode == "integrate":
        var = _free_symbol(expr)
        result = sympy.integrate(expr, var)
        body = rf"\int {raw_latex}\,d{sympy.latex(var)} = {sympy.latex(result)} + C"
        return f"∫ {expr} d{var} = {result} + C", body

    if mode == "simplify":
        result = sympy.simplify(expr)
        body = rf"{raw_latex} = {sympy.latex(result)}"
        return f"{expr} = {result}", body

    # default: evaluate
    if expr.has(sympy.zoo, sympy.nan, sympy.oo, -sympy.oo) and not expr.free_symbols:
        raise CalcError("Result is undefined (division by zero?).")

    result = sympy.simplify(expr) if expr.free_symbols else expr
    result_latex = sympy.latex(result)
    approx = _numeric(result)

    # Show the original input only when it differs from the result, so we
    # don't render a redundant "X = X".
    show_input = raw_latex != result_latex
    head = f"{raw_latex} = " if show_input else ""
    plain_head = f"{expression.strip()} = " if show_input else ""
    if approx is not None and approx != str(result):
        body = rf"{head}{result_latex} \approx {approx}"
        plain = f"{plain_head}{result} ≈ {approx}"
    else:
        body = f"{head}{result_latex}"
        plain = f"{plain_head}{result}"
    return plain, body


_BG = "#313338"  # Discord dark embed background; readable on any theme


def render_latex(body: str) -> io.BytesIO:
    """Render a LaTeX math body to a PNG card (white text on a dark bg)."""
    fig = plt.figure(figsize=(0.01, 0.01))
    try:
        fig.text(
            0, 0, f"${body}$",
            fontsize=26,
            color="white",
            math_fontfamily="cm",
        )
        buf = io.BytesIO()
        fig.savefig(
            buf,
            dpi=200,
            facecolor=_BG,
            bbox_inches="tight",
            pad_inches=0.35,
        )
    finally:
        plt.close(fig)
    buf.seek(0)
    return buf
