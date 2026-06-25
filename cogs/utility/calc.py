"""Math engine and LaTeX rendering for the .calc command.

Parsing is locked down: expressions are evaluated against an explicit
whitelist of SymPy functions/constants (``global_dict``), unknown names
become free symbols, and there is no access to builtins or attributes.

Math-mode results (eval/solve/diff/...) render through matplotlib's
mathtext, which needs no LaTeX install. The ``latex`` mode renders
arbitrary LaTeX documents (text, lists, tables, etc., not just math) by
shelling out to the ``tectonic`` engine and rasterizing the resulting PDF
with PyMuPDF — this requires the ``tectonic`` binary to be on PATH.
"""
from __future__ import annotations

import io
import multiprocessing
import os
import queue as _queue
import re
import subprocess
import tempfile
from pathlib import Path

import fitz  # PyMuPDF, for rasterizing tectonic's PDF output
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
    return _strip_identity_mul(raw), _run(evaluate=True)


def _strip_identity_mul(expr: sympy.Basic) -> sympy.Basic:
    """Drop the spurious ``1 *`` factor unevaluated parsing leaves on terms
    like ``1/x**2`` (parsed as ``Mul(1, x**-2)``), which would otherwise
    print as a stray "1" next to the fraction.
    """
    return expr.replace(
        lambda n: isinstance(n, sympy.Mul) and len(n.args) == 2 and n.args[0] == 1,
        lambda n: n.args[1],
    )


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


def _split_args(expression: str) -> list[str]:
    """Split on top-level commas, ignoring commas nested inside parentheses.

    Lets ``integrate x^2, 0, 1`` separate into bounds while
    ``integrate atan2(y, x)`` keeps its inner comma intact.
    """
    parts = []
    depth = 0
    current = []
    for ch in expression:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return [p.strip() for p in parts]


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


def _compute_diff(expression: str) -> tuple[str, str]:
    """Differentiate ``expr``, or ``expr, point`` to evaluate the derivative there."""
    parts = _split_args(expression)
    if len(parts) not in (1, 2):
        raise CalcError("diff takes an expression, or expression, point.")

    raw, expr = _parse(parts[0])
    raw_latex = sympy.latex(raw)
    var = _free_symbol(expr)
    derivative = sympy.diff(expr, var)
    var_latex = sympy.latex(var)

    if len(parts) == 1:
        body = rf"\frac{{d}}{{d{var_latex}}}\left({raw_latex}\right) = {sympy.latex(derivative)}"
        return f"d/d{var} ({expr}) = {derivative}", body

    _, point = _parse(parts[1])
    try:
        value = sympy.simplify(derivative.subs(var, point))
    except Exception:
        raise CalcError("Could not evaluate the derivative at that point.")
    point_latex = sympy.latex(point)
    value_latex = sympy.latex(value)
    approx = _numeric(value)
    body = rf"\frac{{d}}{{d{var_latex}}}\left({raw_latex}\right)\Bigg|_{{{var_latex}={point_latex}}} = {value_latex}"
    plain = f"d/d{var} ({expr}) at {var}={point} = {value}"
    if approx is not None and approx != str(value):
        body += rf" \approx {approx}"
        plain += f" ≈ {approx}"
    return plain, body


def _compute_integrate(expression: str) -> tuple[str, str]:
    """Indefinite integral of ``expr``, or definite integral over ``expr, lower, upper``."""
    parts = _split_args(expression)
    if len(parts) not in (1, 3):
        raise CalcError("integrate takes an expression, or expression, lower, upper.")

    raw, expr = _parse(parts[0])
    raw_latex = sympy.latex(raw)
    var = _free_symbol(expr)
    var_latex = sympy.latex(var)

    if len(parts) == 1:
        result = sympy.integrate(expr, var)
        body = rf"\int {raw_latex}\,d{var_latex} = {sympy.latex(result)} + C"
        return f"∫ {expr} d{var} = {result} + C", body

    _, lower = _parse(parts[1])
    _, upper = _parse(parts[2])
    try:
        result = sympy.simplify(sympy.integrate(expr, (var, lower, upper)))
    except Exception:
        raise CalcError("Could not evaluate that integral.")
    if result.has(sympy.zoo, sympy.nan):
        raise CalcError("That integral is undefined over the given bounds.")
    lower_latex = sympy.latex(lower)
    upper_latex = sympy.latex(upper)
    result_latex = sympy.latex(result)
    approx = _numeric(result)
    body = rf"\int_{{{lower_latex}}}^{{{upper_latex}}} {raw_latex}\,d{var_latex} = {result_latex}"
    plain = f"∫[{lower},{upper}] {expr} d{var} = {result}"
    if approx is not None and approx != str(result):
        body += rf" \approx {approx}"
        plain += f" ≈ {approx}"
    return plain, body


def _compute_sum(expression: str) -> tuple[str, str]:
    """Evaluate a summation ``expr, lower, upper`` (``upper`` may be ``oo``)."""
    parts = _split_args(expression)
    if len(parts) != 3:
        raise CalcError("sum takes an expression, lower, upper.")

    raw, expr = _parse(parts[0])
    raw_latex = sympy.latex(raw)
    var = _free_symbol(expr)
    var_latex = sympy.latex(var)

    _, lower = _parse(parts[1])
    _, upper = _parse(parts[2])
    try:
        result = sympy.simplify(sympy.Sum(expr, (var, lower, upper)).doit())
    except Exception:
        raise CalcError("Could not evaluate that sum.")
    if result.has(sympy.zoo, sympy.nan):
        raise CalcError("That sum is undefined or does not converge over the given bounds.")
    lower_latex = sympy.latex(lower)
    upper_latex = sympy.latex(upper)
    result_latex = sympy.latex(result)
    approx = _numeric(result)
    body = rf"\sum_{{{var_latex}={lower_latex}}}^{{{upper_latex}}} {raw_latex} = {result_latex}"
    plain = f"sum[{var}={lower}..{upper}] {expr} = {result}"
    if approx is not None and approx != str(result):
        body += rf" \approx {approx}"
        plain += f" ≈ {approx}"
    return plain, body


def compute(mode: str, expression: str) -> tuple[str, str]:
    """Run a calculation.

    Returns ``(plain_text, latex)`` where ``plain_text`` is a copy-pasteable
    summary and ``latex`` is the body to render as an image.

    Not used for ``mode == "latex"`` — that path renders a full LaTeX
    document via ``render_full_latex`` instead, see ``_job_worker``.
    """
    if mode == "diff":
        return _compute_diff(expression)
    if mode == "integrate":
        return _compute_integrate(expression)
    if mode == "sum":
        return _compute_sum(expression)

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


_LATEX_MAX_LEN = 4000

# Best-effort denylist for arbitrary LaTeX. tectonic has no shell-escape
# support at all, so \write18 can't run commands, but \input/\openin etc.
# can still read files from disk -- block the commands that read/write
# files or duck typeset-time inspection tricks. Not bulletproof (catcode
# games can rebuild a banned token), but raises the bar a lot for a bot
# used among trusted users.
_LATEX_FORBIDDEN_RE = re.compile(
    r"\\(input|include|includegraphics|openin|openout|write\d*|read|catcode|csname|directlua|immediate)\b"
)

_LATEX_DOCUMENT = r"""\documentclass[border=4pt,varwidth=16cm]{standalone}
\usepackage{amsmath,amssymb,amsfonts}
\usepackage[dvipsnames]{xcolor}
\pagecolor[HTML]{313338}
\color{white}
\begin{document}
%s
\end{document}
"""

_TECTONIC_TIMEOUT = 12  # seconds; kept under the cog's outer per-job timeout


def _friendly_tectonic_error(stderr: bytes) -> str:
    text = stderr.decode("utf-8", "replace") if stderr else ""
    errors = [line.strip() for line in text.splitlines() if line.strip().startswith("error:")]
    if errors:
        detail = errors[0]
        if len(detail) > 200:
            detail = detail[:200] + "…"
        return f"Could not compile that LaTeX: {detail}"
    return "Could not compile that LaTeX. Check your syntax."


# tectonic still does a metadata round-trip to its bundle server on every
# run even with --only-cached, which dominates wall time (~1.7s) once the
# package cache is warm. Our document template is fixed, so every resource
# it could ever need is already cached after the first compile -- point it
# at an unroutable proxy so that check fails immediately instead of
# round-tripping the network, and skip the env's real proxy if set.
_NETLESS_ENV = {
    "PATH": os.environ.get("PATH", ""),
    "HOME": os.environ.get("HOME", ""),
    "http_proxy": "http://127.0.0.1:1",
    "https_proxy": "http://127.0.0.1:1",
}


def render_full_latex(expression: str) -> bytes:
    """Compile arbitrary LaTeX (not just math) to a PNG.

    Wraps ``expression`` in a standalone document, compiles it with
    tectonic, and rasterizes the resulting PDF with PyMuPDF.
    """
    if len(expression) > _LATEX_MAX_LEN:
        raise CalcError("LaTeX input is too long.")
    if _LATEX_FORBIDDEN_RE.search(expression):
        raise CalcError("That LaTeX command is not allowed.")

    document = _LATEX_DOCUMENT % expression
    with tempfile.TemporaryDirectory() as tmp:
        tex_path = Path(tmp) / "doc.tex"
        tex_path.write_text(document)
        try:
            proc = subprocess.run(
                [
                    "tectonic", "-X", "compile", "doc.tex", "--outfmt", "pdf",
                    "--reruns", "0", "--only-cached",
                ],
                cwd=tmp,
                capture_output=True,
                timeout=_TECTONIC_TIMEOUT,
                env=_NETLESS_ENV,
            )
        except FileNotFoundError:
            raise CalcError("LaTeX rendering is unavailable on this server.")
        except subprocess.TimeoutExpired:
            raise CalcError("That LaTeX document took too long to compile.")
        if proc.returncode != 0:
            raise CalcError(_friendly_tectonic_error(proc.stderr))

        pdf_path = Path(tmp) / "doc.pdf"
        if not pdf_path.exists():
            raise CalcError("Could not render that LaTeX.")
        doc = fitz.open(pdf_path)
        try:
            pix = doc[0].get_pixmap(matrix=fitz.Matrix(6, 6), alpha=False)
            return pix.tobytes("png")
        finally:
            doc.close()


# ── Process isolation ──
# SymPy/matplotlib work is CPU-bound pure Python that holds the GIL, so running
# it in a thread would still freeze the bot's event loop and couldn't be killed.
# Instead we run each job in a child process we can terminate on timeout.
#
# We avoid the "fork" start method: forking a multithreaded process (the bot has
# the asyncio loop + thread pool) risks deadlocks from copied locks. "forkserver"
# forks from a clean single-threaded helper; "spawn" is the portable fallback.
# Neither re-runs main.py's bot startup (forkserver imports only this module;
# spawn relies on main.py's __main__ guard).

def _pick_context() -> multiprocessing.context.BaseContext:
    for method in ("forkserver", "spawn"):
        try:
            ctx = multiprocessing.get_context(method)
        except ValueError:
            continue
        if method == "forkserver":
            try:
                ctx.set_forkserver_preload(["cogs.utility.calc"])
            except Exception:
                pass
        return ctx
    return multiprocessing.get_context()


_MP_CTX = _pick_context()


def _job_worker(mode: str, expression: str, out) -> None:
    """Child-process entry point: compute + render, push result to the queue."""
    try:
        if mode == "latex":
            plain = expression.strip()
            png = render_full_latex(expression)
        else:
            plain, body = compute(mode, expression)
            png = render_latex(body).getvalue()
        out.put(("ok", plain, png))
    except CalcError as exc:
        out.put(("error", str(exc)))
    except Exception:
        out.put(("error", "Invalid expression. Supports functions, constants, fractions and more."))


def run_job(mode: str, expression: str, timeout: float) -> tuple[str, bytes]:
    """Run a calc job in a killable child process. Returns ``(plain, png_bytes)``.

    Blocking — call via ``asyncio.to_thread`` so the event loop stays free.
    Raises CalcError on failure, timeout, or a crashed worker.
    """
    out = _MP_CTX.Queue()
    proc = _MP_CTX.Process(target=_job_worker, args=(mode, expression, out), daemon=True)
    proc.start()
    try:
        # Read before join so a large PNG can't deadlock on a full pipe.
        result = out.get(timeout=timeout)
    except _queue.Empty:
        result = None
    finally:
        if proc.is_alive():
            proc.terminate()
        proc.join(timeout=2)
        if proc.is_alive():  # didn't honor SIGTERM — force it
            proc.kill()
            proc.join()
        out.close()

    if result is None:
        raise CalcError("That calculation took too long.")
    if result[0] == "error":
        raise CalcError(result[1])
    return result[1], result[2]
