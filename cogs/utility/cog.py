from __future__ import annotations
import asyncio
import io
import random
import re
import struct
import zlib

import aiohttp
import discord
from discord.ext import commands

from . import calc

_CALC_TIMEOUT = 10  # seconds; symbolic work can be slow, so cap it
_LATEX_TIMEOUT = 15  # seconds; compiling a full LaTeX doc is slower than mathtext

_HEX_RE = re.compile(r"^#?([0-9a-fA-F]{6})$")

# Translation via Google's free, unofficial endpoint (no API key). If it ever
# starts rate-limiting, swap _translate's request for DeepL/Cloud Translation —
# the rest of the command doesn't care where the text comes from.
_TRANSLATE_URL = "https://translate.googleapis.com/translate_a/single"
_TRANSLATE_TIMEOUT = 10  # seconds
_TRANSLATE_MAX_CHARS = 4000  # GET has URL limits; also keeps replies sane

# Common language names → ISO codes. Anything not listed is passed through as-is,
# so short codes ("en", "pt-br") and "auto" work without being in the map.
_LANG_NAMES = {
    "arabic": "ar", "bulgarian": "bg", "chinese": "zh-CN", "czech": "cs",
    "danish": "da", "dutch": "nl", "english": "en", "finnish": "fi",
    "french": "fr", "german": "de", "greek": "el", "hebrew": "iw",
    "hindi": "hi", "hungarian": "hu", "italian": "it", "japanese": "ja",
    "korean": "ko", "norwegian": "no", "polish": "pl", "portuguese": "pt",
    "romanian": "ro", "russian": "ru", "spanish": "es", "swedish": "sv",
    "thai": "th", "turkish": "tr", "ukrainian": "uk", "vietnamese": "vi",
}


class TranslateError(Exception):
    """Raised when the translation request fails or returns no usable text."""


def _normalize_lang(lang: str) -> str:
    """Map a friendly language name to its code; pass codes/"auto" through."""
    key = lang.strip().lower()
    return _LANG_NAMES.get(key, key)


# ── Novelty "translators" ────────────────────────────────────────────────────
# Pure text transforms with no network dependency. Kept as standalone functions
# so the command wrappers stay one-liners.

_MORSE = {
    "A": ".-", "B": "-...", "C": "-.-.", "D": "-..", "E": ".", "F": "..-.",
    "G": "--.", "H": "....", "I": "..", "J": ".---", "K": "-.-", "L": ".-..",
    "M": "--", "N": "-.", "O": "---", "P": ".--.", "Q": "--.-", "R": ".-.",
    "S": "...", "T": "-", "U": "..-", "V": "...-", "W": ".--", "X": "-..-",
    "Y": "-.--", "Z": "--..", "0": "-----", "1": ".----", "2": "..---",
    "3": "...--", "4": "....-", "5": ".....", "6": "-....", "7": "--...",
    "8": "---..", "9": "----.", ".": ".-.-.-", ",": "--..--", "?": "..--..",
    "'": ".----.", "!": "-.-.--", "/": "-..-.", "(": "-.--.", ")": "-.--.-",
    "&": ".-...", ":": "---...", ";": "-.-.-.", "=": "-...-", "+": ".-.-.",
    "-": "-....-", "_": "..--.-", '"': ".-..-.", "@": ".--.-.",
}
_MORSE_REVERSE = {v: k for k, v in _MORSE.items()}

_LEET = str.maketrans({
    "a": "4", "A": "4", "e": "3", "E": "3", "i": "1", "I": "1",
    "o": "0", "O": "0", "t": "7", "T": "7", "s": "5", "S": "5",
    "g": "9", "G": "9", "b": "8", "B": "8",
})


def _uwuify(text: str) -> str:
    text = re.sub(r"[rl]", "w", text)
    text = re.sub(r"[RL]", "W", text)
    text = re.sub(r"n([aeiou])", r"ny\1", text)
    text = re.sub(r"N([aeiouAEIOU])", r"Ny\1", text)
    text = text.replace("ove", "uv")
    faces = ["uwu", "owo", ">w<", "^w^", ":3", "x3", "nya~"]
    out = []
    for word in text.split(" "):
        # occasional cute stutter on words that start with a letter
        if word[:1].isalpha() and random.random() < 0.15:
            word = f"{word[0]}-{word}"
        out.append(word)
    return " ".join(out) + " " + random.choice(faces)


def _to_binary(text: str) -> str:
    return " ".join(format(b, "08b") for b in text.encode("utf-8"))


def _from_binary(text: str) -> str:
    bits = text.replace(" ", "")
    if len(bits) % 8:
        raise ValueError("binary length must be a multiple of 8")
    byte_vals = bytes(int(bits[i : i + 8], 2) for i in range(0, len(bits), 8))
    return byte_vals.decode("utf-8", errors="replace")


def _to_morse(text: str) -> str:
    # Encode word-by-word, joining words with " / " so it round-trips with
    # _from_morse. Unknown characters within a word are dropped.
    words = []
    for word in text.upper().split():
        letters = [_MORSE[c] for c in word if c in _MORSE]
        if letters:
            words.append(" ".join(letters))
    return " / ".join(words)


def _from_morse(text: str) -> str:
    words = text.strip().split(" / ")
    decoded = []
    for word in words:
        letters = [_MORSE_REVERSE.get(sym, "") for sym in word.split() if sym]
        decoded.append("".join(letters))
    return " ".join(decoded)


def _mock(text: str) -> str:
    out = []
    upper = False
    for ch in text:
        if ch.isalpha():
            out.append(ch.upper() if upper else ch.lower())
            upper = not upper
        else:
            out.append(ch)
    return "".join(out)


# Novelty "languages" usable as an origin or target in .trans.
#   One-way effects apply wherever they appear (origin or target).
#   Codecs are directional: encode when used as target, decode when used as origin.
_FX_ONEWAY = {
    "uwu": _uwuify,
    "leet": lambda t: t.translate(_LEET),
    "mock": _mock,
    "clap": lambda t: " 👏 ".join(t.split()),
    "reverse": lambda t: t[::-1],
}
_FX_CODEC = {  # name: (encode, decode)
    "binary": (_to_binary, _from_binary),
    "morse": (_to_morse, _from_morse),
}
_FX_ALIASES = {
    "1337": "leet", "spongebob": "mock", "mocking": "mock",
    "morsecode": "morse", "backwards": "reverse",
}


def _resolve_fx(token: str) -> str | None:
    """Return the canonical novelty-effect name for a token, or None if it's not
    an effect (i.e. treat it as a real language)."""
    key = token.strip().lower()
    key = _FX_ALIASES.get(key, key)
    if key in _FX_ONEWAY or key in _FX_CODEC:
        return key
    return None


def _apply_fx(name: str, text: str, *, as_origin: bool) -> str:
    """Apply a novelty stage. Codecs decode as origin, encode as target; one-way
    effects apply the same regardless of position."""
    if name in _FX_ONEWAY:
        return _FX_ONEWAY[name](text)
    encode, decode = _FX_CODEC[name]
    return decode(text) if as_origin else encode(text)


class Utility(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._session: aiohttp.ClientSession | None = None

    async def cog_load(self) -> None:
        self._session = aiohttp.ClientSession()

    async def cog_unload(self) -> None:
        if self._session:
            await self._session.close()

    @commands.group(
        invoke_without_command=True,
        extras={"example": ".calc sin(pi/4) + sqrt(2)"},
    )
    async def calc(self, ctx: commands.Context, *, expression: str):
        """Evaluate a math expression and render the result as an image.

        **Operators**
        `+` `-` `*` `/` · `^` or `**` power · `%` modulo · `//` floor division · `!` factorial · `( )` to group

        **Implicit multiplication** — `2x`, `3(4+1)` and `2pi` all work.

        **Functions** — `sqrt` `cbrt` `exp` `log` (`log(x)` natural, `log(x, b)` base-b) `ln` `abs` `floor` `ceil` `sin` `cos` `tan` `asin` `acos` `atan` `sinh` `cosh` `tanh` `factorial` `gamma` `gcd` `lcm` `min` `max` `sign`

        **Constants** — `pi`, `e`, `oo` (infinity), `I` (imaginary unit)

        **Numbers** — integers, decimals, scientific (`1.5e3`); fractions stay exact (`1/3`) and a decimal is shown alongside.

        **Subcommands** (use a variable like `x`):
        `solve` solve = 0 · `diff` differentiate (add `, point` to evaluate there) · `integrate` integrate (add `, lower, upper` for a definite integral) · `sum` summation over `, lower, upper` (upper can be `oo`) · `simplify` · `latex` compile arbitrary LaTeX (text, lists, tables — not just math)

        **Examples**
        `.calc sin(pi/4) + sqrt(2)` · `.calc (10+67/7)^2` · `.calc 5!`
        `.calc solve x^2 - 4` · `.calc diff sin(x)*x^2` · `.calc diff x^2, 3`
        `.calc integrate x^2` · `.calc integrate x^2, 0, 1`
        `.calc sum n^2, 1, 10` · `.calc sum 1/n^2, 1, oo`
        `.calc latex \\textbf{Hello} $\\int_0^1 x^2\\,dx$`"""
        await self._run_calc(ctx, "eval", expression)

    @calc.command(name="solve", extras={"example": ".calc solve x^2 - 4"})
    async def calc_solve(self, ctx: commands.Context, *, expression: str):
        """Solve an equation for its variable (expr = 0). Example: .calc solve x^2 - 4"""
        await self._run_calc(ctx, "solve", expression)

    @calc.command(name="diff", aliases=["derivative"], extras={"example": ".calc diff sin(x)*x^2, 3"})
    async def calc_diff(self, ctx: commands.Context, *, expression: str):
        """Differentiate an expression. Add ", point" to evaluate the derivative there.
        Examples: .calc diff sin(x)*x^2 · .calc diff x^2, 3"""
        await self._run_calc(ctx, "diff", expression)

    @calc.command(name="integrate", aliases=["integral"], extras={"example": ".calc integrate x^2, 0, 1"})
    async def calc_integrate(self, ctx: commands.Context, *, expression: str):
        """Integrate an expression. Add ", lower, upper" for a definite integral.
        Examples: .calc integrate x^2 · .calc integrate x^2, 0, 1"""
        await self._run_calc(ctx, "integrate", expression)

    @calc.command(name="sum", aliases=["summation"], extras={"example": ".calc sum n^2, 1, 10"})
    async def calc_sum(self, ctx: commands.Context, *, expression: str):
        """Evaluate a summation: expression, lower, upper (upper can be "oo" for infinite series).
        Examples: .calc sum n^2, 1, 10 · .calc sum 1/n^2, 1, oo"""
        await self._run_calc(ctx, "sum", expression)

    @calc.command(name="simplify", extras={"example": ".calc simplify (x^2-1)/(x-1)"})
    async def calc_simplify(self, ctx: commands.Context, *, expression: str):
        """Simplify an expression. Example: .calc simplify (x^2-1)/(x-1)"""
        await self._run_calc(ctx, "simplify", expression)

    @calc.command(name="latex", extras={"example": ".calc latex \\textbf{Hello} $\\int_0^1 x^2\\,dx$"})
    async def calc_latex(self, ctx: commands.Context, *, expression: str):
        """Compile arbitrary LaTeX to an image — not just math. Supports text
        formatting, lists, tables, amsmath/amssymb, etc., not just SymPy expressions.
        Example: .calc latex \\textbf{Hello} $\\int_0^1 x^2\\,dx$"""
        await self._run_calc(ctx, "latex", expression)

    async def _run_calc(self, ctx: commands.Context, mode: str, expression: str):
        timeout = _LATEX_TIMEOUT if mode == "latex" else _CALC_TIMEOUT
        async with ctx.typing():
            try:
                # run_job offloads the CPU-bound work to a killable child
                # process, so a runaway expression can't freeze the bot.
                plain, png = await asyncio.to_thread(
                    calc.run_job, mode, expression, timeout
                )
            except calc.CalcError as exc:
                await ctx.send(str(exc))
                return
            except Exception:
                await ctx.send("Invalid expression. See `.help calc` for the supported syntax.")
                return

        # Discord embed descriptions cap at 4096 chars; keep the plaintext sane.
        if len(plain) > 1000:
            plain = plain[:1000] + "…"

        file = discord.File(io.BytesIO(png), filename="calc.png")
        embed = discord.Embed(
            description=f"```{plain}```",
            color=discord.Color.blurple(),
        )
        embed.set_image(url="attachment://calc.png")
        await ctx.send(file=file, embed=embed)

    @commands.command()
    async def color(self, ctx: commands.Context, hex_code: str):
        """Show a color from a hex code. Example: .color #FF5733"""
        match = _HEX_RE.match(hex_code.strip())
        if not match:
            await ctx.send("Invalid hex code. Use format `#RRGGBB` (e.g. `#FF5733`).")
            return

        hex_str = match.group(1)
        r = int(hex_str[0:2], 16)
        g = int(hex_str[2:4], 16)
        b = int(hex_str[4:6], 16)

        width, height = 256, 128
        raw_row = b"\x00" + bytes([r, g, b]) * width
        raw_data = raw_row * height
        compressed = zlib.compress(raw_data)

        def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
            chunk = chunk_type + data
            return struct.pack(">I", len(data)) + chunk + struct.pack(">I", zlib.crc32(chunk) & 0xFFFFFFFF)

        png = b"\x89PNG\r\n\x1a\n"
        png += _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        png += _png_chunk(b"IDAT", compressed)
        png += _png_chunk(b"IEND", b"")

        file = discord.File(io.BytesIO(png), filename="color.png")
        embed = discord.Embed(
            title=f"#{hex_str.upper()}",
            description=f"RGB({r}, {g}, {b})",
            color=discord.Color.from_rgb(r, g, b),
        )
        embed.set_image(url="attachment://color.png")
        await ctx.send(file=file, embed=embed)

    @commands.command()
    async def dice(self, ctx: commands.Context, sides: int = 20):
        """Roll a dice with the given number of sides (default 20). Example: .dice 6"""
        if sides < 2:
            await ctx.send("A dice needs at least 2 sides.")
            return
        if sides > 1_000_000:
            await ctx.send("That dice is too large to roll.")
            return

        result = random.randint(1, sides)
        await ctx.send(f"🎲 You rolled a **{result}** (d{sides})")

    @commands.command(
        aliases=["translator", "translate"],
        extras={"example": ".trans en fr Hello world"},
    )
    async def trans(self, ctx: commands.Context, origin: str, target: str, *, text: str):
        """Translate text from one language to another.

        Usage: `.trans <origin> <target> text`
        Use `auto` as the origin to auto-detect the source language.
        Languages accept ISO codes (`en`, `fr`, `pt-br`) or names (`english`, `french`).

        **Examples**
        `.trans en fr Hello world` · `.trans auto es Bonjour tout le monde`

        **Named languages** (or pass any ISO code directly)
        _LANG_LIST_

        **Novelty "languages"** — use as the origin or target:
        `uwu` `leet` `mock` `clap` `reverse` (effects, apply anywhere)
        `binary` `morse` (encode as target, decode as origin)
        Examples: `.trans en leet Hello` · `.trans uwu leet hi there` · `.trans en morse sos` · `.trans morse en ... --- ...`"""
        text = text.strip()
        if not text:
            await ctx.send("Give me some text to translate.")
            return
        if len(text) > _TRANSLATE_MAX_CHARS:
            await ctx.send(f"That text is too long — keep it under {_TRANSLATE_MAX_CHARS} characters.")
            return

        fx_origin = _resolve_fx(origin)
        fx_target = _resolve_fx(target)

        # If either side is a novelty effect, run the pipeline instead of a real
        # translation (a real translation needs a genuine language pair).
        if fx_origin or fx_target:
            await self._run_fx(ctx, origin, target, fx_origin, fx_target, text)
            return

        sl = _normalize_lang(origin)
        tl = _normalize_lang(target)

        async with ctx.typing():
            try:
                translated, detected = await self._translate(sl, tl, text)
            except TranslateError:
                await ctx.send("Couldn't translate that. Check the language codes and try again.")
                return
            except Exception:
                await ctx.send("Translation service is unavailable right now. Try again later.")
                return

        # Detected source when origin was "auto"; otherwise show what was asked.
        source_label = detected or sl
        embed = discord.Embed(
            description=translated[:4096],
            color=discord.Color.blurple(),
        )
        embed.set_author(name=f"{source_label} → {tl}")
        await ctx.send(embed=embed)

    async def _run_fx(self, ctx, origin, target, fx_origin, fx_target, text):
        """Pass text through the origin stage then the target stage. A stage that
        isn't a novelty effect (a real language) is a passthrough."""
        try:
            if fx_origin:
                text = _apply_fx(fx_origin, text, as_origin=True)
            if fx_target:
                text = _apply_fx(fx_target, text, as_origin=False)
        except ValueError:
            await ctx.send("That doesn't look like valid binary (needs whole bytes).")
            return

        if not text:
            await ctx.send("Nothing translatable in there.")
            return
        embed = discord.Embed(description=text[:4096], color=discord.Color.blurple())
        embed.set_author(name=f"{origin.lower()} → {target.lower()}")
        await ctx.send(embed=embed)

    async def _translate(self, sl: str, tl: str, text: str) -> tuple[str, str | None]:
        """Return (translated_text, detected_source). Raises TranslateError on failure.

        Isolated from the command so the backend can be swapped without touching
        the Discord-facing logic.
        """
        if self._session is None:  # cog_load didn't run (shouldn't happen)
            raise TranslateError("session not ready")

        params = {
            "client": "gtx",
            "sl": sl,
            "tl": tl,
            "dt": "t",
            "q": text,
        }
        timeout = aiohttp.ClientTimeout(total=_TRANSLATE_TIMEOUT)
        async with self._session.get(_TRANSLATE_URL, params=params, timeout=timeout) as resp:
            if resp.status != 200:
                raise TranslateError(f"HTTP {resp.status}")
            # Endpoint mislabels the response as text/html, so skip content-type check.
            data = await resp.json(content_type=None)

        # Shape: [[["translated","original",...], ...], ..., "detected_src", ...]
        try:
            segments = data[0]
            translated = "".join(seg[0] for seg in segments if seg and seg[0])
        except (IndexError, TypeError):
            raise TranslateError("unexpected response shape")
        if not translated:
            raise TranslateError("empty translation")

        detected = None
        if len(data) > 2 and isinstance(data[2], str):
            detected = data[2]
        return translated, detected


# Fill the language list into .trans's help from the single source of truth, so
# `.h trans` always lists exactly what _normalize_lang understands.
_LANG_LIST = ", ".join(f"`{name}`" for name in sorted(_LANG_NAMES))
Utility.trans.help = Utility.trans.help.replace("_LANG_LIST_", _LANG_LIST)
