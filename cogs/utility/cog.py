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
_TRANSLATE_MAX_CHARS = 1000  # GET has URL limits; also keeps replies sane

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

_FUN_MAX_CHARS = 500  # novelty transforms can inflate length (e.g. binary ×8)

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


def _looks_binary(text: str) -> bool:
    stripped = text.replace(" ", "")
    return bool(stripped) and set(stripped) <= {"0", "1"}


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


def _looks_morse(text: str) -> bool:
    return bool(text.strip()) and set(text) <= {".", "-", "/", " "}


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

        **For fun** — `.uwu` · `.binary` · `.morse` · `.leet` · `.mock` · `.clap` · `.reverse`"""
        text = text.strip()
        if not text:
            await ctx.send("Give me some text to translate.")
            return
        if len(text) > _TRANSLATE_MAX_CHARS:
            await ctx.send(f"That text is too long — keep it under {_TRANSLATE_MAX_CHARS} characters.")
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

    # ── Novelty translators ──────────────────────────────────────────────────

    @staticmethod
    def _fun_check(text: str) -> str | None:
        """Validate/trim novelty-transform input; return an error message or None."""
        if not text.strip():
            return "Give me some text."
        if len(text) > _FUN_MAX_CHARS:
            return f"Keep it under {_FUN_MAX_CHARS} characters."
        return None

    @commands.command(extras={"example": ".uwu hello world"})
    async def uwu(self, ctx: commands.Context, *, text: str):
        """UwU-ify your text. Example: .uwu hello there"""
        if err := self._fun_check(text):
            await ctx.send(err)
            return
        await ctx.send(_uwuify(text)[:2000])

    @commands.command(extras={"example": ".binary hello"})
    async def binary(self, ctx: commands.Context, *, text: str):
        """Text → binary, or binary → text if you give it 0s and 1s.
        Examples: .binary hello · .binary 01101000 01101001"""
        if err := self._fun_check(text):
            await ctx.send(err)
            return
        try:
            result = _from_binary(text) if _looks_binary(text) else _to_binary(text)
        except ValueError:
            await ctx.send("That doesn't look like valid binary (needs whole bytes).")
            return
        await ctx.send(result[:2000] or "…")

    @commands.command(aliases=["morsecode"], extras={"example": ".morse sos"})
    async def morse(self, ctx: commands.Context, *, text: str):
        """Text → Morse, or Morse → text if you give it dots and dashes.
        Word separator on decode is ` / `. Examples: .morse sos · .morse ... --- ..."""
        if err := self._fun_check(text):
            await ctx.send(err)
            return
        result = _from_morse(text) if _looks_morse(text) else _to_morse(text)
        await ctx.send(result[:2000] or "Nothing translatable in there.")

    @commands.command(aliases=["1337"], extras={"example": ".leet elite hacker"})
    async def leet(self, ctx: commands.Context, *, text: str):
        """Convert text to l33t speak. Example: .leet elite hacker"""
        if err := self._fun_check(text):
            await ctx.send(err)
            return
        await ctx.send(text.translate(_LEET)[:2000])

    @commands.command(aliases=["spongebob", "mocking"], extras={"example": ".mock stop copying me"})
    async def mock(self, ctx: commands.Context, *, text: str):
        """mOcKiNg SpOnGeBoB text. Example: .mock stop copying me"""
        if err := self._fun_check(text):
            await ctx.send(err)
            return
        await ctx.send(_mock(text)[:2000])

    @commands.command(extras={"example": ".clap you are amazing"})
    async def clap(self, ctx: commands.Context, *, text: str):
        """Put 👏 between 👏 every 👏 word. Example: .clap you are amazing"""
        if err := self._fun_check(text):
            await ctx.send(err)
            return
        clapped = " 👏 ".join(text.split())
        await ctx.send(clapped[:2000])

    @commands.command(aliases=["backwards"], extras={"example": ".reverse hello world"})
    async def reverse(self, ctx: commands.Context, *, text: str):
        """Reverse your text. Example: .reverse hello world"""
        if err := self._fun_check(text):
            await ctx.send(err)
            return
        await ctx.send(text[::-1][:2000])


# Fill the language list into .trans's help from the single source of truth, so
# `.h trans` always lists exactly what _normalize_lang understands.
_LANG_LIST = ", ".join(f"`{name}`" for name in sorted(_LANG_NAMES))
Utility.trans.help = Utility.trans.help.replace("_LANG_LIST_", _LANG_LIST)
