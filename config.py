


from decimal import Decimal

PREFIX = "."
MAIN_CURRENCY_EMOJI = "🌸"
CURRENCY_NAME = "Flowers"

WORK_COOLDOWN = 3600 # 1 hour in seconds

# Crime (.crime) — owner-customizable risk/reward command
CRIME_COOLDOWN = 3600              # 1 hour in seconds (independent of .work)
DEFAULT_CRIME_SUCCESS_RATE = 40    # percent chance of success
DEFAULT_CRIME_PENALTY_PCT = 5      # on failure, lose this % of total (wallet + bank)
CRIME_MIN_PAYOUT = 100             # smallest successful payout
CRIME_MAX_PAYOUT = 1000            # largest successful payout
CRIME_PAYOUT_EXPONENT = 3          # >1 skews payouts toward CRIME_MIN_PAYOUT (diminishing odds of big wins)

# Market economy
REVENUE_BASE_MULTIPLIER = 800    # fallback multiplier if a company isn't solved at listing
REVENUE_INNER_EXP       = 0.5    # per-user char-count exponent (whale compression)
REVENUE_OUTER_EXP       = 1.0    # user-count exponent (activity ∝ revenue at 1.0)
DIVIDEND_PROFIT_SHARE   = Decimal("0.40")
LEVEL_UP_TREASURY_CONSUME = 0.80

DILUTION_MAX_RATE     = Decimal("0.025")  # cap at 2.5 % of total shares per week
DILUTION_DISCOUNT     = Decimal("0.95")   # dilution price = best_bid * 0.95

# ─────────────────────────────────────────────────────────────────────────────
# Cost / risk / leveling model. Tune with the balance simulator
# (simulate_market.py) before changing here.
#
# Cost = fixed overhead + variable share of revenue. The overhead is anchored to
# the revenue the company was IPO-priced for (base_weekly_revenue), so a channel
# that fails to sustain its listed activity goes loss-making — that's the risk.
COST_VARIABLE_RATE = 0.20    # variable cost as a fraction of actual weekly revenue
COST_DANGER_RATIO  = 0.67    # d: loss-making once activity falls below this × baseline
                             #    (0.67 = fragile; a ~33% activity drop already stings)

# Random shock events — occasional bad weeks that spike the fixed overhead,
# so even a healthy company sometimes pays no dividend.
SHOCK_PROB = 0.10            # probability of a shock in any given week
SHOCK_MIN  = 2.0             # overhead is multiplied by uniform(SHOCK_MIN, SHOCK_MAX)
SHOCK_MAX  = 3.0

# Leveling — bounded and scale-free. Level up when treasury reaches N weeks of
# revenue; each level multiplies earning power by LEVEL_MULT_STEP, up to a cap.
LEVEL_MULT_STEP      = 1.25  # revenue multiplier gain per level (was ×2, unbounded)
MAX_COMPANY_LEVEL    = 5     # hard cap → lifetime growth ≈ 1.25^5 ≈ 3.05×
LEVEL_TREASURY_WEEKS = 6     # level up once treasury ≥ this many weeks of revenue

# Dilution — mint shares only when a company outperforms its baseline revenue.
DILUTION_GAIN = 0.05         # dilution rate per unit of fractional revenue excess
                             #    (hits the 2.5% cap at ~50% over baseline)

# IPO defaults used by the multiplier solver.
DEFAULT_TOTAL_SHARES = 500
DEFAULT_IPO_PRICE    = 100
DEFAULT_TARGET_YIELD = 0.05  # target weekly dividend yield at baseline activity

# Real-stock market (Finnhub-backed, see cogs/realstocks). The API key is read
# from the FINNHUB_API_KEY env var. Prices are 1:1 with USD via per-symbol lot
# sizes so the integer currency can carry penny stocks.
REALSTOCK_QUOTE_TTL       = 180  # seconds a fetched quote stays fresh for trading
REALSTOCK_REFRESH_MINUTES = 5    # background refresh + chart-recording cadence
REALSTOCK_MIN_UNIT_PRICE  = 20   # a unit (lot) must cost at least this many coins
REALSTOCK_MAX_LOT         = 1_000_000  # safety cap for absurdly cheap tickers
REALSTOCK_PROFILE_REFRESH_DAYS = 7  # how often cached fundamentals (sector/domain/EPS) refresh

# Waifu system
WAIFU_BASE_VALUE = 5000
WAIFU_VALUE_MULTIPLIER = 1.5   # value after buy = max(paid, current) * 1.5
WAIFU_DECAY_RATE = 0.10        # lose 10% of excess per day
WAIFU_RESALE_RATE = 0.80       # fraction of a buy the previous owner receives (rest is a sink)
WAIFU_GIFT_RATE = 0.05         # daily gift to your waifu needed to pause decay = 5% of their value
WAIFU_GIFT_MIN = 500           # ... but never less than this much
MARRIAGE_FEE = 10_000          # Flowers to propose
ENGAGEMENT_DAYS = 7            # days of mutual ownership before proposing
REMINDER_MAX_DAYS = 10         # max reminder duration

# Lichess integration
LICHESS_VARIANTS = [
    {"name": "Bullet",     "key": "bullet"},
    {"name": "Blitz",      "key": "blitz"},
    {"name": "Rapid",      "key": "rapid"},
    {"name": "Atomic",     "key": "atomic"},
    {"name": "Antichess",  "key": "antichess"},
    {"name": "Crazyhouse", "key": "crazyhouse"},
]

LICHESS_RATING_ROLE_DEFAULTS = {
    "bullet":     {"min": 2000, "step": 100, "max": 2700, "enabled": True},
    "blitz":      {"min": 2000, "step": 100, "max": 2700, "enabled": True},
    "rapid":      {"min": 2000, "step": 100, "max": 2700, "enabled": True},
    "atomic":     {"min": 2000, "step": 100, "max": 2500, "enabled": True},
    "antichess":  {"min": 2000, "step": 100, "max": 2500, "enabled": True},
    "crazyhouse": {"min": 2000, "step": 100, "max": 2500, "enabled": True},
}

RATING_SEPARATOR_ROLE = "--- Ratings ---"