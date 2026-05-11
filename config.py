


from decimal import Decimal

PREFIX = "."
MAIN_CURRENCY_EMOJI = "🌸"
CURRENCY_NAME = "Flowers"

WORK_COOLDOWN = 3600 # 1 hour in seconds

# Market economy
REVENUE_BASE_MULTIPLIER = 200  # starting multiplier for level-0 companies
REVENUE_INNER_EXP       = 0.25   # per-user char-count exponent
REVENUE_OUTER_EXP       = 0.75   # outer exponent — diminishing returns on user count
LEVEL_BASE_THRESHOLD    = 1_000_000
COST_FACTOR             = 0.05
DIVIDEND_PROFIT_SHARE   = Decimal("0.40")
LEVEL_UP_TREASURY_CONSUME = 0.80

DILUTION_MAX_RATE     = Decimal("0.025")  # cap at 2.5 % of total shares per week
DILUTION_PROFIT_SCALE = 100_000           # profit at which max rate is reached
DILUTION_DISCOUNT     = Decimal("0.95")   # dilution price = best_bid * 0.95

# Waifu system
WAIFU_BASE_VALUE = 5000
WAIFU_VALUE_MULTIPLIER = 1.5   # value after buy = max(paid, current) * 1.5
WAIFU_DECAY_RATE = 0.10        # lose 10% of excess per day
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