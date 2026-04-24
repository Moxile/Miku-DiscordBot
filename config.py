


PREFIX = "."
MAIN_CURRENCY_EMOJI = "🌸"
CURRENCY_NAME = "Flowers"

WORK_COOLDOWN = 3600 # 1 hour in seconds

# Market economy
REVENUE_BASE_MULTIPLIER = 800   # starting multiplier for level-0 companies
REVENUE_INNER_EXP       = 0.25   # per-user char-count exponent
REVENUE_OUTER_EXP       = 0.75   # outer exponent — diminishing returns on user count
LEVEL_BASE_THRESHOLD    = 500_000
COST_FACTOR             = 0.05
DIVIDEND_REVENUE_SHARE  = 0.40
LEVEL_UP_TREASURY_CONSUME = 0.80

# Waifu system
WAIFU_BASE_VALUE = 5000
WAIFU_VALUE_MULTIPLIER = 1.5   # value after buy = max(paid, current) * 1.5
WAIFU_DECAY_RATE = 0.10        # lose 10% of excess per day
MARRIAGE_FEE = 10_000          # Flowers to propose
ENGAGEMENT_DAYS = 7            # days of mutual ownership before proposing
REMINDER_MAX_DAYS = 10         # max reminder duration