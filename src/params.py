

DATA_FOLDER = "..\data"

##########################################################################
###                         DATES                                      ###
##########################################################################
START_DATE = '2024-01-01'    # ✅ Période de base (2016-2024)
# START_DATE = '2010-01-01'  # Test avec plus de cycles (inclut 2008-2012)
# START_DATE = '2018-01-01'  # Test post bull-market (période plus difficile)
# START_DATE = '2020-01-01'  # Test COVID uniquement (volatilité extrême)
END_DATE = '2024-12-31'
# END_DATE = '2019-12-31'    # Test pré-COVID (exclut la pandémie)



##########################################################################
###                         ROLLING METRICS                            ###
##########################################################################
ROLLING_WINDOW_DAYS = 21*12  # Fenêtre rolling en jours
ROLLING_STEP_SIZE = 1     # Fréquence de recalcul (1 = chaque jour, 5 = chaque semaine, etc.)
RISK_FREE_RATE = 0.01
RISK_FREE_RATE_DAILY = RISK_FREE_RATE / 252


##########################################################################
###                         OPTIMIZATION                               ###  
##########################################################################
REBALANCING_FREQUENCY = 'monthly' # Options: 'daily', 'weekly', 'monthly', 'quarterly'
REBALANCE_FREQ_CODES = {"weekly": "W-MON", 
                        "monthly": "MS", 
                        "quarterly": "QS", 
                        "yearly": "YS"}
RANDOM_SEED = 42
USE_DRAWDOWN_PENALTY = False
RHO_DRAWDOWN = 400  # higher = more penalizing
MAX_DRAWDOWN = -0.15 
UPPER_BOUND = 0.3  # Upper bound for portfolio weights (max per stock)
LOWER_BOUND = 0.0  # Lower bound for portfolio weights (min per stock)



##########################################################################
###                         SCREENING                                  ###
##########################################################################

SCREENING_CRITERIA = "technical_health"  # Options: 'liquidity', 'market_cap', 'momentum', 'volatility', 
                                                        # 'technical_health', 'liquidity_market_cap', 'liquidity_market_cap_momentum', 
                                                        # 'liquidity_market_cap_momentum_volatility', 'complete_pipeline'

### MARKET CAP
N_PERCENTILES = .3  # Percentile for market cap filtering

### LIQUIDITY
LIQ_LOOKBACK_DAYS = 20  # Lookback period for liquidity
LIQ_MIN_VOLUME_USD = 5000000  # Minimum USD volume for liquidity
LIQ_MIN_PRICE = 5.0  # Minimum price for liquidity
LIQ_MAX_PRICE = 2500.0  # Maximum price for liquidity
LIQ_MIN_VOLUME_COVERAGE = 0.7  # Minimum data coverage ratio for volume window

### MOMENTUM
MOM_MIN_RETURN_6M = 0.05  # Minimum 6-month return for momentum 
MOM_MIN_RETURN_12M = 0.1  # Minimum 12-month return for momentum 
MOM_EXCLUDE_LAST_MONTH = True  # Exclude last month for momentum 

### VOLATILITY
VOL_MAX_THRESHOLD = 0.8  # Maximum threshold for volatility
VOL_LOOKBACK_DAYS = 63  # Lookback period for volatility
VOL_MIN_DATA_RATIO = 0.8  # Minimum data ratio for volatility
VOL_MIN_N_RETURNS = 10    # Minimum number of valid returns to compute std

### TECHNICAL HEALTH
TH_LOOKBACK_DAYS = 10  # Lookback period for technical health
TH_CLIP_PERCENTILE = 95  # Percentile for clipping gaps
TH_MIN_STABILITY_SCORE = 0.7  # Minimum stability score
TH_SMA_PERIOD = 50  # Simple Moving Average period
TH_RSI_PERIOD = 14  # Relative Strength Index period
TH_MAX_GAP_PCT = 0.10  # Maximum gap percentage
TH_MIN_SMA_RATIO = 1.0  # Minimum SMA ratio
TH_RSI_RANGE = (30, 70)  # RSI range for filtering

### COMPOSITE SCREENING
COMPOSITE_SCREENING_WEIGHTS = {
    'w_liquidity': 0.6,
    'w_market_cap': 0.6,
    'w_momentum_6m': 2,
    'w_momentum_12m': 2,
    'w_volatility': 1.0,
    'w_sma_ratio': 0.7,
    'w_rsi': 0.3,
    'w_stability': 4,
    'w_gap': 0.2,
}
COMPOSITE_SCORE_NAN_PENALTY = 0
COMPOSITE_SCORE_TOP_N = 50

