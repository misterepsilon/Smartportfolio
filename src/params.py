

DATA_FOLDER = "..\data"

##########################################################################
###                         DATES                                      ###
##########################################################################
START_DATE = '2020-01-01'    # ✅ Période de base (2016-2024)
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