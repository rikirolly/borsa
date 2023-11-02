import pandas as pd
import numpy as np
import dask.bag as db
import mplfinance as mpf
from dask.distributed import Client, LocalCluster

GIORNI = 30
ORE_GIORNO = 8

def generate_random_input(size=60*ORE_GIORNO*GIORNI, steps_per_minute=100, variance=0.05):
	# L'apertura iniziale è 100
	_open = [100]
	_close = []
	_high = []
	_low = []
	
	for i in range(size):
		# Genera passeggiata casuale per il minuto corrente
		walk = [_open[-1]]
		for s in range(steps_per_minute - 1):
			walk.append(walk[-1] + np.random.randn() * variance)
		
		_close.append(walk[-1])
		_high.append(max(walk))
		_low.append(min(walk))
		
		# L'apertura del minuto successivo è la chiusura di questo minuto più una variazione casuale
		next_open = _close[-1] + np.random.randn() * variance
		_open.append(next_open)
	
	# Rimuovi l'ultimo valore di _open perché non ci sarà un periodo dopo l'ultimo chiuso
	_open.pop()

	_bid_ask_spread = np.random.rand(size) * 0.5  # tra 0.01% e 0.5%

	return zip(_open, _high, _low, _close, _bid_ask_spread)

STOP_LOSS_HIGH_VOLATILITY = 15 # Titoli molto volatili: 10% - 15%
STOP_LOSS_MEDIUM_VOLATILITY = 10 # Titoli moderatamente volatili: 7% - 10%
STOP_LOSS_LOW_VOLATILITY = 5 # Titoli meno volatili: 2% - 5%

STOP_GAIN_CONSERVATIVE = 10 # Obiettivo conservativo: 5% - 10%
STOP_GAIN_MODERATE = 15 # Obiettivo moderato: 10% - 15%
STOP_GAIN_AGGRESSIVE = 20 # Obiettivo aggressivo: 15% - 20% o più
NO_STOP_GAIN = 10000

class MinuteData:
	def __init__(self, 
				 open_price: float,
				 high_price: float,
				 low_price: float,
				 close_price: float,
				 bid_ask_spread: float):
		self.open_price = open_price
		self.high_price = high_price
		self.low_price = low_price
		self.close_price = close_price
		self.bid_ask_spread = bid_ask_spread

class Trade:
	def __init__(self, 
				trailing_stop_loss: float,
				trailing_stop_gain: float,
				entry_price: float,
				buy):
		self.trailing_stop_loss = trailing_stop_loss
		self.trailing_stop_gain = trailing_stop_gain
		self.entry_price = entry_price
		self.exit_price = 0
		self.current_gain = 0
		self.current_loss = 0
		self.length = 0
		self.result = 0
		self.open = True
		self.buy = buy
		if buy == True:
			self.peek_price = 0
		else:
			self.peek_price = 9999999999999999

	def update(self, minuteData: MinuteData):
		self.length += 1
		if self.buy == True:
			# update peak
			if minuteData.high_price > self.peek_price:
				self.peek_price = minuteData.high_price
			# compute results
			self.current_gain = 100 * (minuteData.close_price - self.entry_price) / self.entry_price
			self.current_loss = 100 * (self.peek_price - minuteData.close_price) / self.peek_price
			# check if i can continue on current trading
			if self.current_gain > self.trailing_stop_gain or self.current_loss > self.trailing_stop_loss:
				self.exit_price = minuteData.low_price
				self.result = 100 * (self.exit_price - self.entry_price) / self.entry_price  # (NON FAREI COSI) use low next trading as worst case
				self.open = False
		else:
			# update peak
			if minuteData.low_price < self.peek_price:
				self.peek_price = minuteData.low_price
			# compute results
			self.current_gain = 100 * (self.entry_price - minuteData.close_price) / self.entry_price
			self.current_loss = 100 * (minuteData.close_price - self.peek_price) / self.peek_price

			# check if i can continue on current trading		
			if self.current_gain > self.trailing_stop_gain or self.current_loss > self.trailing_stop_loss:
				self.exit_price = minuteData.high_price
				self.result = 100 * (self.entry_price - self.exit_price) / self.entry_price  # (NON FAREI COSI) use low next trading as worst case
				self.open = False

def update_open_trade(minuteData, open_trades):
	# Update trades based on new data
	for trade in open_trades:
		if not trade.open:
			open_trades.remove(trade)
		else:
			trade.update(minuteData)
		
	return open_trades

def manage_trade(trade_type, data):
	trailing_stop_loss, trailing_stop_gain, buy = trade_type
	open_trades_history = []
	open_trades = []
	for d in data:
		_open, _high, _low, _close, _bid_ask_spread = d
		minuteData = MinuteData(open_price=_open, high_price=_high, low_price=_low, close_price=_close, bid_ask_spread=_bid_ask_spread)

		trade = Trade(trailing_stop_loss=trailing_stop_loss, 
					trailing_stop_gain=trailing_stop_gain, 
					entry_price=minuteData.open_price * (1 + minuteData.bid_ask_spread/100), 
					buy=buy)
		open_trades.append(trade)
		# Update open trades
		open_trades = update_open_trade(minuteData, open_trades)
		open_trades_history.append(open_trades.copy())


	
	return open_trades_history

def manage_stock(stock):

	trade_types_config = tuple((loss, NO_STOP_GAIN, True) for loss in (i * 0.5 for i in range(1, 5)))
	trade_types_bag = db.from_sequence(trade_types_config)
	trade_types_dag = trade_types_bag.map(manage_trade, stock)
	trade_types = trade_types_dag.compute()
	return trade_types

def do_manage_trading(stocks):
	stocks_bag = db.from_sequence(stocks)
	stock_trades_dag = stocks_bag.map(manage_stock)
	stock_trades = stock_trades_dag.compute()
	return stock_trades

def plot_volume(_open, _high, _low, _close, volume):
    # Crea un indice DatetimeIndex fittizio
    date_range = pd.date_range(start="2023-01-01", periods=len(_open), freq="T")
    
    df = pd.DataFrame({
        'Open': _open,
        'High': _high,
        'Low': _low,
        'Close': _close,
        'Volume': volume
    }, index=date_range)
    
    # Ora non c'è più bisogno di gestire buy_positions e sell_positions
    # Si può procedere direttamente con il plot includendo il volume
    
    mpf.plot(df, type='candle', style='charles', title='OHLC Candlestick Chart', volume=True)

def plot_ohlc(_open, _high, _low, _close, buy_positions=None, sell_positions=None):
	# Crea un indice DatetimeIndex fittizio
	date_range = pd.date_range(start="2023-01-01", periods=len(_open), freq="T")
	
	df = pd.DataFrame({
		'Open': _open,
		'High': _high,
		'Low': _low,
		'Close': _close
	}, index=date_range)
	
	ap = []
	
	if buy_positions is not None:
		buy_markers = [np.nan] * len(_open)
		for i in range(len(_open)):
			if buy_positions[i]:
				buy_markers[i] = _low[i]  # o posizionarlo dove preferisci
		ap.append(mpf.make_addplot(buy_markers, type='scatter', markersize=100, marker='^', color='g'))
	
	if sell_positions is not None:
		sell_markers = [np.nan] * len(_open)
		for i in range(len(_open)):
			if sell_positions[i]:
				sell_markers[i] = _high[i]  # o posizionarlo dove preferisci
		ap.append(mpf.make_addplot(sell_markers, type='scatter', markersize=100, marker='v', color='r'))
	
	mpf.plot(df, type='candle', style='charles', title='OHLC Candlestick Chart', volume=False, addplot=ap)



if __name__ == '__main__':
	cluster = LocalCluster()
	client = Client(cluster)
	stocks = [generate_random_input()]
	stock_trades = do_manage_trading(stocks)

	# PLOT PRIMA STOCK (estrapola buy_positions, sell_positions)

	# buy_positions = []
	# _len_buy = []
	# sell_positions = []
	# _len_sell = []

	# index = 0
	# prev_trade = None
	# for obt in stock_trades[0][0]: # BUY
	# 	trade = obt[len(obt)-1]
	# 	if index+1 < len(stock_trades[0][0]) and trade.result > 0 and (prev_trade is None or prev_trade.result <= 0):
	# 		buy_positions.append(True)
	# 		_len_buy.append(trade.length)
	# 	else:
	# 		buy_positions.append(False)
	# 		_len_buy.append(0)
	# 	prev_trade = trade
	# 	index += 1

	# index = 0
	# prev_trade = None
	# for obt in stock_trades[0][1]: # SELL
	# 	trade = obt[len(obt)-1]
	# 	if index+1 < len(stock_trades[0][0]) and trade.result > 0 and (prev_trade is None or prev_trade.result <= 0):
	# 		sell_positions.append(True)
	# 		_len_sell.append(trade.length)
	# 	else:
	# 		sell_positions.append(False)
	# 		_len_sell.append(0)
	# 	prev_trade = trade
	# 	index += 1

	volume = []
	stop_loss_distribution = []
	
	for obt in stock_trades[0]:
		index = 0
		for trades in obt:
			num_positive_trade_closed = 0
			total_positive_gain = 0
			for trade in trades:
				if not trade.open and trade.current_gain > 0:
					num_positive_trade_closed += 1
					total_positive_gain += trade.current_gain
				if len(stop_loss_distribution)<len(obt):
					# volume.append(total_positive_gain)
					stop_loss_distribution.append({trade.trailing_stop_loss: total_positive_gain})
				else:
					# volume[index] += total_positive_gain
					stop_loss_distribution[index][trade.trailing_stop_loss] = total_positive_gain
			index += 1	

	for sld in stop_loss_distribution:
		max = 0
		best_stop_loss = 0.0
		for k, v in sld.items():
			if v>max:
				best_stop_loss = k
		volume.append(best_stop_loss)

	_open, _high, _low, _close, _bid_ask_spread = zip(*stocks[0])
	plot_volume(_open, _high, _low, _close, volume)
	# plot_ohlc(_open, _high, _low, _close, buy_positions, sell_positions)