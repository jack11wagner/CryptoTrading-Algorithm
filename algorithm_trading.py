import quandl as q
import pandas as pd
import matplotlib.pyplot as plt

symbol= "XOM"
q.ApiConfig.api_key = "LwpQ3yJ6eLNbK5JfektZ"
msft_data = q.get("EOD/{}".format(symbol), start_date = "2010-01-01", end_date = "2019-01-01")
msft_data.head()

daily_close = msft_data[['Adj_Close']]
daily_return = daily_close.pct_change()
daily_return.fillna(0,inplace=True)
print(daily_return*100)
mdata = msft_data.resample('M').apply(lambda x: x[-1])

monthly_return = mdata.pct_change()

short_lb = long_lb = 120


adj_price = msft_data['Adj_Close']
moving_avg = adj_price.rolling(window=50).mean()
print(moving_avg[-10:])
adj_price.plot()
plt.title(symbol + ' Adjusted Price')
moving_avg.plot(color='red')
plt.show()
