import sqlite3
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Google Sheets setup
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('trading-396101-57412d0af674.json', scope)
client = gspread.authorize(creds)

# Open the spreadsheet
sheet = client.open("Trading-Journal").sheet1

# SQLite database connection
conn = sqlite3.connect('/home/allay/servershare/trading_data.db')
cursor = conn.cursor()

# Query to select the data you want
query = """
SELECT dateTime, buySell, quantity, symbol, tradePrice, openCloseIndicator, cost, netCash, fifoPnlRealized, 
       mtmPnl, ibCommission, assetCategory, description, listingExchange, multiplier, strike, expiry, putCall, 
       underlyingSymbol, tradeDate, exchange, ibOrderID, orderType, isAPIOrder, currency 
FROM trades
"""

cursor.execute(query)
rows = cursor.fetchall()


# Assuming 'rows' contains the fetched data
data = [list(row) for row in rows]  # Ensure row data is in a list format suitable for batch update

# Batch update to avoid quota limit errors and address deprecation warning
try:
    range_name = 'A2'  # Starting cell for the batch update
    sheet.update(range_name=range_name, values=data, value_input_option='RAW')
except gspread.exceptions.APIError as e:
    print(f"API Error: {e}")
    # Optional: Implement retry logic or logging here
    
# Close the database connection
conn.close()
