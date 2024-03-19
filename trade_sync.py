import sqlite3
import gspread
from oauth2client.service_account import ServiceAccountCredentials

GOOGLE_SHEETS_API_CREDENTIALS = 'trading-396101-57412d0af674.json'
PATH_TO_DATABASE = '/home/allay/servershare/trading_data.db'
TRADE_ID_COLUMN_INDEX = 24  # Replace with the index of the tradeID column in your sheet
TRADING_JOURNAL_FILE_NAME = "Trading-Journal"

def fetch_trades():
    conn = sqlite3.connect(PATH_TO_DATABASE)
    cursor = conn.cursor()
    
    query = """
    SELECT tradePairID, dateTime, buySell, quantity, symbol, tradePrice, openCloseIndicator, cost, netCash, fifoPnlRealized, 
        mtmPnl, ibCommission, assetCategory, description, listingExchange, multiplier, strike, expiry, putCall, 
        underlyingSymbol, tradeDate, exchange, ibOrderID, tradeID, orderType, isAPIOrder, currency 
    FROM trades
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    return rows

def split_trades(rows):
    open_trades, close_trades = [], []
    for row in rows:
        if row[6] == 'OPEN':  
            open_trades.append(row)
        else:
            close_trades.append(row)
    return open_trades, close_trades

def update_sheet(sheet, trades, trade_id_column_index):
    existing_trade_ids = sheet.col_values(trade_id_column_index)
    # Prepare a list for batch update
    new_trades = [trade for trade in trades if trade[-4] not in existing_trade_ids]

    # If there are new trades to add, update the sheet in one batch
    if new_trades:
        sheet.append_rows(new_trades)

def main():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_API_CREDENTIALS, scope)
    client = gspread.authorize(creds)

    sheet_open_positions = client.open(TRADING_JOURNAL_FILE_NAME).worksheet("Open")
    sheet_close_positions = client.open(TRADING_JOURNAL_FILE_NAME).worksheet("Close")

    rows = fetch_trades()
    open_trades, close_trades = split_trades(rows)

    update_sheet(sheet_open_positions, open_trades, TRADE_ID_COLUMN_INDEX)
    update_sheet(sheet_close_positions, close_trades, TRADE_ID_COLUMN_INDEX)

if __name__ == "__main__":
    main()
