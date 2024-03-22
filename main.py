import logging
import pandas as pd
import datetime as dt

import database as db
import ib_trades as ibt
import sheets_sync as ss

# Global variables for database and table names
DB_FILE = '/home/allay/servershare/trading_data.db'
TABLE_NAME = 'trades'
CREATE_TABLE = True
QUERY = 957448361226423544216934
QUERY_ID = 904255

GOOGLE_SHEETS_API_CREDENTIALS = '/home/allay/servershare/Trading-Journal/trading-396101-57412d0af674.json'
TRADE_ID_COLUMN_INDEX = 24  # Replace with the index of the tradeID column in your sheet
TRADING_JOURNAL_FILE_NAME = "Trading-Journal"

# get today's date and store in a variable
today = dt.date.today()
trades_data_file = '/home/allay/servershare/trades-download/'+today.strftime('%Y-%m-%d')+'_ibkr.xml'

# Configure logging
logging.basicConfig(filename='/home/allay/servershare/Trading-Journal/logs/trading-journal-'+today.strftime('%Y-%m-%d')+'.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info(f'Updating Trading Journal at {dt.datetime.now()}')
    
    logger.info(f'Downloading trades data from IBKR')
    ibt.download_trades(trades_data_file, QUERY, QUERY_ID)
    
    logger.info(f'Parsing trades data')
    trades = ibt.extract_trades_from_file(trades_data_file)
    df_trades = ibt.parse_trades_to_df(trades)

    logger.info(f'Adding trades to database')
    conn = db.create_connection(DB_FILE)
    if CREATE_TABLE:
        logger.info(f'Creating table {TABLE_NAME} in database {DB_FILE}')
        db.create_table(conn)
    db.add_new_trades_to_database(df_trades, conn)
    
    logger.info(f'Updating Google Sheets')
    client = ss.initialize_google_sheets_api(GOOGLE_SHEETS_API_CREDENTIALS)
    
    # Process trades
    try:
        sheet_open_positions = client.open(TRADING_JOURNAL_FILE_NAME).worksheet("Open")
        sheet_close_positions = client.open(TRADING_JOURNAL_FILE_NAME).worksheet("Close")

        rows = db.fetch_trades(conn)
        open_trades, close_trades = ibt.split_trades(rows)
        logger.info("Updating google sheet with new open trades.")
        ss.update_sheet(sheet_open_positions, open_trades, TRADE_ID_COLUMN_INDEX)
        logger.info("Updating google sheet with new close trades.")
        ss.update_sheet(sheet_close_positions, close_trades, TRADE_ID_COLUMN_INDEX)
        logger.info("Google sheet updated successfully.")
    except Exception as e:
        logger.error(f"An error occurred during main execution: {e}")
    
    conn.close()

if __name__ == '__main__':
    main()