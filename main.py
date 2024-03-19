import pandas as pd
import datetime as dt

import database as db
import ib_trades as ibt


# Global variables for database and table names
DB_FILE = '/home/allay/servershare/trading_data.db'
TABLE_NAME = 'trades'
CREATE_TABLE = True
QUERY = 957448361226423544216934
QUERY_ID = 904255

# get today's date and store in a variable
today = dt.date.today()
trades_data_file = '/home/allay/servershare/trades-download/'+today.strftime('%Y-%m-%d')+'_ibkr.xml'

def main():
    print(f'Updating Trading Journal at {dt.datetime.now()}')
    
    print(f'Downloading trades data from IBKR')
    # Download the trades data from IBKR
    ibt.download_trades(trades_data_file, QUERY, QUERY_ID)
    
    print(f'Parsing trades data')
    trades = ibt.extract_trades_from_file(trades_data_file)
    df_trades = ibt.parse_trades_to_df(trades)

    print(f'Adding trades to database')
    # Create a database connection
    conn = db.create_connection(DB_FILE)
    # create a new table
    if CREATE_TABLE:
        print(f'Creating table {TABLE_NAME} in database {DB_FILE}')
        db.create_table(conn)
    # Add the trades to the database
    db.add_new_trades_to_database(df_trades, conn)
    # close the database connection
    conn.close()

if __name__ == '__main__':
    main()