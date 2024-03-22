import logging
import sqlite3
import pandas as pd
import sqlite3

logger = logging.getLogger(__name__)

def create_connection(db_file):
    """Create a database connection to the SQLite database specified by db_file"""
    try:
        conn = sqlite3.connect(db_file)
        logger.info("Database connection established.")
        return conn
    except sqlite3.Error as e:
        logger.error(e, exc_info=True)
    return None

def create_table(conn):
    """Create a table from the create_table_sql statement"""
    
    sql_create_trades_table = """ CREATE TABLE IF NOT EXISTS trades (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                tradePairID REAL,
                                accountId TEXT,
                                currency TEXT,
                                dateTime DATETIME,
                                assetCategory TEXT,
                                symbol TEXT,
                                description TEXT,
                                conid TEXT,
                                listingExchange TEXT,
                                multiplier REAL,
                                strike REAL,
                                expiry DATE,
                                putCall TEXT,
                                tradeID TEXT,
                                transactionID TEXT,
                                reportDate DATE,
                                tradeDate DATE,
                                exchange TEXT,
                                quantity REAL,
                                tradePrice REAL,
                                netCash REAL,
                                buySell TEXT,
                                ibOrderID TEXT,
                                openCloseIndicator TEXT,
                                orderType TEXT,
                                cost REAL,
                                fifoPnlRealized REAL,
                                mtmPnl REAL,
                                underlyingSymbol TEXT,
                                underlyingConid TEXT,
                                isAPIOrder TEXT,
                                ibCommission REAL,
                                ibCommissionCurrency TEXT
                            );
                            """

    
    try:
        c = conn.cursor()
        c.execute(sql_create_trades_table)
        logger.info("Table created in database successfully.")
    except sqlite3.Error as e:
        logger.error(e, exc_info=True)
    
def add_trade(conn, row):
    """
    Insert a single row into the trades table, ensuring all data types match the table schema.
    """
    sql_insert_trade = ''' INSERT INTO trades(tradePairID, accountId, currency, dateTime, assetCategory, symbol, description, conid,
                            listingExchange, multiplier, strike, expiry, putCall, tradeID, transactionID, reportDate,
                            tradeDate, exchange, quantity, tradePrice, netCash, buySell, ibOrderID, openCloseIndicator,
                            orderType, cost, fifoPnlRealized, mtmPnl, underlyingSymbol, underlyingConid, isAPIOrder,
                            ibCommission, ibCommissionCurrency)
                          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''

    # Convert dateTime, reportDate, and tradeDate to strings, and ensure numeric fields are properly formatted
    formatted_row = row.copy()
    formatted_row['tradePairID'] = 0
    formatted_row['dateTime'] = row['dateTime'].strftime('%Y-%m-%d %H:%M:%S') if row['dateTime'] else None
    formatted_row['reportDate'] = row['reportDate'].strftime('%Y-%m-%d') if row['reportDate'] else None
    formatted_row['tradeDate'] = row['tradeDate'].strftime('%Y-%m-%d') if row['tradeDate'] else None

    # Numeric fields conversion
    numeric_fields = ['multiplier', 'strike', 'quantity', 'tradePrice', 'netCash', 'cost', 'fifoPnlRealized', 'mtmPnl', 'ibCommission']
    for field in numeric_fields:
        if formatted_row[field] is not None:
            try:
                formatted_row[field] = float(formatted_row[field])
            except ValueError:
                formatted_row[field] = None  # or some default value if appropriate

    trade_data = (
        formatted_row['tradePairID'], formatted_row['accountId'], formatted_row['currency'], formatted_row['dateTime'], formatted_row['assetCategory'], formatted_row['symbol'], formatted_row['description'],
        formatted_row['conid'], formatted_row['listingExchange'], formatted_row['multiplier'], formatted_row['strike'], formatted_row['expiry'], formatted_row['putCall'],
        formatted_row['tradeID'], formatted_row['transactionID'], formatted_row['reportDate'], formatted_row['tradeDate'], formatted_row['exchange'], formatted_row['quantity'],
        formatted_row['tradePrice'], formatted_row['netCash'], formatted_row['buySell'], formatted_row['ibOrderID'], formatted_row['openCloseIndicator'],
        formatted_row['orderType'], formatted_row['cost'], formatted_row['fifoPnlRealized'], formatted_row['mtmPnl'], formatted_row['underlyingSymbol'],
        formatted_row['underlyingConid'], str(formatted_row['isAPIOrder']), formatted_row['ibCommission'], formatted_row['ibCommissionCurrency']
    )
    
    logger.info(f"Inserting trade: {formatted_row['openCloseIndicator']} {formatted_row['tradeDate']} {formatted_row['buySell']} {abs(formatted_row['quantity'])} {formatted_row['symbol']} @ {formatted_row['tradePrice']} {formatted_row['currency']} {formatted_row['fifoPnlRealized']}")
    
    try:
        c = conn.cursor()
        c.execute(sql_insert_trade, trade_data)
        conn.commit()
        logger.info("Trade row inserted successfully.")
    except sqlite3.Error as e:
        logger.error(e, exc_info=True)

def add_new_trades_to_database(df, conn):
    """
    Check if trades already exist in the database based on ibOrderID and tradeDate combination
    :param df: DataFrame containing trades
    :param conn: SQLite database connection
    """
    for index, row in df.iterrows():
        ibOrderID = row['ibOrderID']
        tradeDate = row['tradeDate']
        
        # Query to check if trade already exists in the database
        sql = "SELECT COUNT(*) FROM trades WHERE ibOrderID = ? AND tradeDate = ?"
        cur = conn.cursor()
        cur.execute(sql, (ibOrderID, tradeDate))
        result = cur.fetchone()
        
        if result[0] > 0:
            logger.info(f"Trade with ibOrderID '{ibOrderID}' and tradeDate '{tradeDate}' already exists in the database.")
        else:
            logger.info(f"Trade with ibOrderID '{ibOrderID}' and tradeDate '{tradeDate}' does not exist in the database.")
            add_trade(conn, row)

def fetch_trades(conn):
    logger.info("Fetching trades from database.")
    try:
        cursor = conn.cursor()

        query = """
        SELECT tradePairID, dateTime, buySell, quantity, symbol, tradePrice, openCloseIndicator, cost, netCash, fifoPnlRealized,
            mtmPnl, ibCommission, assetCategory, description, listingExchange, multiplier, strike, expiry, putCall,
            underlyingSymbol, tradeDate, exchange, ibOrderID, tradeID, orderType, isAPIOrder, currency
        FROM trades
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        logger.info(f"Fetched {len(rows)} trades from the database.")
    except sqlite3.Error as e:
        logger.error(f"Error fetching trades from database: {e}")
        rows = []
        
    return rows