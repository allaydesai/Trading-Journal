import logging
from ibflex import client
from ibflex import parser
import subprocess
import pandas as pd

logger = logging.getLogger(__name__)

def download_trades(filename, query, query_id):
    command = f"flexget -t {query} -q {query_id}"
    logger.info(f"Downloading trades data with command: {command}")

    try:
        with open(filename, "w") as file:
            subprocess.run(command.split(), stdout=file, check=True)
        logger.info(f"Trades data successfully downloaded and saved to {filename}.")
    except subprocess.CalledProcessError as e:
        logger.error(f"An error occurred while downloading trades data: {e}", exc_info=True)
    
def extract_trades_from_file(filename):
    logger.info(f"Extracting trades from file: {filename}")
    try:
        response = parser.parse(filename)
        stmt = response.FlexStatements[0]
        trades = stmt.Trades
        logger.info(f"Successfully extracted trades from {filename}.")
        return trades
    except Exception as e:
        logger.error(f"Failed to extract trades from {filename}: {e}", exc_info=True)
        return None


def parse_trades_to_df(trades):
    logger.info("Parsing trades to DataFrame.")

    if trades is None:
        logger.error("No trades data to parse.")
        return pd.DataFrame()  # Return an empty DataFrame
    
    # Define the columns based on the updated attributes for options trading
    columns = [
        'accountId', 'currency', 'dateTime', 'assetCategory', 'symbol', 'description',
        'conid', 'listingExchange', 'multiplier', 'strike', 'expiry',
        'putCall', 'tradeID', 'transactionID', 'reportDate', 'tradeDate',
        'exchange', 'quantity', 'tradePrice', 'netCash', 'buySell',
        'ibOrderID', 'openCloseIndicator', 'orderType', 'cost',
        'fifoPnlRealized', 'mtmPnl', 'underlyingSymbol', 'underlyingConid',
        'isAPIOrder', 'ibCommission', 'ibCommissionCurrency'
    ]

    # Initialize an empty list to store each trade's data
    data = []

    # Iterate over each trade object
    for trade in trades:
        # Extract the relevant attributes from each trade object
        trade_data = {attr: getattr(trade, attr) for attr in columns}
        data.append(trade_data)

    # Create a DataFrame from the collected data
    df = pd.DataFrame(data, columns=columns)
    logger.info("Successfully parsed trades to DataFrame.")
    
    df['openCloseIndicator'] = df['openCloseIndicator'].apply(lambda x: x.name if x is not None else "None")
    df['assetCategory'] = df['assetCategory'].apply(lambda x: x.name if x is not None else "None")
    df['buySell'] = df['buySell'].apply(lambda x: x.name if x is not None else "None")
    df['putCall'] = df['putCall'].apply(lambda x: x.name if x is not None else "None")
    df['orderType'] = df['orderType'].apply(lambda x: x.name if x is not None else "None")
    return df

def split_trades(rows):
    logger.info("Splitting trades into open and close.")
    open_trades, close_trades = [], []
    for row in rows:
        if row[6] == 'OPEN':  
            open_trades.append(row)
        else:
            close_trades.append(row)
    logger.info(f"Split into {len(open_trades)} open trades and {len(close_trades)} close trades.")
    return open_trades, close_trades