from ibflex import client
from ibflex import parser
import subprocess
import pandas as pd

def download_trades(filename, query, query_id):
    #flexget -t query -q query_id > trades_data_file
    # Your shell command as a string
    command = "flexget -t "+str(query)+" -q "+str(query_id)

    # Running the shell command and redirecting output to the file
    with open(filename, "w") as file:
        subprocess.run(command.split(), stdout=file)
    
def extract_trades_from_file(filename):
    response = parser.parse(filename)
    stmt = response.FlexStatements[0]
    return stmt.Trades

def parse_trades_to_df(trades):
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
    
    df['openCloseIndicator'] = df['openCloseIndicator'].apply(lambda x: x.name if x is not None else "None")
    df['assetCategory'] = df['assetCategory'].apply(lambda x: x.name if x is not None else "None")
    df['buySell'] = df['buySell'].apply(lambda x: x.name if x is not None else "None")
    df['putCall'] = df['putCall'].apply(lambda x: x.name if x is not None else "None")
    df['orderType'] = df['orderType'].apply(lambda x: x.name if x is not None else "None")
    return df

