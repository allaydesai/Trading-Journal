import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)

def initialize_google_sheets_api(api_credentials):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
    
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(api_credentials, scope)
        client = gspread.authorize(creds)
        logger.info("Google Sheets API successfully initialized.")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets API: {e}")
        return None

def update_sheet(sheet, trades, trade_id_column_index):
    logger.info(f"Updating sheet: {sheet.title} with new trades.")
    try:
        existing_trade_ids = sheet.col_values(trade_id_column_index)
        new_trades = [trade for trade in trades if trade[-4] not in existing_trade_ids]

        if new_trades:
            sheet.append_rows(new_trades)
            logger.info(f"Added {len(new_trades)} new trades to the sheet: {sheet.title}.")
        else:
            logger.info("No new trades to add.")
    except Exception as e:
        logger.error(f"Failed to update sheet: {sheet.title}. Error: {e}")