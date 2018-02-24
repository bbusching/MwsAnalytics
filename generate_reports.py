from datetime import datetime, timedelta
import os
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET

from mws import mws

# Constants
_SLEEP_TIME = 3  # seconds


# Models
class Purchase(Object):

    def __init__(self, order_id, purchase_date, sku):
        self.order_id = order_id
        self.purchase_date = purchase_date
        self.sku = sku

    def write_to_db(self, db_conn):
        cursor = db_conn.cursor()
        cursor.execute('INSERT INTO purchases VALUES (?, ?, ?)',
                       (self.order_id, self.purchase_date, self.sku))
        cursor.commit()


# Methods
def _build_mws():
    """
    Returns relevant secrets for accessing the MWS API.

    Current implementation expects them as environment variables.
    """
    access_key = os.environ['MWS_ACCESS_KEY']
    secret_key = os.environ['MWS_SECRET_KEY']
    account_id = os.environ['MWS_SELLER_ID']

    return mws.Reports(
        access_key=access_key, secret_key=secret_key, account_id=account_id)


def _open_db():
    """
    Returns the sqlite database connection.

    Also creates the purchases table if it does not exist.
    """
    db = sqlite3.connect('mws.db')

    # Create database table.
    try:
        cursor = db.cursor()
        cursor.execute('''CREATE TABLE purchases
                          (order_id text, purchase_date text, sku text)''')
        cursor.commit()
    except sqlite3.OperationalError:
        # Catch table already exists errors.

    return db


def _request_report(mws_conn):
    """
    Returns the string-represented report_request_id from MWS.
    """
    now = datetime.now()
    start_date = datetime.now() - timedelta(days=7)

    try:
        response = mws_conn.request_report(
            '_GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_', start_date.isoformat(),
            now.isoformat())
    except MWSError as exc:
        print('Error requesting report from MWS.\n\n{0}'.format(exc),
              sys.stderr)
        sys.exit()

    return response.parsed['ReportRequestInfo']['ReportRequestId']['value']


def _poll_for_report(mws_conn, report_request_id):
    """
    Returns the generated report_id.

    Polls MWS report request list for a particular report_request_id.

    Args:
      mws: The MWS object.
      report_request_id: The string-represented integer representing this runs report 
                         request.
    """
    retry_count = 0
    while True:
        retry_count += 1
        try:
            response = mws_conn.get_report_request_list(requestids=[request_id])
            break
        except MWSError as exc:
            print('Error requesting report request list.\n\n{0}'.format(exc))
            if retry_count == 3:
                time.sleep(_SLEEP_TIME)
                continue
            else:
                sys.exit()

    return response.parsed['ReportRequestInfo']['GeneratedReportId']['value']


def _process_report(mws_conn, report_id, db_conn):
    """
    Gets the report DictWrapper object from MWS and writes it to storage.

    Args:
      mws_conn: The MWS object.
      report_id: The string-represented integer representing this report.
    """
    try:
        response = mws_conn.get_report(report_id)
    except MWSError as exc:
        print('Error requesting report from MWS.\n\n{0}'.format(exc),
              sys.stderr)
        sys.exit()

    root = ET.fromstring(response.original)
    for message in root.findall('Message'):
        order_id = message.find('Order/AmazonOrderId').text
        purchase_date = message.find('Order/PurchaseDate').text
        sku = message.find('OrderItem/SKU').text

        purchase = Purchase(order_id, purchase_date, sku)
        purchase.write_to_db(db_conn)


def main():
    """Requests and store an orders report.

    Calls the MWS Reports API to generate a report of all sales in a given time
    period. The data is exported for use in analytics.
    """
    mws_conn = _build_mws()

    report_request_id = _request_report(mws_conn)
    report_id = _poll_for_report(mws_conn, report_request_id)
    db_conn = _open_db()
    report = _process_report(mws_conn, report_id)


if __name__ == "__main__":
    main()
