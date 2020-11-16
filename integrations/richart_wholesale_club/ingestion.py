import os

import numpy as np
import pandas as pd
from models import BranchProduct, Product
from integrations import Session
import logging
import re

logging.basicConfig(filename='integrations/integrations.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

session = Session()

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
ASSETS_DIR = os.path.join(PROJECT_DIR, "assets")
PRODUCTS_PATH = os.path.join(ASSETS_DIR, "PRODUCTS.csv")
PRICES_STOCK_PATH = os.path.join(ASSETS_DIR, "PRICES-STOCK.csv")
BATCH_SIZE = 100000


def process_csv_files():
    """Process csv files"""
    try:
        products_df = pd.read_csv(filepath_or_buffer=PRODUCTS_PATH, sep="|",)
        prices_stock_df = pd.read_csv(filepath_or_buffer=PRICES_STOCK_PATH, sep="|",)
    except Exception as e:
        logging.error(f"Error reading file, {e}")

    # process_products_file(products_df)
    # logging.info(f"PRODUCTS | Done")

    # create a map between sku and id for each product
    products = session.query(Product).with_entities(Product.id, Product.sku)
    sku_id_map = {product[0]:product[1] for product in products}

    process_stock(prices_stock_df, sku_id_map)
    logging.info(f"PRICES-STOCK | Done")



def process_products_file(products_df: pd.DataFrame):
    """Process the products file"""
    products = []
    file_size = len(products_df)
    logging.info(f"Processing PRODUCTS.csv file... {file_size} rows")
    for index, row in products_df.iterrows():
        if not pd.isnull(row["NAME"]):
            product = Product(sku=row["SKU"], store="Richart's")
            product.brand = row["BRAND"]
            product.barcodes = row["BARCODES"]
            product.description = cleanhtml(row["DESCRIPTION"])
            product.category = row["CATEGORY"]
            product.image_url = row["IMAGE_URL"]
            product.package = row["BUY_UNIT"]
            product.name = row["NAME"]
            products.append(product)
        else:
            logging.warning(f"PRODUCTS | Product without name! will not be included - index: {index}")

        bulk_size = len(products)
        if bulk_size == BATCH_SIZE:
            try:
                session.bulk_save_objects(products)
                session.commit()
                logging.info(f"Inserted {bulk_size} rows to PRODUCTS table")
                products = []
            except Exception as e:
                logging.exception(f"PRODUCTS error. msg: {e}")
                session.rollback()

def process_stock(prices_stock_df: pd.DataFrame, sku_id_map: dict):
    """Process the stock prices file"""

    file_size = len(prices_stock_df)
    logging.info(f"Processing PRICES-STOCK file... {file_size} rows")
    # cleaning null values, since nullable is false for all columns
    if prices_stock_df.isnull().values.sum() > 0:
        logging.warning(f"Number of rows with null values (will not be included): {prices_stock_df.isnull().values.sum()}")
        prices_stock_df = prices_stock_df.dropna(how='any',axis=0)
    prices_stock = []
    for index, row in prices_stock_df.iterrows():
        if (row["BRANCH"] == 'MM' or row["BRANCH"] == 'RHSM') and row["STOCK"] > 0:
            try:
                product_id = sku_id_map[row["SKU"]]
            except Exception as e:
                logging.error(f"PRICE-STOCK | could not map product id and sku: {e}")

            try:
                branch_product = BranchProduct(product_id= product_id)
                branch_product.branch = row["BRANCH"]
                branch_product.stock = row["STOCK"]
                branch_product.price = row["PRICE"]
                prices_stock.append(branch_product)
            except Exception as e:
                logging.error(f"PRICE-STOCK | error instanciating BranchProduct: {e}")

            bulk_size = len(prices_stock)
            if bulk_size == BATCH_SIZE:
                try:
                    session.bulk_save_objects(prices_stock)
                    session.commit()
                    logging.info(f"Inserted {bulk_size} rows to PRICES-STOCK table")
                    prices_stock = []
                except Exception as e:
                    logging.exception(f"PRICES-STOCK error. {e}")
                    session.rollback()


def cleanhtml(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext

if __name__ == "__main__":
    process_csv_files()
