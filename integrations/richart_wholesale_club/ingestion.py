import os

import numpy as np
import pandas as pd

from models import BranchProduct, Product
from integrations import Session

session = Session()

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
ASSETS_DIR = os.path.join(PROJECT_DIR, "assets")
PRODUCTS_PATH = os.path.join(ASSETS_DIR, "PRODUCTS.csv")
PRICES_STOCK_PATH = os.path.join(ASSETS_DIR, "PRICES-STOCK.csv")


def process_csv_files():
    products_df = pd.read_csv(filepath_or_buffer=PRODUCTS_PATH, sep="|",)
    prices_stock_df = pd.read_csv(filepath_or_buffer=PRICES_STOCK_PATH, sep="|",)

    # insert the products in the table
    products = []
    for index, row in products_df.iterrows():
        if not pd.isnull(row["NAME"]):
            product = Product(sku=row["SKU"], store="Richart's")
            product.brand = row["BRAND"]
            product.barcodes = row["BARCODES"]
            product.description = row["DESCRIPTION"]
            product.category = row["CATEGORY"]
            product.image_url = row["IMAGE_URL"]
            product.package = row["BUY_UNIT"]
            product.name = row["NAME"]
            products.append(product)
        else:
            print("PRODUCTS | Product without name! index:", index)
    try:
        session.bulk_save_objects(products)
        session.commit()
    except Exception as e:
        print("PRODUCTS error", e)
        session.rollback()


if __name__ == "__main__":
    process_csv_files()
