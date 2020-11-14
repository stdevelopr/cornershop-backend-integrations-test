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


    # create a map between sku and id for each product
    products = session.query(Product).with_entities(Product.id, Product.sku)
    sku_id_map = {product[0]:product[1] for product in products}

    # clean null values
    if prices_stock_df.isnull().values.sum() > 0:
        print("Number of rows with null values (Not inserted): ", prices_stock_df.isnull().values.sum())
        prices_stock_df = prices_stock_df.dropna(how='any',axis=0)

    prices_stock = []
    for index, row in prices_stock_df.iterrows():
        try:
            branch_product = BranchProduct(product_id= sku_id_map[row["SKU"]])
            branch_product.branch = row["BRANCH"]
            branch_product.stock = row["STOCK"]
            branch_product.price = row["PRICE"]
            prices_stock.append(branch_product)
        except Exception as e:
            print("PRICE-STOCK error", e)

    try:
        session.bulk_save_objects(prices_stock)
        session.commit()
    except Exception as e:
        print("error", e)
        session.rollback()


if __name__ == "__main__":
    process_csv_files()
