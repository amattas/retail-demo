# Fabric Notebook (PySpark) — Load master dimensions from CSV into Lakehouse Delta
# Configure master_root to the folder where datagen outputs master CSVs.

from pyspark.sql import functions as F

master_root = "/lakehouse/external/master"  # TODO: replace with actual path or parameterize

def load_csv(name: str, schema=None):
    path = f"{master_root}/{name}.csv"
    df = spark.read.option("header", True).csv(path)
    return df

def write_delta(df, path):
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)

# Geographies
df_geo = load_csv("geographies_master")
write_delta(df_geo, "/Tables/dim/geographies_master")

# Stores
df_stores = load_csv("stores")
write_delta(df_stores, "/Tables/dim/stores")

# Distribution Centers
df_dcs = load_csv("distribution_centers")
write_delta(df_dcs, "/Tables/dim/distribution_centers")

# Trucks
df_trucks = load_csv("trucks")
write_delta(df_trucks, "/Tables/dim/trucks")

# Customers
df_customers = load_csv("customers")
write_delta(df_customers, "/Tables/dim/customers")

# Products
df_products = load_csv("products_master")
write_delta(df_products, "/Tables/dim/products_master")

print("Dimensions loaded to /Tables/dim/*")

