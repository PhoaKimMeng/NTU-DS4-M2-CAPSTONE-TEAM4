import os
import pandas as pd

def transform_data(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    # Define file paths
    customers_path = os.path.join(input_dir, "olist_customers_dataset.csv")
    sellers_path = os.path.join(input_dir, "olist_sellers_dataset.csv")
    products_path = os.path.join(input_dir, "olist_products_dataset.csv")
    category_translation_path = os.path.join(input_dir, "product_category_name_translation.csv")
    orders_path = os.path.join(input_dir, "olist_orders_dataset.csv")
    order_items_path = os.path.join(input_dir, "olist_order_items_dataset.csv")
    order_payments_path = os.path.join(input_dir, "olist_order_payments_dataset.csv")
    order_reviews_path = os.path.join(input_dir, "olist_order_reviews_dataset.csv")

    print("Loading data...")
    df_customers = pd.read_csv(customers_path)
    df_sellers = pd.read_csv(sellers_path)
    df_products = pd.read_csv(products_path)
    df_translations = pd.read_csv(category_translation_path)
    df_orders = pd.read_csv(orders_path)
    df_order_items = pd.read_csv(order_items_path)
    df_order_payments = pd.read_csv(order_payments_path)
    df_order_reviews = pd.read_csv(order_reviews_path)

    print("Processing Dimensions...")
    
    # 1. dim_customers
    dim_customers = df_customers.copy()
    dim_customers.rename(columns={'customer_id': 'customer_key'}, inplace=True)
    dim_customers.to_csv(os.path.join(output_dir, "dim_customers.csv"), index=False)
    
    # 2. dim_sellers
    dim_sellers = df_sellers.copy()
    dim_sellers.rename(columns={'seller_id': 'seller_key'}, inplace=True)
    dim_sellers.to_csv(os.path.join(output_dir, "dim_sellers.csv"), index=False)
    
    # 3. dim_products
    dim_products = pd.merge(df_products, df_translations, on='product_category_name', how='left')
    dim_products.rename(columns={'product_id': 'product_key'}, inplace=True)
    dim_products = dim_products[['product_key', 'product_category_name_english', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']]
    dim_products.to_csv(os.path.join(output_dir, "dim_products.csv"), index=False)
    
    print("Processing dim_time...")
    # 4. dim_time
    date_cols = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']
    all_dates = pd.Series(dtype='datetime64[ns]')
    for col in date_cols:
        all_dates = pd.concat([all_dates, pd.to_datetime(df_orders[col]).dt.normalize()])
    all_dates = pd.concat([all_dates, pd.to_datetime(df_order_reviews['review_creation_date']).dt.normalize(), pd.to_datetime(df_order_reviews['review_answer_timestamp']).dt.normalize()])
    
    all_dates = all_dates.dropna().unique()
    dim_time = pd.DataFrame({'date': all_dates})
    dim_time['time_key'] = dim_time['date'].dt.strftime('%Y%m%d').astype(int)
    dim_time['year'] = dim_time['date'].dt.year
    dim_time['quarter'] = dim_time['date'].dt.quarter
    dim_time['month'] = dim_time['date'].dt.month
    dim_time['day'] = dim_time['date'].dt.day
    dim_time['day_of_week'] = dim_time['date'].dt.dayofweek
    dim_time.to_csv(os.path.join(output_dir, "dim_time.csv"), index=False)

    def safe_time_key(series):
        return pd.to_datetime(series, errors='coerce').dt.normalize().dt.strftime('%Y%m%d').fillna(-1).astype(int)

    print("Processing Facts...")
    
    # 5. fact_order_items
    fact_order_items = df_order_items.copy()
    orders_info = df_orders[['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_delivered_customer_date']]
    fact_order_items = fact_order_items.merge(orders_info, on='order_id', how='left')
    
    fact_order_items['order_item_key'] = fact_order_items['order_id'] + '_' + fact_order_items['order_item_id'].astype(str)
    fact_order_items.rename(columns={
        'customer_id': 'customer_key',
        'seller_id': 'seller_key',
        'product_id': 'product_key',
        'order_item_id': 'order_item_sequence_id'
    }, inplace=True)
    
    fact_order_items['order_purchase_date_key'] = safe_time_key(fact_order_items['order_purchase_timestamp'])
    fact_order_items['order_delivered_customer_date_key'] = safe_time_key(fact_order_items['order_delivered_customer_date'])
    
    fact_order_items = fact_order_items[[
        'order_item_key', 'order_id', 'order_item_sequence_id', 'customer_key', 'seller_key', 'product_key',
        'order_purchase_date_key', 'order_delivered_customer_date_key', 'order_status', 'price', 'freight_value'
    ]]
    fact_order_items.to_csv(os.path.join(output_dir, "fact_order_items.csv"), index=False)

    # 6. fact_order_payments
    fact_order_payments = df_order_payments.copy()
    fact_order_payments['payment_key'] = fact_order_payments['order_id'] + '_' + fact_order_payments['payment_sequential'].astype(str)
    fact_order_payments = fact_order_payments.merge(df_orders[['order_id', 'customer_id', 'order_purchase_timestamp']], on='order_id', how='left')
    fact_order_payments.rename(columns={'customer_id': 'customer_key'}, inplace=True)
    fact_order_payments['order_purchase_date_key'] = safe_time_key(fact_order_payments['order_purchase_timestamp'])
    
    fact_order_payments = fact_order_payments[[
        'payment_key', 'order_id', 'customer_key', 'order_purchase_date_key', 'payment_type', 'payment_sequential', 'payment_installments', 'payment_value'
    ]]
    fact_order_payments.to_csv(os.path.join(output_dir, "fact_order_payments.csv"), index=False)

    # 7. fact_order_reviews
    fact_order_reviews = df_order_reviews.copy()
    fact_order_reviews['review_key'] = fact_order_reviews['review_id'] + '_' + fact_order_reviews['order_id']
    fact_order_reviews = fact_order_reviews.merge(df_orders[['order_id', 'customer_id']], on='order_id', how='left')
    fact_order_reviews.rename(columns={'customer_id': 'customer_key'}, inplace=True)
    
    fact_order_reviews['review_creation_date_key'] = safe_time_key(fact_order_reviews['review_creation_date'])
    fact_order_reviews = fact_order_reviews[[
        'review_key', 'review_id', 'order_id', 'customer_key', 'review_score', 'review_creation_date_key'
    ]]
    fact_order_reviews.to_csv(os.path.join(output_dir, "fact_order_reviews.csv"), index=False)

    print(f"ETL completed successfully. Output saved to: {output_dir}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_directory = os.path.join(base_dir, "downloaded_data(backup)")
    output_directory = os.path.join(base_dir, "processed_data")
    
    transform_data(input_directory, output_directory)
