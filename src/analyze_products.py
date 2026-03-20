import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set_theme(style="whitegrid")

def analyze_products(data_dir, output_dir):
    fact_path = os.path.join(data_dir, 'fact_order_items.csv')
    dim_path = os.path.join(data_dir, 'dim_products.csv')

    print("Loading data...")
    df_items = pd.read_csv(fact_path)
    df_products = pd.read_csv(dim_path)

    print("Merging datasets...")
    # Merge items with products to get category name
    df = pd.merge(df_items, df_products, on='product_key', how='left')

    # Fill missing category names with 'Unknown'
    df['product_category_name_english'] = df['product_category_name_english'].fillna('Unknown')
    
    # We will rank individual products (product_key)
    print("Aggregating metrics by product_key...")
    
    # Group by product_key
    product_metrics = df.groupby(['product_key', 'product_category_name_english']).agg(
        revenue=('price', 'sum'),
        units_sold=('order_item_key', 'count'),
        unique_customers=('customer_key', 'nunique')
    ).reset_index()

    # Create a clean label combining product category and a snippet of the UUID for readability
    # Converting product_key to string just in case
    product_metrics['product_key'] = product_metrics['product_key'].astype(str)
    product_metrics['label'] = product_metrics['product_category_name_english'].str[:20] + " (..." + product_metrics['product_key'].str[-6:] + ")"

    def plot_top10(metric, title, filename, color):
        top10 = product_metrics.sort_values(by=metric, ascending=False).head(10)
        
        plt.figure(figsize=(10, 6))
        ax = sns.barplot(
            data=top10, 
            y='label', 
            x=metric,
            color=color
        )
        plt.title(title, fontsize=16, pad=20)
        plt.xlabel(metric.replace('_', ' ').title(), fontsize=12)
        plt.ylabel('Product (Category + ID)', fontsize=12)
        plt.tight_layout()
        
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved plot: {filepath}")

    print("Generating visualizations...")
    plot_top10('revenue', 'Top 10 Products by Revenue', 'revenue_ranking.png', 'salmon')
    plot_top10('units_sold', 'Top 10 Products by Units Sold', 'units_sold_ranking.png', 'skyblue')
    plot_top10('unique_customers', 'Top 10 Products by Unique Customers', 'unique_customers_ranking.png', 'lightgreen')

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_directory = os.path.join(base_dir, "processed_data")
    output_directory = "/home/hoa_im_eng/.gemini/antigravity/brain/5e8ef3c7-1c81-49c6-96b8-d64f3ce8c0a6"
    
    analyze_products(data_directory, output_directory)
    print("Analysis complete.")
