import os
import pandas as pd
import great_expectations as gx

def validate_data(input_dir):
    print(f"Starting Data Validation for files in {input_dir}...\n")
    
    file_expectations = {
        'fact_order_items.csv': [
            {'type': 'expect_column_to_exist', 'kwargs': {'column': 'order_item_key'}},
            {'type': 'expect_column_values_to_not_be_null', 'kwargs': {'column': 'order_item_key'}},
            {'type': 'expect_column_values_to_be_unique', 'kwargs': {'column': 'order_item_key'}},
            {'type': 'expect_column_values_to_be_between', 'kwargs': {'column': 'price', 'min_value': 0}}
        ],
        'dim_customers.csv': [
            {'type': 'expect_column_to_exist', 'kwargs': {'column': 'customer_key'}},
            {'type': 'expect_column_values_to_not_be_null', 'kwargs': {'column': 'customer_key'}},
            {'type': 'expect_column_values_to_be_unique', 'kwargs': {'column': 'customer_key'}},
        ],
        'dim_products.csv': [
            {'type': 'expect_column_to_exist', 'kwargs': {'column': 'product_key'}},
            {'type': 'expect_column_values_to_not_be_null', 'kwargs': {'column': 'product_key'}},
            {'type': 'expect_column_values_to_be_unique', 'kwargs': {'column': 'product_key'}},
        ],
        'dim_sellers.csv': [
            {'type': 'expect_column_to_exist', 'kwargs': {'column': 'seller_key'}},
            {'type': 'expect_column_values_to_not_be_null', 'kwargs': {'column': 'seller_key'}},
            {'type': 'expect_column_values_to_be_unique', 'kwargs': {'column': 'seller_key'}},
        ],
        'fact_order_payments.csv': [
            {'type': 'expect_column_to_exist', 'kwargs': {'column': 'payment_key'}},
            {'type': 'expect_column_values_to_not_be_null', 'kwargs': {'column': 'payment_key'}},
            {'type': 'expect_column_values_to_be_unique', 'kwargs': {'column': 'payment_key'}},
            {'type': 'expect_column_values_to_be_between', 'kwargs': {'column': 'payment_value', 'min_value': 0}}
        ]
    }

    all_tests_passed = True

    for file_name, expectations in file_expectations.items():
        file_path = os.path.join(input_dir, file_name)
        if not os.path.exists(file_path):
            print(f"Skipping {file_name}: File not found.")
            continue
            
        print(f"Validating {file_name}...")
        df = pd.read_csv(file_path)
        gx_df = gx.from_pandas(df)
        
        file_passed = True
        
        for exp in expectations:
            exp_type = exp['type']
            kwargs = exp['kwargs']
            
            # Dynamically call the expectation method
            method = getattr(gx_df, exp_type)
            result = method(**kwargs)
            
            success = result.success
            msg = f"  - {exp_type} ({kwargs}): {'PASSED' if success else 'FAILED'}"
            print(msg)
            
            if not success:
                file_passed = False
                all_tests_passed = False
                # Print sample of unexpected values if present
                if 'unexpected_list' in result.result and result.result['unexpected_list']:
                    print(f"    Sample unexpected values: {result.result['unexpected_list'][:5]}")
        
        if file_passed:
            print(f"✅ All checks passed for {file_name}\n")
        else:
            print(f"❌ Some checks FAILED for {file_name}\n")

    if all_tests_passed:
        print("🎉 ALL DATA TESTS PASSED SUCCESSFULLY.")
    else:
        print("⚠️ SOME DATA TESTS FAILED. Please review the output above.")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_directory = os.path.join(base_dir, "processed_data")
    validate_data(input_directory)
