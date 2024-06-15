from pathlib import Path
from data_processing.data_reader import DataReader
from data_processing.data_cleaner import DataCleaner
from data_processing.data_uploader import DataUploader
from data_processing.utils import load_env_variables, process_brands_data, process_users_data, process_receipts_data, process_items_data
from sqlalchemy import String, Float, DateTime, Boolean

def main():
    base_dir = Path().resolve().parent

    # Load environment variables
    connection_string = load_env_variables()

    # Initialize DataReader and DataCleaner
    data_reader = DataReader(base_dir)
    data_cleaner = DataCleaner()

    # Process each dataset
    brands_df = process_brands_data(data_reader, data_cleaner)
    users_df = process_users_data(data_reader, data_cleaner)
    receipts_df, receipts_data = process_receipts_data(data_reader, data_cleaner)
    items_df = process_items_data(receipts_data, data_reader, data_cleaner)

    # Initialize DataUploader
    data_uploader = DataUploader(connection_string)

    # Upload data to Postgres
    data_uploader.upload_to_db(brands_df, 'dim_brands', dtype={
        'brand_id': String,
        'brand_name': String,
        'brand_code': String,
        'barcode': String,
        'category': String,
        'category_code': String,
        'top_brand': Boolean,
        'cpg_id': String,
        'cpg_ref': String
    })

    data_uploader.upload_to_db(users_df, 'dim_users', dtype={
        'user_id': String,
        'role': String,
        'sign_up_source': String,
        'state': String,
        'created_date': DateTime,
        'active': Boolean,
        'last_login': DateTime
    })

    data_uploader.upload_to_db(receipts_df, 'fact_receipts', dtype={
        'receipt_id': String,
        'user_id': String,
        'bonus_points_earned': Float,
        'bonus_points_earned_reason': String,
        'create_date': DateTime,
        'date_scanned': DateTime,
        'finished_date': DateTime,
        'modify_date': DateTime,
        'points_awarded_date': DateTime,
        'points_earned': Float,
        'purchase_date': DateTime,
        'purchased_item_count': Float,
        'rewards_receipt_status': String,
        'total_spent': Float
    })

    data_uploader.upload_to_db(items_df, 'dim_receipt_items', dtype={
        'receipt_id': String,
        'partner_item_id': String,
        'barcode': String,
        'description': String,
        'item_price': Float,
        'item_number': Float,
        'quantity_purchased': Float,
        'final_price': Float,
        'target_price': Float,
        'discounted_item_price': Float,
        'price_after_coupon': Float,
        'needs_fetch_review': Boolean,
        'needs_fetch_review_reason': String,
        'points_earned': Float,
        'points_not_awarded_reason': String,
        'points_payer_id': String,
        'prevent_target_gap_points': Boolean,
        'deleted': Boolean,
        'rewards_group': String,
        'rewards_product_partner_id': String,
        'user_flagged_barcode': String,
        'user_flagged_new_item': Boolean,
        'user_flagged_price': Float,
        'user_flagged_quantity': Float,
        'user_flagged_description': String,
        'competitive_product': Boolean,
        'competitor_rewards_group': String,
        'metabrite_campaign_id': String
    })

if __name__ == "__main__":
    main()
