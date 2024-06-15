"""
Main Class for Data Processing
This script orchestrates the reading, cleaning, and uploading of data.
"""

from data_processing.data_reader import DataReader
from data_processing.data_cleaner import DataCleaner
from data_processing.data_uploader import DataUploader
from pathlib import Path
from sqlalchemy import String, Float, DateTime, Boolean

def main():
    base_dir = Path().resolve().parent

    # initialize DataReader
    data_reader = DataReader(base_dir)

    # read JSON data
    brands_data = data_reader.read_json_lines('brands.json')
    users_data = data_reader.read_json_lines('users.json')
    receipts_data = data_reader.read_json_lines('receipts.json')

    # flatten and normalize data
    brands_df = data_reader.flatten_and_normalize(brands_data)
    users_df = data_reader.flatten_and_normalize(users_data)
    receipts_df = data_reader.flatten_and_normalize(receipts_data)

    # adjusting column orders and mappings
    brands_order = ['_id_$oid', 'name', 'brandCode', 'barcode', 'category', 'categoryCode', 'topBrand', 'cpg_$id_$oid', 'cpg_$ref']
    brands_column_mapping = {
        '_id_$oid': 'brand_id',
        'name': 'brand_name',
        'brandCode': 'brand_code',
        'barcode': 'barcode',
        'category': 'category',
        'categoryCode': 'category_code',
        'topBrand': 'top_brand',
        'cpg_$id_$oid': 'cpg_id',
        'cpg_$ref': 'cpg_ref'
    }
    users_order = ['_id_$oid', 'role', 'signUpSource', 'state', 'createdDate_$date', 'active', 'lastLogin_$date']
    users_column_mapping = {
        '_id_$oid': 'user_id',
        'role': 'role',
        'signUpSource': 'sign_up_source',
        'state': 'state',
        'createdDate_$date': 'created_date',
        'active': 'active',
        'lastLogin_$date': 'last_login'
    }

    receipts_order = ['_id_$oid', 'userId', 'bonusPointsEarned', 'bonusPointsEarnedReason', 'createDate_$date', 'dateScanned_$date', 'finishedDate_$date', 'modifyDate_$date', 'pointsAwardedDate_$date', 'pointsEarned', 'purchaseDate_$date', 'purchasedItemCount', 'rewardsReceiptStatus', 'totalSpent']
    receipts_column_mapping = {
        '_id_$oid': 'receipt_id',
        'userId': 'user_id',
        'bonusPointsEarned': 'bonus_points_earned',
        'bonusPointsEarnedReason': 'bonus_points_earned_reason',
        'createDate_$date': 'create_date',
        'dateScanned_$date': 'date_scanned',
        'finishedDate_$date': 'finished_date',
        'modifyDate_$date': 'modify_date',
        'pointsAwardedDate_$date': 'points_awarded_date',
        'pointsEarned': 'points_earned',
        'purchaseDate_$date': 'purchase_date',
        'purchasedItemCount': 'purchased_item_count',
        'rewardsReceiptStatus': 'rewards_receipt_status',
        'totalSpent': 'total_spent'
    }

    items_order = ['receipt_id', 'partnerItemId', 'barcode', 'description', 'itemPrice', 'itemNumber', 'quantityPurchased', 'finalPrice', 'targetPrice', 'discountedItemPrice', 'priceAfterCoupon', 'needsFetchReview', 'needsFetchReviewReason', 'pointsEarned', 'pointsNotAwardedReason', 'pointsPayerId', 'preventTargetGapPoints', 'deleted', 'rewardsGroup', 'rewardsProductPartnerId', 'userFlaggedBarcode', 'userFlaggedNewItem', 'userFlaggedPrice', 'userFlaggedQuantity', 'userFlaggedDescription', 'competitiveProduct', 'competitorRewardsGroup', 'metabriteCampaignId']
    items_column_mapping = {
        'partnerItemId': 'partner_item_id',
        'itemPrice': 'item_price',
        'itemNumber': 'item_number',
        'quantityPurchased': 'quantity_purchased',
        'finalPrice': 'final_price',
        'targetPrice': 'target_price',
        'discountedItemPrice': 'discounted_item_price',
        'priceAfterCoupon': 'price_after_coupon',
        'needsFetchReview': 'needs_fetch_review',
        'needsFetchReviewReason': 'needs_fetch_review_reason',
        'pointsEarned': 'points_earned',
        'pointsNotAwardedReason': 'points_not_awarded_reason',
        'pointsPayerId': 'points_payer_id',
        'preventTargetGapPoints': 'prevent_target_gap_points',
        'rewardsGroup': 'rewards_group',
        'rewardsProductPartnerId': 'rewards_product_partner_id',
        'userFlaggedBarcode': 'user_flagged_barcode',
        'userFlaggedNewItem': 'user_flagged_new_item',
        'userFlaggedPrice': 'user_flagged_price',
        'userFlaggedQuantity': 'user_flagged_quantity',
        'userFlaggedDescription': 'user_flagged_description',
        'competitiveProduct': 'competitive_product',
        'competitorRewardsGroup': 'competitor_rewards_group',
        'metabriteCampaignId': 'metabrite_campaign_id',
        'barcode': 'barcode',
        'description': 'description',
        'deleted': 'deleted',
        'receipt_id': 'receipt_id'
    }

    # initialize DataCleaner
    data_cleaner = DataCleaner()

    # clean and process data
    brands_df = data_cleaner.reorder_and_rename_columns(brands_df, brands_order, brands_column_mapping)
    users_df = data_cleaner.reorder_and_rename_columns(users_df, users_order, users_column_mapping)
    users_df = data_cleaner.convert_to_datetime(users_df, ['created_date', 'last_login'])

    receipts_df = data_cleaner.reorder_and_rename_columns(receipts_df, receipts_order, receipts_column_mapping)
    receipts_df = data_cleaner.convert_to_datetime(receipts_df, ['create_date', 'date_scanned', 'finished_date', 'modify_date', 'points_awarded_date', 'purchase_date'])

    # normalize and clean item data from receipts
    for receipt in receipts_data:
        receipt['id'] = receipt['_id']['$oid']
        if 'rewardsReceiptItemList' not in receipt:
            receipt['rewardsReceiptItemList'] = []
        for item in receipt['rewardsReceiptItemList']:
            item['receipt_id'] = receipt['id']

    items_df = data_reader.flatten_and_normalize(receipts_data, record_path=['rewardsReceiptItemList'], meta=['id'])
    items_df.drop('id', inplace=True, axis=1)
    # print(items_df.columns)

    items_df = data_cleaner.reorder_and_rename_columns(items_df, items_order, items_column_mapping)
    items_df = data_cleaner.convert_columns_to_numeric(items_df, ['final_price', 'item_price', 'user_flagged_quantity', 'points_earned', 'target_price', 'price_after_coupon'])

    # upload data to postgres
    connection_string = 'postgresql://username:password@localhost:5432/fetch_rewards_db'
    data_uploader = DataUploader(connection_string)

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
        'brand_code': String,
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
        'competitive_rewards_group': String,
        'metabrite_campaign_id': String
    })

if __name__ == "__main__":
    main()
