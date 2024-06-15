import os

from dotenv import load_dotenv


def load_env_variables():
    load_dotenv()
    db_username = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    return f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

def process_brands_data(data_reader, data_cleaner):
    brands_data = data_reader.read_json_lines('brands.json')
    brands_df = data_reader.flatten_and_normalize(brands_data)

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

    brands_df = data_cleaner.reorder_and_rename_columns(brands_df, brands_order, brands_column_mapping)
    return brands_df

def process_users_data(data_reader, data_cleaner):
    users_data = data_reader.read_json_lines('users.json')
    users_df = data_reader.flatten_and_normalize(users_data)

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

    users_df = data_cleaner.reorder_and_rename_columns(users_df, users_order, users_column_mapping)
    users_df = data_cleaner.convert_to_datetime(users_df, ['created_date', 'last_login'])
    return users_df

def process_receipts_data(data_reader, data_cleaner):
    receipts_data = data_reader.read_json_lines('receipts.json')
    receipts_df = data_reader.flatten_and_normalize(receipts_data)

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

    receipts_df = data_cleaner.reorder_and_rename_columns(receipts_df, receipts_order, receipts_column_mapping)
    receipts_df = data_cleaner.convert_to_datetime(receipts_df, ['create_date', 'date_scanned', 'finished_date', 'modify_date', 'points_awarded_date', 'purchase_date'])
    return receipts_df, receipts_data

def process_items_data(receipts_data, data_reader, data_cleaner):
    for receipt in receipts_data:
        receipt['id'] = receipt['_id']['$oid']
        if 'rewardsReceiptItemList' not in receipt:
            receipt['rewardsReceiptItemList'] = []
        for item in receipt['rewardsReceiptItemList']:
            item['receipt_id'] = receipt['id']

    items_df = data_reader.flatten_and_normalize(receipts_data, record_path=['rewardsReceiptItemList'], meta=['id'])
    items_df.drop('id', inplace=True, axis=1)

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
        'competitive_product': 'competitive_product',
        'competitorRewardsGroup': 'competitor_rewards_group',
        'metabriteCampaignId': 'metabrite_campaign_id',
        'barcode': 'barcode',
        'description': 'description',
        'deleted': 'deleted',
        'receipt_id': 'receipt_id'
    }

    items_df = data_cleaner.reorder_and_rename_columns(items_df, items_order, items_column_mapping)
    items_df = data_cleaner.convert_columns_to_numeric(items_df, ['final_price', 'item_price', 'user_flagged_quantity', 'points_earned', 'target_price', 'price_after_coupon'])
    return items_df
