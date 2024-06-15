SELECT
    CASE
        WHEN rewards_receipt_status = 'FINISHED' THEN 'ACCEPTED'
        ELSE rewards_receipt_status
        END AS rewards_receipt_status,
    SUM(purchased_item_count) AS total_items_purchased
FROM Fact_Receipts
WHERE rewards_receipt_status IN ('FINISHED', 'REJECTED')
GROUP BY rewards_receipt_status;
