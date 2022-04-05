event_v2_data = "event_v2_data"
transaction = "transaction"
transaction_request = "transaction_request"
payment_instrument_token_data = "payment_instrument_token_data"

event_v2_data_column_names = ["event_id", "flow_id", "created_at", "transaction_lifecycle_event", "decline_reason",
                              "decline_type", "transaction_id", "flow_id"]

transaction_column_names = ["transaction_id", "transaction_type", "amount", "currency_code",
                            "processor_merchant_account_id"]

transaction_request_column_names = ["payment_method", "token_id", "flow_id"]

payment_instrument_token_data_column_names = ["three_d_secure_authentication", "payment_instrument_type", "customer_id",
                                              "token_id"]

nested_key_mapping = {
    "decline_reason": "error_details",
    "decline_type": "error_details",
    "customer_id": "vault_data",
    "payment_method": "vault_options"
}

db_name = "metrics.db"
new_table = "metrics"

file_name = "wal.json"
