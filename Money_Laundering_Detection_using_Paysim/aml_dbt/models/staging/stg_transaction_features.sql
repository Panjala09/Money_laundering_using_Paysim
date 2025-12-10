{{--
  Staging model for AML transaction features.
  In a real dbt run, this would select from the raw feature table in the warehouse.
--}}

select
    transaction_id,
    event_time,
    step,
    hour_of_day,
    day_of_week,
    transaction_type,
    transaction_amount,
    src_account_id,
    old_balance_orig,
    new_balance_orig,
    src_balance_change,
    dst_account_id,
    old_balance_dest,
    new_balance_dest,
    dst_balance_change,
    is_high_value,
    is_night_txn,
    is_fraud,
    is_flagged_fraud
from {{ source('aml', 'transaction_features') }}
