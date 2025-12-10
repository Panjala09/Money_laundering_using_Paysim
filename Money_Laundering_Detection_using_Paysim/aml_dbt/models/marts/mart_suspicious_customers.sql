{{--
  Mart: suspicious customers aggregated at src_account_id level.
--}}

with tx as (

    select *
    from {{ ref('stg_transaction_features') }}
    where is_fraud = 1
       or is_high_value = 1
       or is_night_txn = 1

)

select
    src_account_id,
    count(*) as suspicious_txn_count,
    sum(transaction_amount) as suspicious_total_amount,
    max(fraud_score) as max_fraud_score,      -- if you later include fraud_score
    max(event_time) as last_suspicious_time
from tx
group by src_account_id
