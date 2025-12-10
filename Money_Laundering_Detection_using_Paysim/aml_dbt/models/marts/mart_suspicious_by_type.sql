{{--
  Mart: suspicious transactions aggregated by transaction type.
--}}

with tx as (

    select *
    from {{ ref('stg_transaction_features') }}
    where is_fraud = 1
       or is_high_value = 1
       or is_night_txn = 1

)

select
    transaction_type,
    count(*) as suspicious_txn_count,
    sum(transaction_amount) as suspicious_total_amount
from tx
group by transaction_type
