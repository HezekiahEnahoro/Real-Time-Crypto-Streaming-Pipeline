with pivoted as (
    select
        hour,
        max(case when symbol = 'BTC'   then hour_close end) as btc,
        max(case when symbol = 'ETH'   then hour_close end) as eth,
        max(case when symbol = 'BNB'   then hour_close end) as bnb,
        max(case when symbol = 'SOL'   then hour_close end) as sol,
        max(case when symbol = 'ADA'   then hour_close end) as ada,
        max(case when symbol = 'MATIC' then hour_close end) as matic,
        max(case when symbol = 'AVAX'  then hour_close end) as avax,
        max(case when symbol = 'DOT'   then hour_close end) as dot
    from {{ ref('fct_ohlcv') }}
    group by hour
)

select
    hour,
    round(cast(corr(btc, eth)   over w as numeric), 4) as btc_eth,
    round(cast(corr(btc, bnb)   over w as numeric), 4) as btc_bnb,
    round(cast(corr(btc, sol)   over w as numeric), 4) as btc_sol,
    round(cast(corr(btc, ada)   over w as numeric), 4) as btc_ada,
    round(cast(corr(btc, matic) over w as numeric), 4) as btc_matic,
    round(cast(corr(btc, avax)  over w as numeric), 4) as btc_avax,
    round(cast(corr(btc, dot)   over w as numeric), 4) as btc_dot,
    round(cast(corr(eth, sol)   over w as numeric), 4) as eth_sol
from pivoted
window w as (order by hour rows between 23 preceding and current row)
order by hour desc
