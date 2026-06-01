with prices as (
    select
        symbol,
        hour,
        hour_close,
        lag(hour_close) over (partition by symbol order by hour) as prev_close
    from {{ ref('fct_ohlcv') }}
),

changes as (
    select
        symbol,
        hour,
        hour_close,
        greatest(hour_close - prev_close, 0)  as gain,
        greatest(prev_close - hour_close, 0)  as loss
    from prices
    where prev_close is not null
),

avgs as (
    select
        symbol,
        hour,
        hour_close,
        avg(gain) over (
            partition by symbol order by hour
            rows between 13 preceding and current row
        ) as avg_gain,
        avg(loss) over (
            partition by symbol order by hour
            rows between 13 preceding and current row
        ) as avg_loss
    from changes
)

select
    symbol,
    hour,
    round(hour_close, 4)                                        as hour_close,
    round(
        case
            when avg_loss = 0 then 100
            else 100 - 100 / (1 + avg_gain / nullif(avg_loss, 0))
        end,
        2
    )                                                           as rsi_14,
    avg_gain > avg_loss                                         as bullish_momentum,
    case
        when avg_loss = 0                                         then 'overbought'
        when 100 - 100 / (1 + avg_gain / nullif(avg_loss, 0)) >= 70 then 'overbought'
        when 100 - 100 / (1 + avg_gain / nullif(avg_loss, 0)) <= 30 then 'oversold'
        else 'neutral'
    end                                                         as rsi_signal
from avgs
order by symbol, hour desc
