with bands as (
    select
        symbol,
        hour,
        hour_close,
        avg(hour_close) over (
            partition by symbol order by hour
            rows between 19 preceding and current row
        )                                                       as sma_20,
        stddev_pop(hour_close) over (
            partition by symbol order by hour
            rows between 19 preceding and current row
        )                                                       as stddev_20,
        count(*) over (
            partition by symbol order by hour
            rows between 19 preceding and current row
        )                                                       as window_rows
    from {{ ref('fct_ohlcv') }}
)

select
    symbol,
    hour,
    round(hour_close, 4)                                        as hour_close,
    round(sma_20, 4)                                            as sma_20,
    round(sma_20 + 2 * stddev_20, 4)                           as upper_band,
    round(sma_20 - 2 * stddev_20, 4)                           as lower_band,
    round(
        (hour_close - (sma_20 - 2 * stddev_20))
            / nullif(4 * stddev_20, 0),
        4
    )                                                           as pct_b,
    round(
        (4 * stddev_20) / nullif(sma_20, 0),
        4
    )                                                           as bandwidth,
    case
        when hour_close > sma_20 + 2 * stddev_20 then 'above_upper'
        when hour_close < sma_20 - 2 * stddev_20 then 'below_lower'
        when hour_close > sma_20                  then 'above_mid'
        else                                           'below_mid'
    end                                                         as band_position
from bands
where window_rows = 20
order by symbol, hour desc
