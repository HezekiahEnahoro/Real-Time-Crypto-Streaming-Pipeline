-- models/marts/fct_ohlcv.sql
-- Hourly OHLCV candlesticks built from Spark's 5-minute windows.

with windows as (
    select
        symbol,
        window_start,
        price_open,
        price_high,
        price_low,
        price_close,
        volume_usd,
        vwap,
        tick_count,
        flag_large_move,
        flag_high_volatility,
        date_trunc('hour', window_start) as hour
    from {{ source('raw', 'price_windows') }}
    where price_close is not null
),

-- Rank windows within each hour to find the first (open) and last (close).
ranked as (
    select *,
        row_number() over (partition by symbol, hour order by window_start asc)  as rn_first,
        row_number() over (partition by symbol, hour order by window_start desc) as rn_last
    from windows
),

-- One row per (symbol, hour): OHLCV with correct open from earliest window
-- and close from latest window.
hourly as (
    select
        w.symbol,
        w.hour,
        max(case when r.rn_first = 1 then r.price_open  end) as hour_open,
        max(w.price_high)                                     as hour_high,
        min(w.price_low)                                      as hour_low,
        max(case when r.rn_last  = 1 then r.price_close end) as hour_close,
        sum(w.volume_usd)                                     as hour_volume_usd,
        avg(w.vwap)                                           as hour_vwap,
        sum(w.tick_count)                                     as hour_tick_count,
        bool_or(w.flag_large_move)                            as had_large_move,
        bool_or(w.flag_high_volatility)                       as had_high_volatility
    from windows w
    join ranked r using (symbol, hour, window_start)
    group by w.symbol, w.hour
),

-- Moving averages and returns computed on clean one-row-per-hour data.
final as (
    select
        symbol,
        hour,
        hour_open,
        hour_high,
        hour_low,
        hour_close,
        hour_volume_usd,
        hour_vwap,
        hour_tick_count,
        had_large_move,
        had_high_volatility,

        avg(hour_close) over (
            partition by symbol order by hour
            rows between 23 preceding and current row
        )                                                    as ma_24h,

        avg(hour_close) over (
            partition by symbol order by hour
            rows between 167 preceding and current row
        )                                                    as ma_7d,

        round(
            (hour_close - lag(hour_close) over (partition by symbol order by hour))
            / nullif(lag(hour_close) over (partition by symbol order by hour), 0) * 100
        , 4)                                                 as hourly_return_pct,

        round(
            hour_volume_usd / nullif(
                avg(hour_volume_usd) over (
                    partition by symbol order by hour
                    rows between 23 preceding and 1 preceding
                ), 0)
        , 2)                                                 as volume_vs_24h_avg

    from hourly
)

select * from final
order by symbol, hour desc
