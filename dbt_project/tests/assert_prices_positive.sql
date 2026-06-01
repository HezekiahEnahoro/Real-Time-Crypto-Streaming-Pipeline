select *
from {{ ref('fct_ohlcv') }}
where hour_open  <= 0
   or hour_close <= 0
   or hour_high  <= 0
   or hour_low   <= 0
