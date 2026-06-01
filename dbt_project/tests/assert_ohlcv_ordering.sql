select *
from {{ ref('fct_ohlcv') }}
where hour_high < hour_low
   or hour_high < hour_open
   or hour_high < hour_close
   or hour_low  > hour_open
   or hour_low  > hour_close
