================================
Welcome to backtest-crypto
================================

Project structure
------------------

* read coin-history overall (eg. SQL DB.)
* B. Yield prediction every x minutes/hours

 - coin history splitter, available and masked
 - Obtain oversold values dict with crypto_oversold.
   Also allow plugging in other models


* 3D surface graph


.. code:: python

    4d_array.init()
    for increase_expected in []:
      for reduction_expected in []:
            for oversold-cutoff-value in []:
                for available, masked in coin_history:
                    4d_array[][][][] = average_profit_normalized_all_coins



optimize the result with different candle-width

Simple buy and sell orders 20 days limit. Check

Set trailing order. And estimate value


Operations
-------------
0. Create 3d surface graph
x-axis increase expected
y-axis reduction expected
z-axis oversold-cutoff value

1. Create 3d surafce graph
x-axis increase expected
y-axis reduction expected
z-axis days for execution

2. For a particular "expected increase" and "expected reduction", see how it fares across time
2d line graph

3. Limit vs trailing order
-set x percentage increase
-trailing variables:
..kick-in-after x percentage
..what should be the lower stop limit
2d line graphs running parallel
