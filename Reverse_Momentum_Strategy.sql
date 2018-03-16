DELIMITER //
DROP PROCEDURE IF EXISTS ReverseMomentum //
CREATE PROCEDURE ReverseMomentum(IN loops int)
BEGIN

-- variables for DailyStockFeed table
DECLARE this_trading_symbol varchar(15);
DECLARE this_trade_time datetime;
DECLARE this_trade_price decimal(18,4);
DECLARE this_trade_size int(11);

DECLARE sum_stocks int(11);

-- loop count variables
DECLARE loopcount int(11);
DECLARE maxloops int(11);

-- EOP variable
DECLARE db_done int default false;

-- declare cursor
DECLARE cur1 CURSOR FOR SELECT TRADING_SYMBOL, TRADE_TIME, TRADE_PRICE, TRADE_SIZE FROM F17336Pteam6.DailyStockFeed ORDER BY TRADE_TIME;

DECLARE continue handler for not found SET db_done=1;
SET maxloops = loops * 1000;
SET loopcount = 0;

-- initialize the streak table
TRUNCATE TABLE TempStockStreak2;

INSERT INTO F17336Pteam6.TempStockStreak2 (SYMBOL, THIS_TIME, CURRENT_PRICE, STREAK)
SELECT TRADE_SYMBOL, TRADE_TIME, TRADE_PRICE, 0
FROM
    (SELECT *
        FROM F17336Pteam6.STOCK_TRADE_FEED
        GROUP BY TRADE_SYMBOL) as FEED;

/* live feed simulation */
open cur1;
   quote_loop: LOOP
        if (db_done OR loopcount >= maxloops) then leave quote_loop;
        end if;

        fetch cur1 into this_trading_symbol, this_trade_time, this_trade_price, this_trade_size;

        -- if the price increased since the last check
        if (SELECT CURRENT_PRICE
            FROM F17336Pteam6.TempStockStreak2
            WHERE SYMBOL = this_trading_symbol) <= this_trade_price then
            -- if the stock is on a positive streak, increment its streak by 1
            if (SELECT STREAK
                FROM F17336Pteam6.TempStockStreak2
                WHERE SYMBOL = this_trading_symbol) >= 0 then
                UPDATE F17336Pteam6.TempStockStreak2 SET STREAK = STREAK + 1
                WHERE SYMBOL = this_trading_symbol;

            -- if the stock is on a negative streak, change it so it's on a positive streak
            else
                UPDATE F17336Pteam6.TempStockStreak2 SET STREAK = 1
                WHERE SYMBOL = this_trading_symbol;
            end if;

        -- if the price decreased since the last check
        else
            -- if the stock is on a positive streak, change it so it's on a negative streak
            if (SELECT STREAK
                FROM F17336Pteam6.TempStockStreak2
                WHERE SYMBOL = this_trading_symbol) >= 0
            then
                UPDATE F17336Pteam6.TempStockStreak2 SET STREAK = -1
                WHERE SYMBOL = this_trading_symbol;

            -- if the stock is on a negative streak, decrement it by 1
            else
                UPDATE F17336Pteam6.TempStockStreak2 SET STREAK = STREAK - 1
                WHERE SYMBOL = this_trading_symbol;
            end if;
        end if;

        -- always update the current price and current time
        UPDATE F17336Pteam6.TempStockStreak2 SET CURRENT_PRICE = this_trade_price,
        THIS_TIME = this_trade_time
        WHERE SYMBOL = this_trading_symbol;

 	if (SELECT STREAK
            FROM F17336Pteam6.TempStockStreak2
                WHERE SYMBOL = this_trading_symbol) >= 3
        then
            SET sum_stocks = (SELECT SUM(QUANTITY) FROM F17336Pteam6.ReverseMomentum WHERE TRADE_SYMBOL = this_trading_symbol) * -1;

                -- sell stocks by adding it to the ReverseMomentum table
            INSERT INTO F17336Pteam6.ReverseMomentum (TRADE_TIME, TRADE_SYMBOL, QUANTITY, PRICE)
            VALUES (this_trade_time, this_trading_symbol, LEAST(sum_stocks,  6000)
, this_trade_price);

        -- after updating the stocks, check the streak
        -- if streak <= -3, we should buy 6k shares of stocks and reset the streak to -1
        -- so we don't continuously buy stocks every single time
        elseif (SELECT STREAK
            FROM F17336Pteam6.TempStockStreak2
                WHERE SYMBOL = this_trading_symbol) <= -3
        then
            -- buy stocks by adding it to ReverseMomentum table
            INSERT INTO F17336Pteam6.ReverseMomentum (TRADE_TIME, TRADE_SYMBOL, QUANTITY, PRICE)
            VALUES (this_trade_time, this_trading_symbol, -6000, this_trade_price);

            -- reset the streak
            UPDATE F17336Pteam6.TempStockStreak2 SET STREAK = -1
            WHERE SYMBOL = this_trading_symbol;


end if;

        set loopcount = loopcount + 1;
        END LOOP;

close cur1;

END //
DELIMITER ;
