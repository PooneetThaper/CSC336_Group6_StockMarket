DELIMITER //
DROP PROCEDURE IF EXISTS momentum_get_net_earnings //
CREATE PROCEDURE momentum_get_net_earnings(IN loops int)
BEGIN

-- variables for Momentum table
DECLARE this_trade_id int(11);
DECLARE this_trade_time datetime;
DECLARE this_trade_symbol varchar(15);
DECLARE this_quantity int(11);
DECLARE this_price decimal(18,4);

DECLARE sum_stocks int(11);

-- loop count variables
DECLARE loopcount int(11);
DECLARE maxloops int(11);

-- EOP variable
DECLARE db_done int default false;

-- declare cursor
DECLARE cur1 CURSOR FOR
    SELECT TRADE_ID, TRADE_TIME, TRADE_SYMBOL, SUM(QUANTITY), PRICE
    FROM F17336Pteam6.Momentum
    GROUP BY TRADE_SYMBOL;

DECLARE continue handler for not found SET db_done=1;
SET maxloops = loops * 1000;
SET loopcount = 0;

-- open loop and go through Momentum, sell any existing holdings
open cur1;
   quote_loop: LOOP
        if (db_done OR loopcount >= maxloops) then leave quote_loop;
        end if;

        fetch cur1 into this_trade_id, this_trade_time, this_trade_symbol, this_quantity, this_price;

        -- if we have some holdings for this stock, sell it
        if (this_quantity < 0) then
            INSERT INTO F17336Pteam6.Momentum(TRADE_TIME, TRADE_SYMBOL, QUANTITY, PRICE)
            VALUES('2017-08-18 12:00:00', this_trade_symbol, this_quantity * -1,
                (SELECT TRADE_PRICE
                    FROM F17336Pteam6.DailyStockFeed
                    WHERE TRADING_SYMBOL = this_trade_symbol AND TRADE_TIME LIKE '2017-08-18%'));
        end if;

        set loopcount = loopcount + 1;
        END LOOP;

close cur1;

-- check the net loss/gain
SELECT SUM(PRICE * QUANTITY)
FROM Momentum;

END //
DELIMITER ;
