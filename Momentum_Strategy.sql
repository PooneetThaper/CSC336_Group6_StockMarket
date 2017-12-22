CREATE TABLE DailyStockFeed (
    TRADE_ID int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
    TRADING_SYMBOL varchar(15) NOT NULL,
    TRADE_TIME datetime NOT NULL,
    TRADE_PRICE decimal(18,4) NOT NULL,
    TRADE_SIZE int(11) NOT NULL,
    UNIQUE (TRADE_TIME)
);

TRUNCATE TABLE F17336Pteam6.DailyStockFeed;
INSERT INTO F17336Pteam6.DailyStockFeed (TRADING_SYMBOL, TRADE_TIME, TRADE_PRICE, TRADE_SIZE)
SELECT * FROM
    -- gets a row for each stock once a day (the first row for each day of each stock)
    (SELECT TRADING_SYMBOL, TRADE_TIME, TRADE_PRICE, TRADE_SIZE
        FROM
            (SELECT DISTINCT *,
                @stock_rank := IF(@stock = TRADING_SYMBOL, @stock_rank + 1, 1) AS stock_rank,
                @stock := TRADING_SYMBOL
            FROM F17336Pteam6.STOCK_TRADE_FEED2
            WHERE DATE(TRADE_DATE) BETWEEN '2017-02-08' AND '2017-08-18'
            GROUP BY TRADE_DATE, TRADING_SYMBOL
            ORDER BY TRADE_TIME ASC
            ) ranked
        WHERE stock_rank = 1) as FEED;

CREATE TABLE TempStockStreak (
    TRADE_ID int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
    SYMBOL varchar(255),
    THIS_TIME datetime,
    CURRENT_PRICE decimal(18,4),
    STREAK int(11)
);

CREATE TABLE Momentum (
    TRADE_ID int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
    TRADE_TIME datetime,
    TRADE_SYMBOL varchar(15),
    QUANTITY int(11),
    PRICE decimal(18,4)
);

-- empty the momentum and streak tables in between running the stored procedures
TRUNCATE TABLE Momentum;
TRUNCATE TABLE TempStockStreak;

-- initialize the streak table so that the first day's prices are stored
INSERT INTO F17336Pteam6.TempStockStreak (SYMBOL, THIS_TIME, CURRENT_PRICE, STREAK)
SELECT TRADING_SYMBOL, TRADE_TIME, TRADE_PRICE, 0
FROM
    (SELECT *
        FROM F17336Pteam6.STOCK_TRADE_FEED2
        GROUP BY TRADING_SYMBOL) as FEED;

-- momentum strategy
DELIMITER //
DROP PROCEDURE IF EXISTS momentum //
CREATE PROCEDURE momentum(IN loops int)
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

/* live feed simulation */
open cur1;
   quote_loop: LOOP
        if (db_done OR loopcount >= maxloops) then leave quote_loop;
        end if;

        fetch cur1 into this_trading_symbol, this_trade_time, this_trade_price, this_trade_size;

        -- if the price increased since the last check
        if (SELECT CURRENT_PRICE
            FROM F17336Pteam6.TempStockStreak
            WHERE SYMBOL = this_trading_symbol) >= this_trade_price then
            -- if the stock is on a positive streak, increment its streak by 1
            if (SELECT STREAK
                FROM F17336Pteam6.TempStockStreak
                WHERE SYMBOL = this_trading_symbol) >= 0 then
                UPDATE F17336Pteam6.TempStockStreak SET STREAK = STREAK + 1
                WHERE SYMBOL = this_trading_symbol;

            -- if the stock is on a negative streak, change it so it's on a positive streak
            else
                UPDATE F17336Pteam6.TempStockStreak SET STREAK = 1
                WHERE SYMBOL = this_trading_symbol;
            end if;

        -- if the price decreased since the last check
        else
            -- if the stock is on a positive streak, change it so it's on a negative streak
            if (SELECT STREAK
                FROM F17336Pteam6.TempStockStreak
                WHERE SYMBOL = this_trading_symbol) >= 0
            then
                UPDATE F17336Pteam6.TempStockStreak SET STREAK = -1
                WHERE SYMBOL = this_trading_symbol;

            -- if the stock is on a negative streak, decrement it by 1
            else
                UPDATE F17336Pteam6.TempStockStreak SET STREAK = STREAK - 1
                WHERE SYMBOL = this_trading_symbol;
            end if;
        end if;

        -- always update the current price and current time
        UPDATE F17336Pteam6.TempStockStreak SET CURRENT_PRICE = this_trade_price,
        THIS_TIME = this_trade_time
        WHERE SYMBOL = this_trading_symbol;

        -- after updating the stocks, check the streak
        -- if steak >= 4, we should buy 6k shares of stocks and reset the streak to 1 so we don't continuously buy stocks every single time
        if (SELECT STREAK
            FROM F17336Pteam6.TempStockStreak
                WHERE SYMBOL = this_trading_symbol) >= 4
        then
            -- buy stocks by adding it to momentum table
            INSERT INTO F17336Pteam6.Momentum (TRADE_TIME, TRADE_SYMBOL, QUANTITY, PRICE)
            VALUES (this_trade_time, this_trading_symbol, -6000, this_trade_price);

            -- reset the streak
            UPDATE F17336Pteam6.TempStockStreak SET STREAK = 1
            WHERE SYMBOL = this_trading_symbol;

        -- if streak <= -2, we should sell shares of this stock if we own any of it
        elseif (SELECT STREAK
            FROM F17336Pteam6.TempStockStreak
                WHERE SYMBOL = this_trading_symbol) <= -2
        then
            SET sum_stocks = (SELECT SUM(QUANTITY) FROM F17336Pteam6.Momentum
                    WHERE TRADE_SYMBOL = this_trading_symbol) * -1;

            if sum_stocks > 0
            then
                -- sell stocks by adding it to the momentum table
                INSERT INTO F17336Pteam6.Momentum (TRADE_TIME, TRADE_SYMBOL, QUANTITY, PRICE)
                VALUES (this_trade_time, this_trading_symbol, sum_stocks,
                    this_trade_price);
            end if;
        end if;

        set loopcount = loopcount + 1;
        END LOOP;

close cur1;

END //
DELIMITER ;
