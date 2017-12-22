DELIMITER //
DROP PROCEDURE IF EXISTS Moving_Averages_SP //
create procedure Moving_Averages_SP(IN loops int, IN x int)
BEGIN
/* variables for STOCK_TRADE_FEED2 table */
declare this_instrument int(11);
declare this_trade_date date;
declare this_trade_seq_nbr int(11);
declare this_trading_symbol varchar(15);
declare this_trade_time datetime;
declare this_trade_price decimal(18,4);
declare this_trade_size int(11);
declare this_current_price decimal(18,4);
/* loop count variables */
declare loopcount int(11);
declare maxloops int(11);
/* EOP variable */
declare db_done int default false;
/* Moving Averages Variables */
declare SMA decimal(18,4);
declare start_date date;
/* declare cursor */
declare cur1 cursor for select * from F17336Pteam6.STOCK_TRADE_FEED2 order by TRADE_SEQ_NBR,TRADE_TIME;
declare continue handler for not found set db_done=1;
set loopcount=1;
set maxloops=loops*1000;
set start_date = '2017-02-08';
/* live feed simulation */
open cur1;
  quote_loop: LOOP
       if (db_done OR loopcount=maxloops) then leave quote_loop; end if;
       fetch cur1 into this_instrument, this_trade_date, this_trade_seq_nbr, this_trading_symbol, this_trade_time, this_trade_price, this_trade_size;

       SELECT AVG(PRICE) INTO SMA FROM (SELECT DATE(TRADE_TIME) as DATE, SUM(TRADE_PRICE) as PRICE
       FROM STOCK_TRADE_FEED2
       WHERE DATE(TRADE_TIME) >= start_date + INTERVAL 0 DAY AND DATE(TRADE_TIME) < start_date + INTERVAL 29 DAY
       AND TRADING_SYMBOL = this_trading_symbol GROUP BY DATE(TRADE_TIME)) AS T;

       if ((select count(*) from F17336Pteam6.Moving_Average where TRADING_SYMBOL = this_trading_symbol) = 0) then
           insert into F17336Pteam6.Moving_Averages values(this_trade_time, this_trading_symbol, '0', this_trade_price);

       elseif ((select QUANTITY from F17336Pteam6.Moving_Averages where TRADING_SYMBOL = this_trading_symbol) = 0)then
           update F17336Pteam6.Moving_Averages set TRADE_TIME = this_trade_time where TRADING_SYMBOL = this_trading_symbol; -- updating the trading time
                   update F17336Pteam6.Moving_Averages set PRICE = this_trade_price where TRADING_SYMBOL = this_trading_symbol;     -- updating the price
           if (((((SMA-this_current_price)/SMA)*100) > 5/100)) then /*buy*/
               update F17336Pteam6.Moving_Averages set QUANTITY = -x where TRADING_SYMBOL = this_trading_symbol;

           elseif (((((SMA-this_current_price)/SMA)*100) < -5/100)) then /*sell*/
               update F17336Pteam6.Moving_Averages set QUANTITY = -this_trade_size where TRADING_SYMBOL = this_trading_symbol;

           end if;
       end if;
       set loopcount = loopcount + 1;
       END LOOP;
close cur1;
call sell_all();
END //
