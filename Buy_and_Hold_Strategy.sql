/* Buy and hold portfolio */

DELIMITER //
DROP PROCEDURE IF EXISTS buy_and_hold //
create procedure buy_and_hold(IN loops int, IN x int)
BEGIN
/* variables for STOCK_TRADE_FEED2 table */
declare this_instrument int(11);
declare this_trade_date date;
declare this_trade_seq_nbr int(11);
declare this_trading_symbol varchar(15);
declare this_trade_time datetime;
declare this_trade_price decimal(18,4);
declare this_trade_size int(11);
/* loop count variables */
declare loopcount int(11);
declare maxloops int(11);
/* EOP variable */
declare db_done int default false;
/* declare cursor */
declare cur1 cursor for select * from F17336Pteam6.STOCK_TRADE_FEED2 order by TRADE_SEQ_NBR,TRADE_TIME;
declare continue handler for not found set db_done=1;
set maxloops=loops*1000;
set loopcount=0;
/* live feed simulation */
open cur1;
   quote_loop: LOOP
        if (db_done OR loopcount=maxloops) then leave quote_loop; end if;
        fetch cur1 into this_instrument, this_trade_date, this_trade_seq_nbr, this_trading_symbol, this_trade_time, this_trade_price, this_trade_size;

        /* buy in */
        /* don't not buy at the first time, because the datas have problems*/
        if (select count(*) from F17336Pteam6.BuyAndHold where TRADING_SYMBOL = this_trading_symbol) = 0 then
      		insert into F17336Pteam6.BuyAndHold values(this_trade_time, this_trading_symbol, '0', this_trade_price);
      		insert into F17336Pteam6.CurrentPrice values(this_trading_symbol, this_trade_time, this_trade_price);

		elseif (select QUANTITY from F17336Pteam6.BuyAndHold where TRADING_SYMBOL = this_trading_symbol) = 0 then
      			update F17336Pteam6.BuyAndHold set TRADE_TIME = this_trade_time where TRADING_SYMBOL = this_trading_symbol;
      			update F17336Pteam6.BuyAndHold set PRICE = this_trade_price where TRADING_SYMBOL = this_trading_symbol;

      				if (x < this_trade_size) then
      					update F17336Pteam6.BuyAndHold set QUANTITY = -x where TRADING_SYMBOL = this_trading_symbol;
      				else
      					update F17336Pteam6.BuyAndHold set QUANTITY = -this_trade_size where TRADING_SYMBOL = this_trading_symbol;
      				end if;

      			update F17336Pteam6.CurrentPrice set CURRENT_PRICE = this_trade_price where SYMBOL = this_trading_symbol;
      			update F17336Pteam6.CurrentPrice set THIS_TIME = this_trade_time where SYMBOL = this_trading_symbol;

      	else
      			update F17336Pteam6.CurrentPrice set CURRENT_PRICE = this_trade_price where SYMBOL = this_trading_symbol;
      			update F17336Pteam6.CurrentPrice set THIS_TIME = this_trade_time where SYMBOL = this_trading_symbol;

      	end if;

      	set loopcount = loopcount + 1;
      	END LOOP;

close cur1;

call sell_all();

END //
DELIMITER ;
