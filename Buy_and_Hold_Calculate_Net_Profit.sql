/* Buy and hold portfolio */

DELIMITER //
DROP PROCEDURE IF EXISTS sell_all //
create procedure sell_all()
BEGIN
/* variable for BuyAndHold table */
declare trade_time datetime;
declare trading_symbol varchar(15);
declare trade_quantity int(11);
declare trade_price decimal(18,4);
/* loop count variables */
declare loopcount int(11);
declare maxloops int(11);
/* EOP variable */
declare db_done int default false;
/* declare cursor */
declare cur2 cursor for select * from F17336Pteam6.BuyAndHold order by TRADING_SYMBOL;
declare continue handler for not found set db_done=1;
set maxloops = (select count(*) from F17336Pteam6.BuyAndHold);
set loopcount = 0;

open cur2;
	quote_loop: LOOP
        if (db_done OR loopcount=maxloops) then leave quote_loop; end if;

		fetch cur2 into trade_time, trading_symbol, trade_quantity, trade_price;
		/* sell */
		insert into F17336Pteam6.BuyAndHold values((select THIS_TIME from F17336Pteam6.CurrentPrice where SYMBOL = trading_symbol), trading_symbol, -trade_quantity, (select CURRENT_PRICE from F17336Pteam6.CurrentPrice where SYMBOL = trading_symbol));

		set loopcount = loopcount + 1;
	end LOOP;

close cur2;

END //
DELIMITER ;
