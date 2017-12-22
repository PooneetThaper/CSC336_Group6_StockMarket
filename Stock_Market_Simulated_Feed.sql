DELIMITER //
DROP PROCEDURE IF EXISTS sp_quote_feed_randparms //
/* first try just input to output just loop count*/
/* added input parameters to control switchpoint rand seed and amplitude rand seed*/
create procedure sp_quote_feed_randparms(IN loops int, IN switch_seed int, IN amp_seed int)
BEGIN
declare this_instrument int(11);
declare this_trade_date date;
declare this_trade_seq_nbr int(11);
declare this_trading_symbol varchar(15);
declare this_trade_time datetime;
declare this_trade_price decimal(18,4);
declare this_trade_size int(11);
/*loop count variables*/
declare loopcount int(11);
declare maxloops int(11);
/*variables for F17335Pteam6.TRADE_ADJUST values*/
declare tr_trade_price decimal(18,4);
declare tr_trade_seq_nbr int(11);
declare qa_amplitude decimal(18,4);
declare qa_switchpoint int(11);
declare qa_direction tinyint;
declare db_done int default false;
declare cur1 cursor for select * from F17336Pteam6.STOCK_TRADE use index for order by (XK2_STOCK_TRADE,XK4_STOCK_TRADE)  order by TRADE_SEQ_NBR,TRADE_TIME;
declare continue handler for not found set db_done=1;
set maxloops=loops*1000;
set loopcount=1;
open cur1;
   quote_loop: LOOP
        if (db_done OR loopcount=maxloops) then leave quote_loop; end if;
        fetch cur1 into this_instrument, this_trade_date, this_trade_seq_nbr, this_trading_symbol, this_trade_time, this_trade_price, this_trade_size;
        /*all update logic goes here...first get F17335Pteam6.TRADE_ADJUST values into variables*/
        select LAST_TRADE_PRICE, LAST_TRADE_SEQ_NBR, AMPLITUDE, SWITCHPOINT, DIRECTION into tr_trade_price,tr_trade_seq_nbr,qa_amplitude,qa_switchpoint,qa_direction from F17336Pteam6.TRADE_ADJUST where INSTRUMENT_ID=this_instrument;

            update F17336Pteam6.TRADE_ADJUST set LAST_TRADE_PRICE=this_trade_price where INSTRUMENT_ID=this_instrument;
            update F17336Pteam6.TRADE_ADJUST set LAST_TRADE_SEQ_NBR=this_trade_seq_nbr where INSTRUMENT_ID=this_instrument;

            if tr_trade_price > 0 then /*not first ask for this inst*/

                set this_trade_price=tr_trade_price+(ABS(this_trade_price-tr_trade_price)*qa_amplitude*qa_direction);
            else
                set this_trade_price=ABS(tr_trade_price+(ABS(this_trade_price-tr_trade_price)*qa_amplitude*qa_direction));
                end if;

/* in all cases check and reset switchpoint if needed reset amplitude and update dates*/
        if qa_switchpoint > 0 then

                        update F17336Pteam6.TRADE_ADJUST set SWITCHPOINT=SWITCHPOINT-1 where INSTRUMENT_ID=this_instrument ;
                    else  /*switchpoint <=0, recalculate switchpoint and change direction */
                        update F17336Pteam6.TRADE_ADJUST set SWITCHPOINT=ROUND((RAND()+.5)*switch_seed), DIRECTION=DIRECTION*-1 where INSTRUMENT_ID=this_instrument;
                end if;
        update F17336Pteam6.TRADE_ADJUST set AMPLITUDE=(RAND()+ amp_seed) where INSTRUMENT_ID=this_instrument;
        set this_trade_date=DATE_ADD(this_trade_date, INTERVAL 12 YEAR);
        set this_trade_time=DATE_ADD(this_trade_time, INTERVAL 12 YEAR);
/* now write out the record*/
        insert into F17336Pteam6.STOCK_TRADE_FEED values(this_instrument, this_trade_date, this_trade_seq_nbr, this_trading_symbol, this_trade_time, this_trade_price, this_trade_size);
        set loopcount=loopcount+1;
        END LOOP;
close cur1;
END //
DELIMITER ;
