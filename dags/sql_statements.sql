insert into products_post ( select distinct a.* from (select id,deleted, releasedversion,productcode,productname,energy ,consumptiontype,modificationdate from products )a inner join (select id , max(modificationdate) as maxd from products group by 1)b on a.id=b.id and a.modificationdate=b.maxd order by 1 )

insert into prices_post ( select distinct a.* from (select id  ,productid ,pricecomponentid ,productcomponent ,price , unit ,valid_from ,valid_until , modificationdate from prices ) a inner join (select id , max(modificationdate) as maxd from prices group by 1)b on a.id=b.id and a.modificationdate=b.maxd order by 1 )

insert into contracts_post ( select distinct a.* from (select id ,type ,energy ,usage ,usagenet ,createdat ,startdate ,enddate ,fillingdatecancellation ,cancellationreason ,city ,status,productid , modificationdate from contracts ) a inner join (select id , max(modificationdate) as maxd from contracts group by 1)b on a.id=b.id and a.modificationdate=b.maxd order by 1 )


select a.*,b.id,b.productcode,b.productname from 
(select * from contracts_post ) a left outer join (select * from products_post) b on a.productid=b.id



select distinct createdat,usage,productcode,productname,pricecomponentid,productcomponent,price from
(select a.createdat,a.usage,b.id,b.productcode,b.productname from 
(select * from contracts_post ) a left outer join (select * from products_post) b  on a.productid=b.id and a.energy=b.energy ) a1 left outer join 
(select * from prices_post) c on a1.id=c.productid;


,c.pricecomponent,c.price
left outer join (select * from prices_post) c
and a.productid=c.productid and b.id=c.productid); 

select productname, createdat,sum(consumption) as consumption,sum(revenue) as revenue from (
select productname,createdat,usage as consumption, (baseprice+(usage*workingprice)) as revenue from (
select distinct a1.createdat,a1.usage,a1.productname,avg(workingprice) as workingprice, avg(baseprice) as baseprice from 
(select createdat,usage,productname, price as workingprice from output_to_analyst_mid where productcomponent='workingprice') a1
inner join
(select createdat,usage,productname, price as baseprice from output_to_analyst_mid where productcomponent='baseprice') b1
on a1.createdat=b1.createdat and a1.usage=b1.usage and a1.productname=b1.productname  
group by 1,2,3) as e order by productname , createdat) f group by 1,2 order by 1,2


select createdat,productname,sum(consumption) as consumption,sum(revenue) as revenue from ( select productname,createdat,usage as consumption, (baseprice+(usage*workingprice)) as revenue from ( select distinct a1.createdat,a1.usage,a1.productname,avg(workingprice) as workingprice, avg(baseprice) as baseprice from  (select createdat,usage,productname, price as workingprice from output_to_analyst_mid where productcomponent='workingprice') a1 inner join (select createdat,usage,productname, price as baseprice from output_to_analyst_mid where productcomponent='baseprice') b1 on a1.createdat=b1.createdat and a1.usage=b1.usage and a1.productname=b1.productname   group by 1,2,3) as e order by productname , createdat) f group by 1,2 order by 1,2
