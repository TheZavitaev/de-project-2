
DROP TABLE IF EXISTS public.shipping;

--shipping
CREATE TABLE public.shipping(
   ID serial ,
   shippingid                         BIGINT,
   saleid                             BIGINT,
   orderid                            BIGINT,
   clientid                           BIGINT,
   payment_amount                          NUMERIC(14,2),
   state_datetime                    TIMESTAMP,
   productid                          BIGINT,
   description                       text,
   vendorid                           BIGINT,
   namecategory                      text,
   base_country                      text,
   status                            text,
   state                             text,
   shipping_plan_datetime            TIMESTAMP,
   hours_to_plan_shipping           NUMERIC(14,2),
   shipping_transfer_description     text,
   shipping_transfer_rate           NUMERIC(14,3),
   shipping_country                  text,
   shipping_country_base_rate       NUMERIC(14,3),
   vendor_agreement_description      text,
   PRIMARY KEY (ID)
);
CREATE INDEX shippingid ON public.shipping (shippingid);
COMMENT ON COLUMN public.shipping.shippingid is 'id of shipping of sale';


drop table if exists shipping_transfer CASCADE;  
create table public.shipping_transfer (
	transfer_type_id  			SERIAL,
    transfer_type 				TEXT ,
    transfer_model  			TEXT ,
    shipping_transfer_rate		NUMERIC(14, 3) ,
    PRIMARY KEY (transfer_type_id)
);

--migration
insert into public.shipping_transfer 
(transfer_type,  transfer_model, shipping_transfer_rate)
select distinct (regexp_split_to_array(shipping_transfer_description, E'\\:+'))[1] as transfer_type,
	(regexp_split_to_array(shipping_transfer_description, E'\\:+'))[2] as transfer_model,
	shipping_transfer_rate
	from shipping;

--rollback	
--нэту	

drop table if exists shipping_country CASCADE;  
create table public.shipping_country (
	shipping_country_id   		SERIAL,
    shipping_country 			TEXT ,
    shipping_country_base_rate	NUMERIC(14, 3) ,
    PRIMARY KEY (shipping_country_id)
);

insert into public.shipping_country 
(shipping_country,  shipping_country_base_rate)
select distinct shipping_country,
	shipping_country_base_rate
	from shipping;

drop table if exists shipping_agreement CASCADE;  
create table public.shipping_agreement (
	agreementid   				BIGINT,
    agreement_number 			text ,
    agreement_rate				NUMERIC(14, 2) ,
    agreement_commission		NUMERIC(14, 2) ,    
    PRIMARY KEY (agreementid)
);

insert into public.shipping_agreement 
(agreementid, agreement_number,  agreement_rate, agreement_commission)
select distinct (regexp_split_to_array(vendor_agreement_description, E'\\:+'))[1]::BIGINT as agreementid,
	(regexp_split_to_array(vendor_agreement_description, E'\\:+'))[2] as agreement_number,
	(regexp_split_to_array(vendor_agreement_description, E'\\:+'))[3]::NUMERIC(14, 3) as agreement_rate,
	(regexp_split_to_array(vendor_agreement_description, E'\\:+'))[4]::NUMERIC(14, 3) as agreement_commission
	from shipping;

drop table if exists shipping_info;  
create table public.shipping_info (
	shippingid   				BIGINT,
    vendorid		 			BIGINT ,
    payment						NUMERIC(14, 2) ,
    shipping_plan_datetime		TIMESTAMP , 
    transfer_type_id			BIGINT ,
    shipping_country_id			BIGINT ,
    agreementid					BIGINT ,
    PRIMARY KEY (shippingid),
    FOREIGN KEY (transfer_type_id) REFERENCES public.shipping_transfer(transfer_type_id) ON UPDATE CASCADE,
    FOREIGN KEY (shipping_country_id) REFERENCES public.shipping_country(shipping_country_id) ON UPDATE CASCADE,
    FOREIGN KEY (agreementid) REFERENCES public.shipping_agreement(agreementid) ON UPDATE CASCADE
);

insert into public.shipping_info 
(shippingid, vendorid,  payment, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)
select distinct sh.shippingid, sh.vendorid, sh.payment,
sh.shipping_plan_datetime, st.transfer_type_id, sc.shipping_country_id,  
(regexp_split_to_array(sh.vendor_agreement_description, E'\\:+'))[1]::BIGINT as agreementid
from public.shipping as sh 
inner join public.shipping_transfer as st  
	on sh.shipping_transfer_description = st.transfer_type || ':' || st.transfer_model and 
		sh.shipping_transfer_rate =  st.shipping_transfer_rate
inner join public.shipping_country as sc 
	on sh. shipping_country = sc.shipping_country and
	sh.shipping_country_base_rate = sc.shipping_country_base_rate;

drop table if exists shipping_status;  
create table public.shipping_status (
	shippingid   				BIGINT,
    status 		 				TEXT ,
    state 						TEXT ,
    shipping_start_fact_datetime TIMESTAMP , 
    shipping_end_fact_datetime	TIMESTAMP ,
    PRIMARY KEY (shippingid)
);

with max_date as (
select shippingid, max(state_datetime) as maxdate
	from shipping as sh
	group by shippingid 
),
booked as (
select shippingid, state_datetime as shipping_start_fact_datetime
	from shipping as sh
	where state = 'booked'),
	
received as (
select shippingid, state_datetime as shipping_end_fact_datetime
	from shipping as sh
	where state = 'recieved')

insert into shipping_status
(shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
select sh.shippingid, sh.status, sh.state, 
shipping_start_fact_datetime,
shipping_end_fact_datetime
	from shipping as sh
	inner join max_date as md on sh.shippingid = md.shippingid
	inner join booked as b on sh.shippingid = b.shippingid
	inner join received as r on sh.shippingid = r.shippingid
	where sh.state_datetime = md.maxdate;
	
	
create view shipping_datamart as (select si.shippingid, transfer_type, 
date_part('days', shipping_end_fact_datetime - shipping_start_fact_datetime) as full_day_at_shipping,
case when shipping_end_fact_datetime > shipping_plan_datetime then 1 else 0 end as is_delay,
case when status = 'finished' then 1 else 0 end as is_shipping_finish,
case when shipping_end_fact_datetime > shipping_plan_datetime then 
date_part('days', shipping_end_fact_datetime - shipping_plan_datetime) else 0 end as delay_day_at_shipping,
payment,
payment * shipping_country_base_rate * agreement_rate * shipping_transfer_rate as VAT,
payment * agreement_commission as profit 
	
	from shipping_info as si
	inner join shipping_status as ss on si.shippingid = ss.shippingid
	inner join shipping_transfer as st on si.transfer_type_id = st.transfer_type_id
	inner join shipping_country as sc on si.shipping_country_id = sc.shipping_country_id
	inner join shipping_agreement as sa on si.agreementid = sa.agreementid) 
	
	
	