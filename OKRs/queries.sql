#OKR 1: 2460

with first_uses as
(SELECT client_user_id, min(coalesce(finished_at, started_at )) as first_use 
FROM `belka-dh.business_layer.walks`
where status = "finished"
group by 1
UNION ALL
SELECT client_user_id, min(coalesce(finished_at, started_at )) as first_use
from business_layer.daycare
where status = "finished"
group by 1
UNION ALL
SELECT client_user_id, min(coalesce(payment_verified_at, started_at )) as first_use
FROM business_layer.pet_sitting
where status in ( "confirmed", "finished") 
group by 1
UNION ALL
SELECT client_user_id, min(coalesce(payment_verified_at, cast(checkin_date as timestamp))) as first_use
from business_layer.bookings
where has_converted is true
and country="bra"
group by 1 
), 
first_date as(
select client_user_id, min(first_use) as first_use
from first_uses
group by 1
)
select count(distinct client_user_id) as qty_new_client
from first_date
where first_use >= "2020-01-01"

#OKR 2: 1598

with first_uses as
(SELECT client_user_id, min(finished_at) as first_use
FROM `belka-dh.business_layer.walks`
where status = "finished"
and city in ('sao paulo', 'rio de janeiro')
group by 1
UNION ALL
SELECT client_user_id, min(finished_at) as first_use
from business_layer.daycare
where status = "finished"
and city in ('sao paulo', 'rio de janeiro')
group by 1
UNION ALL
SELECT client_user_id, min(payment_verified_at) as first_use
FROM business_layer.pet_sitting
where status in ( "confirmed", "finished") 
and city in ('sao paulo', 'rio de janeiro')
group by 1
UNION ALL
SELECT client_user_id, min(payment_verified_at) as first_use
from business_layer.bookings
where has_converted 
and country='bra'
and city in ('sao paulo', 'rio de janeiro')
group by 1 
), 
first_date as(
select client_user_id, min(first_use) as first_use
from first_uses
group by 1
), 
m0 as(
 select client_user_id, first_use
 from first_date fd
 join business_layer.users u on fd.client_user_id=u.user_id
 where date_diff( date(first_use), date(u.sign_up_at), day)<=30 
 and first_use>='2020-01-01'
 #confirmar se essa ativação M0 é só a partir da janeiro
 and date(u.sign_up_at) >= '2020-01-01'
)
 select count(distinct client_user_id) as qty_new_client
 from m0

#OKR 3: 862

with first_uses as
(SELECT client_user_id, min(finished_at) as first_use
FROM `belka-dh.business_layer.walks`
where status = "finished"
and city in ('sao paulo', 'rio de janeiro')
group by 1
UNION ALL
SELECT client_user_id, min(finished_at) as first_use
from business_layer.daycare
where status = "finished"
and city in ('sao paulo', 'rio de janeiro')
group by 1
UNION ALL
SELECT client_user_id, min(payment_verified_at) as first_use
FROM business_layer.pet_sitting
where status in ( "confirmed", "finished") 
and city in ('sao paulo', 'rio de janeiro')
group by 1
UNION ALL
SELECT client_user_id, min(payment_verified_at) as first_use
from business_layer.bookings
where has_converted 
and country='bra'
and city in ('sao paulo', 'rio de janeiro')
group by 1 
), 
first_date as(
select client_user_id, min(first_use) as first_use
from first_uses
group by 1
), 
m1plus as(
 select client_user_id, first_use
 from first_date fd
 join business_layer.users u on fd.client_user_id=u.user_id
 where date_diff( date(first_use), date(u.sign_up_at), day)>30
 and first_use>='2020-01-01'
)
 select count(distinct client_user_id) as qty_new_client
 from m1plus

#OKR 4: 189

with first_uses as
(SELECT client_user_id, min(finished_at) as first_use
FROM `belka-dh.business_layer.walks`
where status = "finished"
group by 1
UNION ALL
SELECT client_user_id, min(finished_at) as first_use
from business_layer.daycare
where status = "finished"
group by 1
UNION ALL
SELECT client_user_id, min(payment_verified_at) as first_use
FROM business_layer.pet_sitting
where status in ( "confirmed", "finished") 
group by 1
UNION ALL
SELECT client_user_id, min(payment_verified_at) as first_use
from business_layer.bookings
where has_converted 
and country='bra'
group by 1 
),
 
first_date as(
select client_user_id, min(first_use) as first_use
from first_uses
group by 1
), 

#faltava garantir que aquele era de fato o segundo uso da pessoa, sem essa verificação, 
#acabava pegando gente que já tinha usado um segundo (ou terceiro, etc) serviço antes
#criei uma verificação aqui para contar quantos usus a pessoa teve antes do período da análise e garantir que só foi apenas 1
take_just_second_use as
(
select fd.client_user_id, count(fu.client_user_id) as qty_uses
from first_date fd
join first_uses fu 
  on fd.client_user_id=fu.client_user_id
where fu.first_use < '2020-01-01'
group by 1
having qty_uses = 1
)

select count(distinct f2.client_user_id) as second_use 
from first_uses f2
join first_uses f1 on f1.client_user_id=f2.client_user_id
where f1.first_use>='2020-01-01' 
and f2.first_use<f1.first_use
and f2.client_user_id in (select client_user_id from take_just_second_use)


#OKR 6: 3.58%

with base as (
  select
    visitor_id,
    user_id,
    list_id,
    date(event_at) as date,
    event_at,
    case when event_name in ('search', 'view_search') then 'search'
    end as event_type,
    ST_GEOGPOINT(longitude_search, latitude_search) as my_point
  from `belka-dh.int_layer.events_info`
  where 
    event_name in ('search', 'view_search')
    and date(event_at) between '2020-01-01' and current_date()
),
searches_base as (
  select
    sb.*,
    nb.NAME_1 as state,
    nb.NAME_2 as city,
    nb.NAME_3 as neighborhood,
    nb.geom
  from
    base sb, `strelka.diva_gis.neighborhoods_bra` nb
  where
    ST_DWITHIN(sb.my_point, ST_GeogFromGeoJSON(nb.geom), 0)
    and event_type = 'search'
),
searches as (
  select *
  from searches_base
  where (state = 'São Paulo' and city = 'São Paulo')
    or (state = 'Rio de Janeiro' and city = 'Rio de Janeiro')
  and user_id not in (select user_id from business_layer.hosts where is_active_host is true)
),
bookings as (
  select
    client_user_id as user_id,
    list_id,
    first_message_at,
    date(first_message_at) as date
  from `belka-dh.business_layer.bookings` 
where first_message_at >= '2020-01-01'
and has_converted is true
and city in ('sao paulo', 'rio de janeiro')
),
pet_sittings as (
  select
    client_user_id as user_id,
    list_id,
    created_at as first_message_at,
    date(created_at) as date
  from
    `belka-dh.business_layer.pet_sitting` 
where created_at >= '2020-01-01'
and status in ('finished', 'confirmed')
and city in ('sao paulo', 'rio de janeiro')
),
daycares as (
  select
    d.client_user_id as user_id,
    l.list_id,
    first_message_at,
    date(first_message_at) as date
  from
    `belka-dh.business_layer.daycare` d
  left join
    business_layer.hosts l on l.user_id = d.host_user_id
  where first_message_at >= '2020-01-01'
  and status = 'finished'
  and d.city in ('sao paulo', 'rio de janeiro')
), 

all_clients as
(
select user_id, first_message_at, list_id from bookings
UNION ALL
select user_id, first_message_at, list_id  from pet_sittings
UNION ALL
select user_id, first_message_at, list_id  from daycares
)
  select count(distinct ac.user_id)/count(distinct s.visitor_id) as fulfillment  
  from searches s
  left join all_clients ac
    on s.user_id=ac.user_id 
    --and s.list_id=ac.list_id 
    and ac.first_message_at between s.event_at and timestamp_add(s.event_at, interval 5 day)

#OKR 7 e 8: 0.4073

select case when city in ( 'sao paulo' , 'rio de janeiro') then 'sp-rj'
else 'others' end as type,
count(distinct case when status_CS="finished" then walk_id end)/count(distinct walk_id) as CS
from business_layer.walks
where is_valid_for_CS_analysis is true and 
date(scheduled_at) between '2020-01-01' and current_date()
group by 1


____________________________________________________________ ****_________________________________________________________
schedule queries

6:


with base as (
  select
    visitor_id,
    user_id,
    list_id,
    date(event_at) as date,
    event_at,
    case when event_name in ('search', 'view_search') then 'search'
    end as event_type,
    ST_GEOGPOINT(longitude_search, latitude_search) as my_point
  from `belka-dh.int_layer.events_info`
  where 
    event_name in ('search', 'view_search')
    and date(event_at) between '2020-01-01' and current_date()
    and checkin_date<current_date()
),
searches_base as (
  select
    sb.*,
    nb.NAME_1 as state,
    nb.NAME_2 as city,
    nb.NAME_3 as neighborhood,
    nb.geom
  from
    base sb, `strelka.diva_gis.neighborhoods_bra` nb
  where
    ST_DWITHIN(sb.my_point, ST_GeogFromGeoJSON(nb.geom), 0)
    and event_type = 'search'
),
searches as (
  select *
  from searches_base
  where (state = 'São Paulo' and city = 'São Paulo')
    or (state = 'Rio de Janeiro' and city = 'Rio de Janeiro')
  and user_id not in (select user_id from business_layer.hosts where is_active_host is true)
),
bookings as (
  select
    client_user_id as user_id,
    list_id,
    first_message_at,
    date(first_message_at) as date
  from `belka-dh.business_layer.bookings` 
where first_message_at >= '2020-01-01'
and has_converted is true
and city in ('sao paulo', 'rio de janeiro')
and checkin_date<current_date()
),
pet_sittings as (
  select
    client_user_id as user_id,
    list_id,
    created_at as first_message_at,
    date(created_at) as date
  from
    `belka-dh.business_layer.pet_sitting` 
where created_at >= '2020-01-01'
and status in ('finished', 'confirmed')
and city in ('sao paulo', 'rio de janeiro')
),
daycares as (
  select
    d.client_user_id as user_id,
    l.list_id,
    first_message_at,
    date(first_message_at) as date
  from
    `belka-dh.business_layer.daycare` d
  left join
    business_layer.hosts l on l.user_id = d.host_user_id
  where first_message_at >= '2020-01-01'
  and status = 'finished'
  and d.city in ('sao paulo', 'rio de janeiro')
and date(checkin_scheduled_to)<current_date()
), 

all_clients as
(
select user_id, first_message_at, list_id from bookings
UNION ALL
select user_id, first_message_at, list_id  from pet_sittings
UNION ALL
select user_id, first_message_at, list_id  from daycares
)
  select 
   s.date as date,
  count(distinct ac.user_id)/count(distinct s.visitor_id) as fulfillment  
  from searches s
  left join all_clients ac
    on s.user_id=ac.user_id 
    --and s.list_id=ac.list_id 
    and ac.first_message_at between s.event_at and timestamp_add(s.event_at, interval 5 day)
    group by 1
    order by 1

7 e 8 :

select  date(scheduled_at) as date, count(case when city in ('sao paulo', 'rio de janeiro') and status_CS="finished" then walk_id  end) / count(case when city in ('sao paulo', 'rio de janeiro')  then walk_id  end) as walks_sp, count(case when city not in ('sao paulo', 'rio de janeiro') and status_CS="finished" then walk_id  end) / count(case when city not in ('sao paulo', 'rio de janeiro')  then walk_id  end) as walks_others
from business_layer.walks
where is_valid_for_CS_analysis is true and 
date(scheduled_at) between '2020-01-01' and current_date()
group by 1
order by 1
