analyse;

--- 1.1
explain
select sum(lo_revenue) as revenue
from lineorder
         left join date on lo_orderdate = d_datekey
where d_year = 1993
  and lo_discount between 1 and 3
  and lo_quantity < 25;
--- THIS ONE

/*
+----------------------------------------------------------------------------------------------------+
|QUERY PLAN                                                                                          |
+----------------------------------------------------------------------------------------------------+
|Finalize Aggregate  (cost=1848145.93..1848145.94 rows=1 width=8)                                    |
|  ->  Gather  (cost=1848145.71..1848145.92 rows=2 width=8)                                          |
|        Workers Planned: 2                                                                          |
|        ->  Partial Aggregate  (cost=1847145.71..1847145.72 rows=1 width=8)                         |
|              ->  Hash Join  (cost=74.53..1845384.41 rows=704520 width=4)                           |
|                    Hash Cond: (lineorder.lo_orderdate = date.d_datekey)                            |
|                    ->  Parallel Seq Scan on lineorder  (cost=0.00..1832332.72 rows=4935504 width=8)|
|                          Filter: ((lo_discount >= 1) AND (lo_discount <= 3) AND (lo_quantity < 25))|
|                    ->  Hash  (cost=69.96..69.96 rows=365 width=4)                                  |
|                          ->  Seq Scan on date  (cost=0.00..69.96 rows=365 width=4)                 |
|                                Filter: (d_year = 1993)                                             |
+----------------------------------------------------------------------------------------------------+

Will do a partial aggregate in parallel with 2 workers.
each worker will:
     1 - hash all entries of table date's year to sequentially search for dates where the year is 1993.
     2 - filter all entries of line order where (lo_discount >= 1) AND (lo_discount <= 3) AND (lo_quantity < 25))
     3 - join the lineorder and date tables where lineorder.lo_orderdate = date.d_datekey using the hashes of these parameters
     4 - calculate the partial aggregate (sum of lo_revenue in this case)
After each worker is done, the aggregate is finalized by summing both partial aggregates
*/

--- 2.1

explain
select sum(lo_revenue) as lo_revenue, d_year, p_brand1
from lineorder
         left join date on lo_orderdate = d_datekey
         left join part on lo_partkey = p_partkey
         left join supplier on lo_suppkey = s_suppkey
where p_category = 'MFGR#12' and s_region = 'AMERICA'
group by d_year, p_brand1
order by d_year, p_brand1;
/*
+------------------------------------------------------------------------------------------------------------------------+
|QUERY PLAN                                                                                                              |
+------------------------------------------------------------------------------------------------------------------------+
|Finalize GroupAggregate  (cost=1683186.70..1683396.70 rows=7000 width=21)                                               |
|  Group Key: date.d_year, part.p_brand1                                                                                 |
|  ->  Sort  (cost=1683186.70..1683221.70 rows=14000 width=21)                                                           |
|        Sort Key: date.d_year, part.p_brand1                                                                            |
|        ->  Gather  (cost=1680752.58..1682222.58 rows=14000 width=21)                                                   |
|              Workers Planned: 2                                                                                        |
|              ->  Partial HashAggregate  (cost=1679752.58..1679822.58 rows=7000 width=21)                               |
|                    Group Key: date.d_year, part.p_brand1                                                               |
|                    ->  Hash Left Join  (cost=22843.27..1677373.19 rows=317252 width=17)                                |
|                          Hash Cond: (lineorder.lo_orderdate = date.d_datekey)                                          |
|                          ->  Hash Join  (cost=22747.74..1676443.49 rows=317252 width=17)                               |
|                                Hash Cond: (lineorder.lo_suppkey = supplier.s_suppkey)                                  |
|                                ->  Hash Join  (cost=21878.00..1671449.80 rows=1570811 width=21)                        |
|                                      Hash Cond: (lineorder.lo_partkey = part.p_partkey)                                |
|                                      ->  Parallel Seq Scan on lineorder  (cost=0.00..1551161.27 rows=37489527 width=16)|
|                                      ->  Hash  (cost=21459.00..21459.00 rows=33520 width=13)                           |
|                                            ->  Seq Scan on part  (cost=0.00..21459.00 rows=33520 width=13)             |
|                                                  Filter: ((p_category)::text = 'MFGR#12'::text)                        |
|                                ->  Hash  (cost=794.00..794.00 rows=6059 width=4)                                       |
|                                      ->  Seq Scan on supplier  (cost=0.00..794.00 rows=6059 width=4)                   |
|                                            Filter: ((s_region)::text = 'AMERICA'::text)                                |
|                          ->  Hash  (cost=63.57..63.57 rows=2557 width=8)                                               |
|                                ->  Seq Scan on date  (cost=0.00..63.57 rows=2557 width=8)                              |
+------------------------------------------------------------------------------------------------------------------------+
Will do a partial HashAggregate in parallel with 2 workers.
Each worker will:
    1 - Filter parts table where p_category = 'MFGR#12' with a parallel sequential hash scan
    2 - Join lineorder and parts tables with Hash Join where lineorder.lo_partkey = part.p_partkey
    3 - Filter supplier table where s_region = 'AMERICA' with a parallel sequential hash scan
    4 - Join table resulting from point 2 with supplier table where lineorder.lo_suppkey = supplier.s_suppkey
    5 - Join table resulting from point 4 with date table where lineorder.lo_orderdate = date.d_datekey
    6 - calculate partial aggregates for groups of distinct pairs (date.d_year, part.p_brand1), with each group being identifier by their hash
When each group is done the final GroupAggregate is calculating by summing the results of each worker.
 */


--- this one is the only one using an index index

explain
select sum(lo_revenue) as lo_revenue, d_year, p_brand1
from lineorder
         left join date on lo_orderdate = d_datekey
         left join part on lo_partkey = p_partkey
         left join supplier on lo_suppkey = s_suppkey
where p_brand1 between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA'
group by d_year, p_brand1
order by d_year, p_brand1;

/*
+-----------------------------------------------------------------------------------------------------------------------------------------------+
|QUERY PLAN                                                                                                                                     |
+-----------------------------------------------------------------------------------------------------------------------------------------------+
|Finalize GroupAggregate  (cost=1674789.60..1674795.46 rows=21 width=21)                                                                        |
|  Group Key: date.d_year, part.p_brand1                                                                                                        |
|  ->  Gather Merge  (cost=1674789.60..1674794.94 rows=42 width=21)                                                                             |
|        Workers Planned: 2                                                                                                                     |
|        ->  Partial GroupAggregate  (cost=1673789.58..1673790.07 rows=21 width=21)                                                             |
|              Group Key: date.d_year, part.p_brand1                                                                                            |
|              ->  Sort  (cost=1673789.58..1673789.65 rows=28 width=17)                                                                         |
|                    Sort Key: date.d_year, part.p_brand1                                                                                       |
|                    ->  Hash Left Join  (cost=23554.86..1673788.90 rows=28 width=17)                                                           |
|                          Hash Cond: (lineorder.lo_orderdate = date.d_datekey)                                                                 |
|                          ->  Nested Loop  (cost=23459.32..1673693.30 rows=28 width=17)                                                        |
|                                ->  Hash Join  (cost=23459.04..1673030.84 rows=140 width=21)                                                   |
|                                      Hash Cond: (lineorder.lo_partkey = part.p_partkey)                                                       |
|                                      ->  Parallel Seq Scan on lineorder  (cost=0.00..1551161.27 rows=37489527 width=16)                       |
|                                      ->  Hash  (cost=23459.00..23459.00 rows=3 width=13)                                                      |
|                                            ->  Seq Scan on part  (cost=0.00..23459.00 rows=3 width=13)                                        |
|                                                  Filter: (((p_brand1)::text >= 'MFGR#2221'::text) AND ((p_brand1)::text <= 'MFGR#2228'::text))|
|                                ->  Index Scan using supplier_pkey on supplier  (cost=0.29..4.73 rows=1 width=4)                               |
|                                      Index Cond: (s_suppkey = lineorder.lo_suppkey)                                                           |
|                                      Filter: ((s_region)::text = 'ASIA'::text)                                                                |
|                          ->  Hash  (cost=63.57..63.57 rows=2557 width=8)                                                                      |
|                                ->  Seq Scan on date  (cost=0.00..63.57 rows=2557 width=8)                                                     |
+-----------------------------------------------------------------------------------------------------------------------------------------------+
Will do a GroupAggregate in parallel with 2 workers.
each worker will:
    1 - filter results from part table where p_brand1 between 'MFGR#2221' and 'MFGR#2228'
    2 - join lineorder table with results from 1 where lineorder.lo_partkey = part.p_partkey
    3 - using the supplier table primary key as an index will filter results from supplier where s_region = 'ASIA'
    4 - join results from 2 and 3 using a nested loop where lineorder.lo_orderdate = date.d_datekey
    5 - join table date with results from 4 where lineorder.lo_orderdate = date.d_datekey
    6 - sort results by date.d_year and part.p_brand
*/


--- 3.1

explain
select c_nation, s_nation, d_year, sum(lo_revenue) as lo_revenue
from lineorder
         left join date on lo_orderdate = d_datekey
         left join customer on lo_custkey = c_custkey
         left join supplier on lo_suppkey = s_suppkey
where c_region = 'ASIA' and s_region = 'ASIA' and d_year >= 1992 and d_year <= 1997
group by c_nation, s_nation, d_year
order by d_year asc, lo_revenue desc;
/*
+------------------------------------------------------------------------------------------------------------------------------+
|QUERY PLAN                                                                                                                    |
+------------------------------------------------------------------------------------------------------------------------------+
|Sort  (cost=1793542.97..1793553.91 rows=4375 width=28)                                                                        |
|  Sort Key: date.d_year, (sum(lineorder.lo_revenue)) DESC                                                                     |
|  ->  Finalize GroupAggregate  (cost=1793125.27..1793278.39 rows=4375 width=28)                                               |
|        Group Key: customer.c_nation, supplier.s_nation, date.d_year                                                          |
|        ->  Sort  (cost=1793125.27..1793147.14 rows=8750 width=28)                                                            |
|              Sort Key: customer.c_nation, supplier.s_nation, date.d_year                                                     |
|              ->  Gather  (cost=1791633.61..1792552.36 rows=8750 width=28)                                                    |
|                    Workers Planned: 2                                                                                        |
|                    ->  Partial HashAggregate  (cost=1790633.61..1790677.36 rows=4375 width=28)                               |
|                          Group Key: customer.c_nation, supplier.s_nation, date.d_year                                        |
|                          ->  Hash Join  (cost=15048.19..1777405.82 rows=1322779 width=24)                                    |
|                                Hash Cond: (lineorder.lo_orderdate = date.d_datekey)                                          |
|                                ->  Hash Join  (cost=14944.44..1773244.88 rows=1543040 width=24)                              |
|                                      Hash Cond: (lineorder.lo_custkey = customer.c_custkey)                                  |
|                                      ->  Hash Join  (cost=869.50..1650454.54 rows=7547891 width=20)                          |
|                                            Hash Cond: (lineorder.lo_suppkey = supplier.s_suppkey)                            |
|                                            ->  Parallel Seq Scan on lineorder  (cost=0.00..1551161.27 rows=37489527 width=16)|
|                                            ->  Hash  (cost=794.00..794.00 rows=6040 width=12)                                |
|                                                  ->  Seq Scan on supplier  (cost=0.00..794.00 rows=6040 width=12)            |
|                                                        Filter: ((s_region)::text = 'ASIA'::text)                             |
|                                      ->  Hash  (cost=12475.00..12475.00 rows=91995 width=12)                                 |
|                                            ->  Seq Scan on customer  (cost=0.00..12475.00 rows=91995 width=12)               |
|                                                  Filter: ((c_region)::text = 'ASIA'::text)                                   |
|                                ->  Hash  (cost=76.35..76.35 rows=2192 width=8)                                               |
|                                      ->  Seq Scan on date  (cost=0.00..76.35 rows=2192 width=8)                              |
|                                            Filter: ((d_year >= 1992) AND (d_year <= 1997))                                   |
+------------------------------------------------------------------------------------------------------------------------------+
Will do a partial GroupAggregate in parallel with 2 workers.
Each worker will:
    1 - filter results from supplier table where s_region = 'ASIA', using an hash sequential scan
    2 - join lineorder table with supplier table where lineorder.lo_suppkey = supplier.s_suppkey using a parallel sequential scan
    3 - filter results from customer table where c_region = 'ASIA', using an hash sequential scan
    4 - Join tables resulting from 2 and 3 where lineorder.lo_custkey = customer.c_custkey
    5 - filter results from date table where (d_year >= 1992) AND (d_year <= 1997)
    6 - join tables resulting from 4 and 5 where lineorder.lo_orderdate = date.d_datekey
    7 - sort the results from 6 by the fields customer.c_nation, supplier.s_nation and date.d_year
    8 - Finalize the aggregation by the unique combinations of customer.c_nation, supplier.s_nation and date.d_year
    9 - sort results by date.d_year ascending and (sum(lineorder.lo_revenue)) descending
Once each worker is done, the results of the aggregates calculated by each worker are once again aggregated
Once the final aggregate is completed results are sorted by date.d_year ascending and (sum(lineorder.lo_revenue)) descending
*/

--- 4.1

explain
select d_year, c_nation, sum(lo_revenue) - sum(lo_supplycost) as profit
from lineorder
         left join date on lo_orderdate = d_datekey
         left join customer on lo_custkey = c_custkey
         left join supplier on lo_suppkey = s_suppkey
         left join part on lo_partkey = p_partkey
    where c_region = 'AMERICA'
      and s_region = 'AMERICA'
      and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by d_year, c_nation
order by d_year, c_nation;

/*


+------------------------------------------------------------------------------------------------------------------------------+
|QUERY PLAN                                                                                                                    |
+------------------------------------------------------------------------------------------------------------------------------+
|Finalize GroupAggregate  (cost=1832680.12..1832686.68 rows=175 width=20)                                                      |
|  Group Key: date.d_year, customer.c_nation                                                                                   |
|  ->  Sort  (cost=1832680.12..1832680.99 rows=350 width=28)                                                                   |
|        Sort Key: date.d_year, customer.c_nation                                                                              |
|        ->  Gather  (cost=1832628.58..1832665.33 rows=350 width=28)                                                           |
|              Workers Planned: 2                                                                                              |
|              ->  Partial HashAggregate  (cost=1831628.58..1831630.33 rows=175 width=28)                                      |
|                    Group Key: date.d_year, customer.c_nation                                                                 |
|                    ->  Hash Left Join  (cost=43177.90..1826176.29 rows=545229 width=20)                                      |
|                          Hash Cond: (lineorder.lo_orderdate = date.d_datekey)                                                |
|                          ->  Hash Join  (cost=43082.36..1824647.16 rows=545229 width=20)                                     |
|                                Hash Cond: (lineorder.lo_partkey = part.p_partkey)                                            |
|                                ->  Hash Join  (cost=14916.24..1773551.01 rows=1520384 width=24)                              |
|                                      Hash Cond: (lineorder.lo_custkey = customer.c_custkey)                                  |
|                                      ->  Hash Join  (cost=869.74..1650454.77 rows=7571635 width=20)                          |
|                                            Hash Cond: (lineorder.lo_suppkey = supplier.s_suppkey)                            |
|                                            ->  Parallel Seq Scan on lineorder  (cost=0.00..1551161.27 rows=37489527 width=24)|
|                                            ->  Hash  (cost=794.00..794.00 rows=6059 width=4)                                 |
|                                                  ->  Seq Scan on supplier  (cost=0.00..794.00 rows=6059 width=4)             |
|                                                        Filter: ((s_region)::text = 'AMERICA'::text)                          |
|                                      ->  Hash  (cost=12475.00..12475.00 rows=90360 width=12)                                 |
|                                            ->  Seq Scan on customer  (cost=0.00..12475.00 rows=90360 width=12)               |
|                                                  Filter: ((c_region)::text = 'AMERICA'::text)                                |
|                                ->  Hash  (cost=23459.00..23459.00 rows=286890 width=4)                                       |
|                                      ->  Seq Scan on part  (cost=0.00..23459.00 rows=286890 width=4)                         |
|                                            Filter: (((p_mfgr)::text = 'MFGR#1'::text) OR ((p_mfgr)::text = 'MFGR#2'::text))  |
|                          ->  Hash  (cost=63.57..63.57 rows=2557 width=8)                                                     |
|                                ->  Seq Scan on date  (cost=0.00..63.57 rows=2557 width=8)                                    |
+------------------------------------------------------------------------------------------------------------------------------+
Will do a GroupAggregate in parallel with 2 workers.
Each worker will:
    1 - filter results from supplier table where s_region = 'AMERICA'
    2 - join lineorder and supplier tables where lineorder.lo_custkey = customer.c_custkey
    3 - filter results from customer table where c_region = 'AMERICA'
    4 - join results from 2 and 3 where lineorder.lo_custkey = customer.c_custkey
    5 - filter results from part table where (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
    6 - join results of 4 and 5 where lineorder.lo_partkey = part.p_partkey
    7 - join results of 6 with table date where lineorder.lo_orderdate = date.d_datekey
    8 - sort results from 7 by date.d_year and customer.c_nation
Once each worker is done, the results of the aggregates calculated by each worker are once again aggregated
*/