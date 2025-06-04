package postgres

func init() {
	set.Add("views", "views, updatable views and materialized views", views)
}

func views(s *Session) {
	s.Exec(`drop materialized view if exists region_totals`)
	s.Exec(`drop view if exists big_orders`)
	s.Exec(`drop table if exists orders`)

	s.Exec(`create table orders (id serial primary key, region text, total numeric, placed_at date)`)
	s.Exec(`insert into orders (region, total, placed_at) values
		('north', 120, '2026-01-05'), ('north', 40, '2026-01-11'),
		('south', 300, '2026-01-07'), ('south', 80, '2026-02-02'),
		('east', 55, '2026-02-14')`)

	s.Note("view")
	s.Exec(`create view big_orders as select id, region, total from orders where total >= 100`)
	s.Show(`select * from big_orders order by id`)

	s.Note("views are updatable when simple enough")
	s.Exec(`update big_orders set total = total + 5 where region = 'north'`)
	s.Show(`select id, region, total from orders where region = 'north' order by id`)

	s.Note("materialized view")
	s.Exec(`create materialized view region_totals as
		select region, count(*) as orders, sum(total) as revenue from orders group by region`)
	s.Exec(`create unique index region_totals_idx on region_totals (region)`)
	s.Show(`select * from region_totals order by region`)

	s.Note("new rows are invisible until refresh")
	s.Exec(`insert into orders (region, total, placed_at) values ('east', 500, '2026-03-01')`)
	s.Show(`select * from region_totals where region = 'east'`)
	s.Exec(`refresh materialized view concurrently region_totals`)
	s.Show(`select * from region_totals where region = 'east'`)

	s.Note("check option guards writes through a view")
	s.Exec(`create or replace view big_orders as select id, region, total from orders where total >= 100 with check option`)
	s.Show(`select count(*) as visible from big_orders`)
}
