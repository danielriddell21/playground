package postgres

func init() {
	set.Add("windows", "window functions, frames and running totals", windows)
}

func windows(s *Session) {
	s.Exec(`drop table if exists sales`)
	s.Exec(`create table sales (region text, month date, amount int)`)
	s.Exec(`insert into sales values
		('north', '2026-01-01', 100), ('north', '2026-02-01', 140), ('north', '2026-03-01', 120),
		('south', '2026-01-01', 90),  ('south', '2026-02-01', 200), ('south', '2026-03-01', 160)`)

	s.Note("ranking within partitions")
	s.Show(`select region, month, amount,
			row_number() over w as rn,
			rank() over w as rnk,
			dense_rank() over w as dense
		from sales window w as (partition by region order by amount desc)
		order by region, rn`)

	s.Note("running total and moving average")
	s.Show(`select region, month, amount,
			sum(amount) over (partition by region order by month) as running,
			round(avg(amount) over (partition by region order by month rows between 1 preceding and current row), 1) as moving
		from sales order by region, month`)

	s.Note("lag and lead")
	s.Show(`select region, month, amount,
			lag(amount) over w as prev,
			amount - lag(amount) over w as delta,
			lead(amount) over w as next
		from sales window w as (partition by region order by month)
		order by region, month`)

	s.Note("first, last and ntile")
	s.Show(`select region, month, amount,
			first_value(amount) over w as first_month,
			last_value(amount) over (partition by region order by month rows between unbounded preceding and unbounded following) as last_month,
			ntile(2) over w as half
		from sales window w as (partition by region order by month)
		order by region, month`)

	s.Note("share of partition total")
	s.Show(`select region, month, amount,
			round(100.0 * amount / sum(amount) over (partition by region), 1) as pct_of_region
		from sales order by region, month`)
}
