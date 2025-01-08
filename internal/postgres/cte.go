package postgres

func init() {
	set.Add("cte", "common table expressions, recursion and data-modifying CTEs", cte)
}

func cte(s *Session) {
	s.Exec(`drop table if exists employees`)
	s.Exec(`create table employees (id int primary key, name text, manager_id int)`)
	s.Exec(`insert into employees values
		(1, 'ada', null),
		(2, 'linus', 1),
		(3, 'grace', 1),
		(4, 'ken', 2),
		(5, 'rob', 2),
		(6, 'barbara', 4)`)

	s.Note("plain CTE")
	s.Show(`with managers as (select distinct manager_id from employees where manager_id is not null)
		select e.name from employees e join managers m on m.manager_id = e.id order by 1`)

	s.Note("recursive tree walk")
	s.Show(`with recursive tree as (
			select id, name, manager_id, 1 as depth, name::text as path
			from employees where manager_id is null
			union all
			select e.id, e.name, e.manager_id, t.depth + 1, t.path || ' > ' || e.name
			from employees e join tree t on e.manager_id = t.id
		)
		select depth, name, path from tree order by path`)

	s.Note("recursive sequence generation")
	s.Show(`with recursive fib(a, b) as (
			select 0, 1
			union all
			select b, a + b from fib where b < 100
		)
		select a from fib`)

	s.Note("data-modifying CTE")
	s.Exec(`drop table if exists archive`)
	s.Exec(`create table archive (id int, name text)`)
	s.Show(`with moved as (
			delete from employees where manager_id = 2 returning id, name
		)
		insert into archive select id, name from moved returning *`)
	s.Show(`select count(*) as employees_left from employees`)

	s.Note("materialized hint")
	s.Show(`with counted as materialized (select manager_id, count(*) as reports from employees group by manager_id)
		select * from counted order by 1 nulls last`)
}
