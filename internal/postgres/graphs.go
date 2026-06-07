package postgres

func init() {
	set.Add("graphs", "property graphs over ordinary tables, and on conflict do select", graphs)
}

func graphs(s *Session) {
	if !s.AtLeast(19) {
		return
	}

	s.Note("graph queries over plain tables")
	s.Exec(`drop property graph if exists social`)
	s.Exec(`drop table if exists follows`)
	s.Exec(`drop table if exists person`)
	s.Exec(`create table person (id int primary key, name text, city text)`)
	s.Exec(`create table follows (
		follower_id int references person(id),
		followee_id int references person(id),
		since date
	)`)
	s.Exec(`insert into person values
		(1,'ada','london'), (2,'linus','helsinki'), (3,'grace','new york'),
		(4,'ken','murray hill'), (5,'barbara','philadelphia')`)
	s.Exec(`insert into follows values
		(1,2,'2026-01-01'), (2,3,'2026-01-05'), (3,4,'2026-02-01'),
		(1,3,'2026-02-10'), (4,5,'2026-03-01')`)

	s.Exec(`create property graph social
		vertex tables (person key (id) label person properties (name, city))
		edge tables (
			follows key (follower_id, followee_id)
				source key (follower_id) references person (id)
				destination key (followee_id) references person (id)
				label follows properties (since)
		)`)

	s.Note("who does ada follow")
	s.Show(`select * from graph_table (social
		match (a is person where a.name = 'ada') -[is follows]-> (b is person)
		columns (a.name as who, b.name as follows)
	)`)

	s.Note("two hops out, friends of friends")
	s.Show(`select * from graph_table (social
		match (a is person where a.name = 'ada') -[is follows]-> (b) -[is follows]-> (c)
		columns (a.name as who, b.name as via, c.name as reaches)
	)`)

	s.Note("edges carry properties too")
	s.Show(`select * from graph_table (social
		match (a) -[f is follows]-> (b)
		columns (a.name as who, b.name as whom, f.since as since)
	) order by since`)

	s.Note("an undirected edge looks both ways")
	s.Show(`select * from graph_table (social
		match (a is person where a.name = 'grace') -[is follows]- (b is person)
		columns (b.name as connected_to_grace)
	)`)

	s.Note("results are just rows, so join them like anything else")
	s.Show(`select g.who, p.city from graph_table (social
			match (a is person) -[is follows]-> (b is person where b.name = 'grace')
			columns (a.name as who)
		) g join person p on p.name = g.who order by g.who`)

	s.Note("on conflict do select, an atomic get or create")
	s.Exec(`drop table if exists tags`)
	s.Exec(`create table tags (id int generated always as identity primary key, name text unique)`)
	s.Exec(`insert into tags (name) values ('postgres')`)
	s.Show(`insert into tags (name) values ('postgres') on conflict (name) do select returning id, name`)
	s.Show(`insert into tags (name) values ('kafka') on conflict (name) do select returning id, name`)

	s.Note("copy straight out as json")
	s.CopyOut(`copy person to stdout (format json, force_array true)`)
}
