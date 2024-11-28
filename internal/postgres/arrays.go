package postgres

func init() {
	set.Add("arrays", "array columns, operators, unnest and aggregation", arrays)
}

func arrays(s *Session) {
	s.Exec(`drop table if exists posts`)
	s.Exec(`create table posts (id serial primary key, title text, tags text[], scores int[])`)
	s.Exec(`insert into posts (title, tags, scores) values
		('indexes', '{db,perf}', '{4,5,5}'),
		('vacuum', '{db,ops}', '{3,4}'),
		('generics', '{go}', '{5,5,5,4}')`)

	s.Note("membership and overlap")
	s.Show(`select title, tags from posts where 'db' = any(tags)`)
	s.Show(`select title from posts where tags && '{go,ops}'`)
	s.Show(`select title from posts where tags @> '{db,perf}'`)

	s.Note("shape and slicing")
	s.Show(`select title, array_length(scores, 1) as n, scores[1:2] as first_two, scores[array_upper(scores,1)] as last from posts order by 1`)

	s.Note("unnest back to rows")
	s.Show(`select title, s from posts, unnest(scores) as s order by title, s`)
	s.Show(`select title, round(avg(s), 2) as avg_score from posts, unnest(scores) as s group by title order by 2 desc`)

	s.Note("building arrays")
	s.Show(`select array_agg(distinct t order by t) as all_tags from posts, unnest(tags) as t`)
	s.Show(`select array(select generate_series(1, 5)) as built, array_cat('{a,b}'::text[], '{c}') as joined`)
	s.Show(`select title, array_to_string(tags, ' + ') as flat from posts order by 1`)

	s.Note("array index")
	s.Exec(`create index posts_tags_idx on posts using gin (tags)`)
	s.Show(`select count(*) as tagged_db from posts where tags @> '{db}'`)
}
