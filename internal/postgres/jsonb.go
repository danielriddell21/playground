package postgres

func init() {
	set.Add("jsonb", "jsonb storage, operators, path queries and GIN indexes", jsonb)
}

func jsonb(s *Session) {
	s.Exec(`drop table if exists docs`)
	s.Exec(`create table docs (id serial primary key, body jsonb)`)
	s.Exec(`insert into docs (body) values
		('{"name":"ada","tags":["math","code"],"score":91,"meta":{"city":"london"}}'),
		('{"name":"linus","tags":["code","git"],"score":77,"meta":{"city":"helsinki"}}'),
		('{"name":"grace","tags":["navy","code"],"score":99,"meta":{"city":"new york"}}')`)

	s.Exec(`create index docs_body_idx on docs using gin (body)`)

	s.Note("field access and nested paths")
	s.Show(`select body->>'name' as name, body->'meta'->>'city' as city, body#>>'{tags,0}' as first_tag from docs order by 1`)

	s.Note("containment and key existence")
	s.Show(`select body->>'name' as name from docs where body @> '{"meta":{"city":"london"}}'`)
	s.Show(`select body->>'name' as name from docs where body->'tags' ? 'git'`)

	s.Note("jsonpath")
	s.Show(`select body->>'name' as name, jsonb_path_query_first(body, '$.score') as score
		from docs where jsonb_path_exists(body, '$.score ? (@ > 90)')`)

	s.Note("expanding and rebuilding")
	s.Show(`select d.body->>'name' as name, t.tag from docs d, jsonb_array_elements_text(d.body->'tags') as t(tag) order by 1, 2`)
	s.Show(`select jsonb_pretty(jsonb_agg(jsonb_build_object('name', body->>'name', 'score', body->'score'))) as rebuilt from docs`)

	s.Note("mutation")
	s.Show(`select jsonb_set(body, '{score}', '100') ->> 'score' as bumped from docs where body->>'name' = 'ada'`)
	s.Show(`select (body - 'meta') || '{"active":true}' as trimmed from docs where body->>'name' = 'ada'`)
}
