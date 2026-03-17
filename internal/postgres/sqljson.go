package postgres

func init() {
	set.Add("sqljson", "SQL/JSON constructors, IS JSON and any_value", sqljson)
}

func sqljson(s *Session) {
	if !s.AtLeast(16) {
		return
	}

	s.Note("SQL/JSON constructors")
	s.Show(`select json_object('name': 'ada', 'score': 91) as obj,
			json_array(1, 2, 3) as arr,
			json_object('id': 1 returning jsonb) as as_jsonb`)

	s.Exec(`drop table if exists scores`)
	s.Exec(`create table scores (team text, player text, points int)`)
	s.Exec(`insert into scores values ('red', 'ada', 10), ('red', 'grace', 7), ('blue', 'linus', 9)`)
	s.Show(`select team, json_arrayagg(player) as players, json_objectagg(player: points) as points
		from scores group by team order by team`)

	s.Note("IS JSON tells you what a string actually holds")
	s.Show(`select v,
			v is json as valid,
			v is json object as object,
			v is json array as array,
			v is json scalar as scalar
		from (values ('{"a":1}'), ('[1,2]'), ('7'), ('nope')) t(v)`)
	s.Show(`select '{"a":1,"a":2}' is json with unique keys as unique_keys`)

	s.Note("any_value picks any row per group, no fake aggregate needed")
	s.Show(`select team, any_value(player) as a_player, count(*) as n from scores group by team order by team`)

	s.Note("array_sample and array_shuffle")
	s.Show(`select array_sample('{1,2,3,4,5,6}'::int[], 3) as sampled,
			array_shuffle('{1,2,3,4,5}'::int[]) as shuffled`)

	s.Note("hex, octal, binary and readable literals")
	s.Show(`select 0x2f as hex, 0o17 as octal, 0b1011 as binary, 1_000_000 as readable`)
}
