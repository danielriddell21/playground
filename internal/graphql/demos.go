package graphql

func init() {
	set.Add("query", "ask for exactly the fields you want, nested in one round trip", queryDemo)
	set.Add("shape", "aliases, fragments and variables", shapeDemo)
	set.Add("mutation", "writes that return the new object", mutationDemo)
	set.Add("errors", "partial data alongside errors", errorsDemo)
	set.Add("introspection", "the schema describes itself", introspectionDemo)
}

func queryDemo(s *Session) {
	s.Note("ask for two fields and get exactly two fields")
	s.Query(`{ author(id: "a1") { name city } }`)

	s.Note("walk the graph, author to books to author again, one request")
	s.Query(`{
		author(id: "a1") {
			name
			books { title year author { name } }
		}
	}`)

	s.Note("arguments work on nested fields too")
	s.Query(`{ author(id: "a1") { name books(before: 1845) { title year } } }`)

	s.Note("a list field")
	s.Query(`{ authors { id name } }`)
}

func shapeDemo(s *Session) {
	s.Note("aliases let you ask for the same field twice")
	s.Query(`{
		ada: author(id: "a1") { name city }
		grace: author(id: "a2") { name city }
	}`)

	s.Note("fragments stop you repeating the field list")
	s.Query(`
		fragment card on Author { id name city }
		{
			one: author(id: "a1") { ...card }
			two: author(id: "a3") { ...card }
		}
	`)

	s.Note("variables keep the query static and the values separate")
	s.QueryWith(`query Find($term: String!) { search(term: $term) { title year } }`,
		map[string]any{"term": "on"})
	s.QueryWith(`query Find($term: String!) { search(term: $term) { title year } }`,
		map[string]any{"term": "compiler"})
}

func mutationDemo(s *Session) {
	s.Note("a write returns whatever shape you ask for")
	s.QueryWith(`
		mutation Add($title: String!, $year: Int, $authorId: String!) {
			addBook(title: $title, year: $year, authorId: $authorId) {
				id title year author { name }
			}
		}`,
		map[string]any{"title": "on engines", "year": 1846, "authorId": "a1"})

	s.Note("and the graph now includes it")
	s.Query(`{ author(id: "a1") { name books { title } } }`)

	s.Note("a bad reference comes back as an error")
	s.QueryWith(`mutation Add($t: String!, $a: String!) { addBook(title: $t, authorId: $a) { id } }`,
		map[string]any{"t": "orphan", "a": "nope"})
}

func errorsDemo(s *Session) {
	s.Note("one field fails, the rest of the response still arrives")
	s.Query(`{ author(id: "a1") { name city secret } }`)

	s.Note("asking for a field that does not exist fails before execution")
	s.Query(`{ author(id: "a1") { name nonsense } }`)

	s.Note("a missing required argument is caught by the schema")
	s.Query(`{ author { name } }`)

	s.Note("a required variable that was never supplied")
	s.QueryWith(`query Find($term: String!) { search(term: $term) { title } }`,
		map[string]any{})
}

func introspectionDemo(s *Session) {
	s.Note("what types exist")
	s.Query(`{ __schema { queryType { name } mutationType { name } } }`)

	s.Note("what does Author look like")
	s.Query(`{
		__type(name: "Author") {
			name
			fields { name type { name kind ofType { name } } }
		}
	}`)

	s.Note("what arguments does a field take")
	s.Query(`{
		__type(name: "Query") {
			fields { name args { name type { name kind ofType { name } } } }
		}
	}`)
}
