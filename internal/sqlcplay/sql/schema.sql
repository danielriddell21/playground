CREATE TABLE authors (
    id   BIGSERIAL PRIMARY KEY,
    name text      NOT NULL,
    city text
);

CREATE TABLE books (
    id        BIGSERIAL PRIMARY KEY,
    author_id bigint NOT NULL REFERENCES authors(id),
    title     text   NOT NULL,
    year      int    NOT NULL,
    tags      text[]
);
