[![Build Status](https://travis-ci.org/flowcommerce/lib-postgresql.svg?branch=main)](https://travis-ci.org/flowcommerce/lib-postgresql)

# lib-postgresql

Libraries supporting postgresql at flow

## schema

to install:

    cd <yourproject>-postgresql
    ../lib-postgresql/schema/install

then git commit and push. This will create a single SQL script
providing the audit, journal, and util namespaces.

Note that this depends on https://github.com/mbryzek/schema-evolution-manager

## auditing

    create table organizations (
      guid                    uuid primary key,
      id                      text not null unique check (util.lower_non_empty_trimmed_string(id)),
      name                    text not null check (util.non_empty_trimmed_string(name))
    );

    select audit.setup('public', 'organizations');

To see what this has created:

    psql <db>
    \d organizations
    \d journal.organizations

This library uses the https://github.com/gilt/db-journaling library under the hood.

## Queue

You can create a a table to queue records for later processing:

```
select queue.create_queue('journal', 'catalogs', 'journal_queue', 'catalogs');
```

Example:

```
drop schema journal_queue cascade;
create schema journal_queue;
select queue.create_queue('journal', 'catalogs', 'journal_queue', 'catalogs');

insert into public.catalogs(id, organization_id, updated_by_user_id) values ('test', 'test-org', '1');
update public.catalogs set organization_id='test-org-2' where id = 'test';
select util.delete_by_id('1', 'public.catalogs', 'test');

select * from journal.catalogs;
select * from journal_queue.catalogs;
```

## Docker

To enable Docker:

    cd <yourproject>-postgresql
    cp ../docker/templates/postgresql/Dockerfile .
    cp ../docker/templates/postgresql/install.sh .

Then edit install.sh, replacing <NAME> with the name of your project.

    git add install.sh Dockerfile
    git commit -m "Add Dockerfile"

## Query 

`Query` is a wrapper around anorm to allow the creation of sql queries in a more functional way.

### Column functions:

```scala
val q = Query("SELECT * FROM organizations")
val q1 = q.equals("id", "org1")
val q1 = q.isTrue("processed")
val q2 = q.isNull("parent_id")
val q3 = q.notEquals("id", "org1")

val q4 = q.in("id", Seq("org1", "org2"))
val q5 = q.isTrue("processed")

val q6 = q.notIn("id", Seq("org1", "org2"))
val q7 = q.in2(("id", "name"), Seq(("o1", "n1"), ("o2", "n2"), ("o3", "n3")))
val q8 = q.in3(("id", "name", "key"), Seq(("o1", "n1", "k1"), ("o2", "n2", "k2"), ("o3", "n3", "k3")))

// Option
val q1 = q.equals("id", Some("org1"))
val q2 = q.notEquals("id", Some("org1"))

val q3 = q.inOptional("id", Some(Seq("org1", "org2")))
val q4 = q.notInOptional("id", Some(Seq("org1", "org2")))

val q5 = q.in2Optional(("id", "name"), Some(Seq(("o1", "n1"), ("o2", "n2"), ("o3", "n3"))))
val q6 = q.in3Optional(("id", "name", "key"), Some(Seq(("o1", "n1", "k1"), ("o2", "n2", "k2"), ("o3", "n3", "k3"))))

// If the value is None, the condition is ignored

// Less / Greater than
val q1 = q.lessThan("counter", 10)
val q2 = q.greaterThan("created_at", DateTime.now().minusDays(1))


```

### Subquery

```scala
val q = Query("SELECT * FROM organizations")

// And / Or
val q1 = q.or(Seq("parent_id is null", "channel_id is null"))
// SELECT * FROM organizations where parent_id is null or channel_id is null

val q2 = q.and(Seq("parent_id is null", "channel_id is null"))
// SELECT * FROM organizations where parent_id is null and channel_id is null

val actives = Query("SELECT * FROM organizations").equals("status", "active")
val webs = Query("SELECT id FROM organizations").equals("channel", "web")
val q1 = actives.notIn("id", webs)
// "SELECT * FROM organizations where status = trim('active') and id not in (select id from experiences where channel = trim('web'))"
```

### Custom bindings

```scala
// Define your own bindings
val id = "org1"
val name = "test"

// Use the bindings in the SQL query
val query = Query(s"SELECT * FROM organizations WHERE id = {id} AND name = {name}")

// Bind the variables
val bound: Query = query.bind("id", id).bind("name" -> name)
```

### Parsing results

Parsing is based on anorm parser.

```scala
val q = Query("SELECT id, name FROM organizations")
val res: List[(String, String)] = q.as((str("id") ~ str("name")).*).map { case id ~ name => (id, name) }
```
