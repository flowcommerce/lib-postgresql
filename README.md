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
