[![Build Status](https://travis-ci.org/flowcommerce/lib-postgresql.svg?branch=master](https://travis-ci.org/flowcommerce/lib-postgresql)

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

## Docker

To enable Docker:

    cd <yourproject>-postgresql
    cp ../docker/templates/postgresql/Dockerfile .
    cp ../docker/templates/postgresql/install.sh .

Then edit install.sh, replacing <NAME> with the name of your project.

    git add install.sh Dockerfile
    git commit -m "Add Dockerfile"
