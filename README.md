# data-collector




## Requirements:

#### SQLX:

https://docs.rs/sqlx/latest/sqlx/

    cargo install --version='~0.6' sqlx-cli --no-default-features --features rustls,postgres

#### Postgres Docker :

https://hub.docker.com/_/postgres

    docker pull postgres


#### PSQL:

https://www.timescale.com/blog/how-to-install-psql-on-mac-ubuntu-debian-windows/

## Notes:


### Start postgres, create database and migrate:

make script executable:

    chmod +x scripts/init_db.sh
run script:

    ./scripts/init_db.sh



#### Migration
sqls code for migrations are placed in migrations directory


### Env File (.env)
contains db information needed for compiling sqlx:
* sqlx reaches out to Postgres at compile-time to check that queries are well-formed. Just like sqlx-cli commands, it relies on the DATABASE_URL environment variable to know where to find the database.
* sqlx will read DATABASE_URL from it and save us the hassle of re-exporting the environment variable every single time.
* this is only needed for compiling, during runtime the configuration.yaml is used to change the db connection