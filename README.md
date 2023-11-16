# data-collector




## Requirements:

#### SQLX:

https://docs.rs/sqlx/latest/sqlx/

    cargo install --version='~0.7.2' sqlx-cli --no-default-features --features rustls,postgres

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
(If there are still problems try `export PATH=$PATH:/root/.cargo/bin`)


#### Migration
sqls code for migrations are placed in migrations directory


### Env File (.env)
contains db information needed for compiling sqlx:
* sqlx reaches out to Postgres at compile-time to check that queries are well-formed. Just like sqlx-cli commands, it relies on the DATABASE_URL environment variable to know where to find the database.
* sqlx will read DATABASE_URL from it and save us the hassle of re-exporting the environment variable every single time.
* this is only needed for compiling, during runtime the configuration.yaml is used to change the db connection

### Build Docker Image
#### update sqlx cli
update sqlx cli to version of toml (same cargo install sqlx command from above)
#### Prepare sqlx meta for offline mode
To create a json file in .sqlx which will be used in offline mode (needed to build docker) to check the queries, run:    
    
    cargo sqlx prepare
The created file needs to be checked into git. 
#### In Case of Error or no file output etc:
If no file was created or not updated after the code was changed, run cargo clean and then try the prepare command again:

    cargo clean

#### Run Docker Build
When the file is up-to-date the docker build command should finish successfully. 

    docker build --tag data-collector --file Dockerfile . 



#### Deploy on Digital Ocean (only required for repo owner):
Create Token at https://cloud.digitalocean.com/account/api/tokens (with write rights)
Authenticate:

    doctl auth init 

Deploy App:

    doctl apps create --spec spec.yaml 

Check Apps:

    doctl apps list 

migrate cloud db (might require disabling trusted sources temporarily https://docs.digitalocean.com/products/databases/postgresql/how-to/secure/):

    DATABASE_URL=YOUR-DIGITAL-OCEAN-DB-CONNECTION-STRING sqlx migrate run