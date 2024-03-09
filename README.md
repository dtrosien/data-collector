# data-collector

- [Requirements](#requirements)
- [Developer Setup - Tools](#developer-setup---tools)
	- [SQLX:](#sqlx)
	- [Postgres Docker:](#postgres-docker)
	- [PSQL:](#psql)
	- [Setup Git Hook](#setup-git-hook)
- [Developer Setup - Building](#developer-setup---building)
	- [Start postgres, create database and migrate:](#start-postgres-create-database-and-migrate)
	- [Migration](#migration)
	- [Env File (.env)](#env-file-env)
- [Deployment](#deployment)
	- [Build Docker Image](#build-docker-image)
		- [update sqlx cli](#update-sqlx-cli)
		- [Prepare sqlx meta for offline mode](#prepare-sqlx-meta-for-offline-mode)
		- [In Case of Error or no file output etc:](#in-case-of-error-or-no-file-output-etc)
		- [Run Docker Build](#run-docker-build)
		- [Deploy on Digital Ocean (only required for repo owner):](#deploy-on-digital-ocean-only-required-for-repo-owner)


## Requirements
Have docker installed

## Developer Setup - Tools
Follow the sections below for installation details or run all Linux commands for developer setup at once.
<details>
	<summary>Linux all</summary>
Run all Linux commands to install the components. Docker is needed beforehand:	


```bash
## Postgres client (not the database)
sudo apt-get install -y postgresql-client
## Get Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
##  Get sqlx-cli
cargo install --version='~0.7.2' sqlx-cli --no-default-features --features rustls,postgres
## Get Postgres docker image
docker pull postgres
## Get nightly rust
rustup toolchain install nightly
## Prepare Git hooks
cargo install cargo-udeps --locked
```
</details>

### SQLX:

https://docs.rs/sqlx/latest/sqlx/

    cargo install --version='~0.7.2' sqlx-cli --no-default-features --features rustls,postgres

### Postgres Docker:

https://hub.docker.com/_/postgres

    docker pull postgres


### PSQL:

https://www.timescale.com/blog/how-to-install-psql-on-mac-ubuntu-debian-windows/

```bash
## Postgres client (not the database)
sudo apt-get install -y postgresql-client
```


### Setup Git Hook
Install Rust nightly version:
~~~bash
rustup toolchain install nightly
~~~
and then install the <a href="https://github.com/est31/cargo-udeps">udeps package</a>:
~~~bash
cargo install cargo-udeps --locked
~~~
Now ``cargo fmt`` and  ``cargo +nightly udeps --all-targets`` will be executed before each commit statement and `cargo fix --bin "data_collector" --allow-dirty` after the commit. See [rusty hook config file](.rusty-hook.toml) for details.

## Developer Setup - Building


### Start postgres, create database and migrate:

make script executable:

    chmod +x scripts/init_db.sh
run script from root directory:

    ./scripts/init_db.sh
(If there are still problems try `export PATH=$PATH:/root/.cargo/bin`)


### Migration
sqlx code for migrations are placed in migrations directory


### Env File (.env)
contains db information needed for compiling sqlx:
* sqlx reaches out to Postgres at compile-time to check that queries are well-formed. Just like sqlx-cli commands, it relies on the DATABASE_URL environment variable to know where to find the database.
* sqlx will read DATABASE_URL from it and save us the hassle of re-exporting the environment variable every single time.
* this is only needed for compiling, during runtime the configuration.yaml is used to change the db connection

## Deployment

### Build Docker Image
#### update sqlx cli
update sqlx cli to version of toml (same cargo install sqlx command from above)

    cargo install sqlx-cli
#### Prepare sqlx meta for offline mode
<i>This step is now build in into the git-hook; files should already exist. If not proceed as stated.</i> \
To create a json file in .sqlx which will be used in offline mode (needed to build docker) to check the queries, run:    
    
    cargo sqlx prepare -- --tests
The created file needs to be checked into git. (If you just get one file with the database in it, then something went wrong.)
#### In Case of Error or no file output etc:
If no file was created or not updated after the code was changed, run cargo clean and then try the prepare command again:

    cargo clean

#### Run Docker Build
When the file is up-to-date the docker build command should finish successfully (will take a few minutes). 

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



### How to use Jaeger to visualize traces provided via open_telemetry tracing
Run Jaeger docker (Port 4317 is used for grpc to transport the traces)

    docker run -d --name jaeger -e COLLECTOR_OTLP_ENABLED=true -p 16686:16686 -p 4317:4317 -p 4318:4318 jaegertracing/all-in-one:latest

The UI is then running at: http://localhost:16686

****

https://grafana.com/docs/grafana-cloud/monitor-applications/application-observability/setup/collector/opentelemetry-collector/

https://bademeister.grafana.net/connections/add-new-connection/collector-open-telemetry

    docker run -d --name opentelemetry-collector otel/opentelemetry-collector-contrib:latest

    docker run -d --name opentelemetry-collector -v .grafana/config.yaml -p 1888:1888 -p 8888:8888 -p 8889:8889 -p 13133:13133 -p 4317:4317 -p 4318:4318 -p 55679:55679 otel/opentelemetry-collector-contrib:latest

ports:

- 1888:1888 # pprof extension
- 8888:8888 # Prometheus metrics exposed by the Collector
- 8889:8889 # Prometheus exporter metrics
- 13133:13133 # health_check extension
- 4317:4317 # OTLP gRPC receiver
- 4318:4318 # OTLP http receiver
- 55679:55679 # zpages extension