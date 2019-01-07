## Overpedia Database mini-manual
Overpedia uses this as a temporary database for calculating statistics, as such this system is designed as a passthrough database:
1. `base.sql` imports the data from the csv files;
2. `indices/default.sql` (or `indices/alternate.sql`) calculates yearly conflict and polemic statistics;
3. `types.sql`,`query-pages.sql` and `query-toptenbyyear.sql` define utilities for data exporting.

### Requirements
You will need a machine with 300GB of storage and [docker storage base directory properly setted](https://forums.docker.com/t/how-do-i-change-the-docker-image-installation-directory/1169).

### Description of the Docker image
This image is aimed at simplifing overpedia data analysis. It takes in input the folder containing the csv files (`pages.csv`,`revisions.csv` and `socialjumps.csv`) and optionally sql database files, otherwise they will be retrieved from the image.

### Examples
1. `docker run -v /path/2/db:/db ebonetti/overpediadb --name myoverpediadb`:
..1. [mount as a volume](https://docs.docker.com/storage/volumes/) the guest `/db` folder to the host folder `/path/2/db`. This folder can be changed to an arbitrary folder of your choice;
..2. open a psql shell on the database, so that you can import the database with `\i base.sql;` and `\i indices/default.sql;`;
..3. save the container as `myoverpediadb` for later use, for further explanations please refer to [docker run reference](https://docs.docker.com/engine/reference/run).
2. `docker start -i myoverpediadb`: start the database container created with the previous command.
3. `docker rm myoverpediadb`: remove the database container.

### Useful commands
1. `docker pull ebonetti/overpediadb` Update the image to the last revision.
2. `docker kill --signal=SIGQUIT  $(docker ps -ql)` Quit the last container and log trace dump.
4. `docker logs -f $(docker ps -lq)` Fetch the logs of the last container.
5. `docker system prune -fa --volumes` Remove all unused images and volume without asking for confirmation.