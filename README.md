## Overpedia refresh mini-manual
Overpedia refresh generator and development environment: this package and docker image is responsible of generating [negapedia](http://en.negapedia.org/), a website on social data extracted from [wikipedia](https://en.wikipedia.org).

### Requirements
You will need a machine with internet connection, 16GB of RAM, 300GB of storage and [docker storage base directory properly setted](https://forums.docker.com/t/how-do-i-change-the-docker-image-installation-directory/1169).

### Description of operations flow
This image take in input the nationalization - as of now only english and italian are supported - and store the result of the operations in `/data` (in-container folder). All the operation of data fetching are totally automatized and the result is [negapedia website](http://negapedia.org) in the form of a gzipped tarball of gzipped webpages. The operations flow is composed of thee phases:
1. preprocessing of data and exporting to csv - CPU intensive, it requires a good internet connection and 8GB of RAM.
2. construction of in-container database - IO intensive, requires 300GB of storage, best if SSD.
3. exporting and compressing the static website from quering the database.

### Refresh options
1. `lang`: wikipedia nationalization to parse (`en` or `it`), default `it`.
2. `source`: source of data (`net`,`csv` or `db`), default `net`.
3. `keep`: keep every savepoint - `csv` and `db` - after the execution (`true` or `false`), default `false`.

### Examples
1. `docker run ebonetti/overpedia refresh -lang en`: basic usage, run the image on the english nationalization and store the result in the in-containter `/data` folder.
2. `docker run -v /path/2/out/dir:/data ebonetti/overpedia --rm refresh -lang en`:
..1. run the image as before.
..2. [mount as a volume](https://docs.docker.com/storage/volumes/) the guest `/data` folder to the host folder `/path/2/out/dir`, the output folder, so that at the end of the operations  `/path/2/out/dir` will contain the result. This folder can be changed to an arbitrary folder of your choice.
..3. remove the image right after the execution.
3. `docker run -v /path/2/out/dir:/data  --rm --init -d ebonetti/overpedia refresh -lang en`, **you may want to use this commad** :
..1. run the image as before.
..2. run an init process that will take care of killing eventual zombie processes - just in case.
..3. run the image in detatched mode.
For further explanations please refer to [docker run reference](https://docs.docker.com/engine/reference/run)

### Useful commands
1. `docker pull ebonetti/overpedia` Updating the image to the last revision.
2. `docker logs -f $(docker ps -lq)` Remove all unused images not just dangling ones.
3. `docker system prune -a` Remove all unused images not just dangling ones.