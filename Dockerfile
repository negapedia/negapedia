FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

RUN set -eux; \
	apt-get update && apt-get install -y --no-install-recommends \
        postgresql-12 \
		g++ \
		gcc \
		libc6-dev \
		make \
		p7zip-full \
		curl \
		ca-certificates \
		git \
		pkg-config \
        default-jdk \
        python \
        python3-dev \
		python3-pip \
        python3-setuptools \
        python3-wheel \
        sudo; \
    sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen ;\
    locale-gen ;\
    apt-get -y install locales ;\
    pip3 install --no-cache-dir -U \
        nltk \
        cython \
        spacy[ja,th]; \
    python3 -m spacy download ca_core_news_sm; \
    python3 -m spacy download da_core_news_sm; \
    python3 -m spacy download de_core_news_sm; \
    python3 -m spacy download el_core_news_sm; \
    python3 -m spacy download en_core_web_sm; \
    python3 -m spacy download es_core_news_sm; \
    python3 -m spacy download fr_core_news_sm; \
    python3 -m spacy download it_core_news_sm; \
    python3 -m spacy download ja_core_news_sm; \
    python3 -m spacy download lt_core_news_sm; \
    python3 -m spacy download nl_core_news_sm; \
    python3 -m spacy download pl_core_news_sm; \
    python3 -m spacy download pt_core_news_sm; \
    python3 -m spacy download ro_core_news_sm; \
    python3 -m spacy download ru_core_news_sm; \
    python3 -m spacy download zh_core_web_sm; \
    python3 -m spacy download xx_ent_wiki_sm; \
	apt-get clean; \
	rm -rf /var/lib/apt/lists/*;

#complete locale definitions
ENV LANG=en_US.UTF-8 \ LANGUAGE=en_US \ LC_ALL=en_US.UTF-8
#install latest petsc
ENV PETSC_DOWNLOAD_URL https://ftp.mcs.anl.gov/pub/petsc/petsc-lite-3.9.tar.gz
ENV PETSC_ARCH arch-linux2-c-opt
ENV PETSC_DIR /usr/local/petsc
ENV PETSC_LIB $PETSC_DIR/$PETSC_ARCH/lib/
ENV LD_LIBRARY_PATH $PETSC_LIB:$LD_LIBRARY_PATH
RUN set -eux; \
    cd $PETSC_DIR/..; \
    curl -fsSL "$PETSC_DOWNLOAD_URL" -o petsc.tar.gz; \
    tar -xzf petsc.tar.gz; \
    rm petsc.tar.gz; \
    mv petsc* petsc; \
    cd $PETSC_DIR; \
    ./configure --with-cc=gcc --with-cxx=0 --with-fc=0 --with-debugging=0 \
        --download-mpich --download-f2cblaslapack; \
    make all test; \
    rm -rf /tmp/* /var/tmp/*;
#        PETSC configure Optimization: if compiled somewhere, may not work elsewhere.
#        COPTFLAGS='-O3 -march=native -mtune=native' \

#install latest golang
ENV GO_DIR /usr/local/go
ENV GOPATH /go
ENV PATH $GOPATH/bin:$GO_DIR/bin:$PATH
RUN set -eux; \
	cd $GO_DIR/..; \
    V=10; \
    while curl --output /dev/null --silent --head --fail "https://dl.google.com/go/go1.$V.linux-amd64.tar.gz"; do \
        GO_DOWNLOAD_URL="https://dl.google.com/go/go1.$V.linux-amd64.tar.gz"; \
        V=$((V+1)); \
    done; \
    V=$((V-1)); \
    v=1; \
    while curl --output /dev/null --silent --head --fail "https://dl.google.com/go/go1.$V.$v.linux-amd64.tar.gz"; do \
        GO_DOWNLOAD_URL="https://dl.google.com/go/go1.$V.$v.linux-amd64.tar.gz"; \
        v=$((v+1)); \
    done; \
    curl -fsSL "$GO_DOWNLOAD_URL" -o go.tar.gz; \
	tar -xzf go.tar.gz; \
	rm go.tar.gz; \
	mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"; \
	go version;

#install and compile the project
ENV PROJECT github.com/negapedia/negapedia
ADD . $GOPATH/src/$PROJECT
RUN set -eux; \
cd /; \
echo "#!/usr/bin/env bash\n\
set -e;\n\
echo \"STARTING PSQL\";\n\
/etc/init.d/postgresql start;\n\
echo \"SETTING UP FILES\";\n\
echo \"local   all             postgres                                trust\" >> /etc/postgresql/12/main/pg_hba.conf\n\
echo \"RESTARTING THE SERVER\";\n\
/etc/init.d/postgresql restart;\n\
echo \"CHANGING PWD POSTGRES\";\n\
sudo -u postgres psql -c \"ALTER USER postgres PASSWORD 'postgres';\"\n\
exec \"\$@\"\n" > /usr/local/bin/docker-entrypoint.sh; \
chmod a+x /usr/local/bin/docker-entrypoint.sh; \
go get $PROJECT/...;
RUN git clone https://github.com/negapedia/wikitfidf.git /go/src/github.com/negapedia/wikitfidf;

WORKDIR /data
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["refresh"]
