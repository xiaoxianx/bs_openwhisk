FROM openwhisk/action-nodejs-v12:1.14.0-incubating AS ownode

FROM weaveworks/ignite-ubuntu

RUN curl -sL https://deb.nodesource.com/setup_12.x  | bash -
RUN apt-get install -y  nodejs

COPY --from=ownode /nodejsAction /nodejsAction
COPY --from=ownode /node_modules /node_modules
COPY ./action.service /etc/systemd/system/

RUN chmod 664 /etc/systemd/system/action.service \
    && systemctl enable action.service

