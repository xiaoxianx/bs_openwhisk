#

FROM openwhisk/action-nodejs-v12:1.14.0-incubating AS ownode

FROM weaveworks/ignite-centos

RUN curl -sL https://rpm.nodesource.com/setup_10.x | bash -
RUN sudo yum -y install nodejs

COPY --from=ownode /nodejsAction /nodejsAction
COPY --from=ownode /node_modules /node_modules
COPY ./action.service /etc/systemd/system/

RUN chmod 664 /etc/systemd/system/action.service \
    && systemctl enable action.service
