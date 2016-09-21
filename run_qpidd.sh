#!/bin/bash

cd /opt/irods-externals/qpid-with-proton0.34-0/
./sbin/qpidd --log-to-file ./qpidd.log --log-enable trace+ --data-dir ./data

