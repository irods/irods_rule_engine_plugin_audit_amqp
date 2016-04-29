#!/bin/bash

cd /opt/irods-externals/qpid0.34-0/
./sbin/qpidd --log-to-file ./qpidd.log --log-enable trace+ --data-dir ./data

