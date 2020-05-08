from __future__ import print_function

import optparse
import os
import shutil
import glob
import time
import tempfile
import irods_python_ci_utilities


def get_build_prerequisites_all():
    return['gcc', 'swig']

def get_build_prerequisites_apt():
    return get_build_prerequisites_all()+['default-jre','uuid-dev', 'libssl-dev', 'libsasl2-2', 'libsasl2-dev', 'python-dev']

def get_build_prerequisites_yum():
    return get_build_prerequisites_all()+['java-1.7.0-openjdk-devel', 'libuuid-devel', 'openssl-devel', 'cyrus-sasl-devel', 'python-devel']

def get_build_prerequisites_zypper():
    return get_build_prerequisites_all()+['java-1.7.0-openjdk-devel']

def get_build_prerequisites():
    dispatch_map = {
        'Ubuntu': get_build_prerequisites_apt,
        'Centos': get_build_prerequisites_yum,
        'Centos linux': get_build_prerequisites_yum,
        'Opensuse': get_build_prerequisites_zypper,
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()

def install_build_prerequisites():
    irods_python_ci_utilities.install_os_packages(get_build_prerequisites())

def install_messaging_package(message_broker):
    if 'apache-activemq-' in message_broker:
        version_number = message_broker.split('-')[2]
        tarfile = message_broker + '-bin.tar.gz'
        url = 'http://archive.apache.org/dist/activemq/' + version_number + '/' + tarfile
        activemq_dir = message_broker + '/bin/activemq'

        irods_python_ci_utilities.subprocess_get_output(['wget', url])
        irods_python_ci_utilities.subprocess_get_output(['tar', 'xvfz', tarfile])
        irods_python_ci_utilities.subprocess_get_output([activemq_dir, 'start'])

    if 'rabbitmq' in message_broker:
        if irods_python_ci_utilities.get_distribution() == 'Ubuntu':
            irods_python_ci_utilities.subprocess_get_output('curl -s https://packagecloud.io/install/repositories/rabbitmq/rabbitmq-server/script.deb.sh | sudo bash', shell=True)
            irods_python_ci_utilities.subprocess_get_output(['wget', 'https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'dpkg', '-i', 'erlang-solutions_1.0_all.deb'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'update', '-y'])
            if irods_python_ci_utilities.get_distribution_version_major() == '18':
                irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'esl-erlang', '-y'])
            else:
                irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'esl-erlang=1:19.3.6', '-y'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'rabbitmq-server', '-y'])

        if irods_python_ci_utilities.get_distribution() == 'Centos' or irods_python_ci_utilities.get_distribution() == 'Centos linux':
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'rpm', '--rebuilddb'])
            irods_python_ci_utilities.subprocess_get_output('curl -s https://packagecloud.io/install/repositories/rabbitmq/rabbitmq-server/script.rpm.sh | sudo bash', shell=True)
            irods_python_ci_utilities.subprocess_get_output('curl -s https://packagecloud.io/install/repositories/rabbitmq/erlang/script.rpm.sh | sudo bash', shell=True)
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'yum', 'install', 'erlang', '-y'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'yum', 'install', 'rabbitmq-server', '-y'])
            if irods_python_ci_utilities.get_distribution_version_major() == '6':
                irods_python_ci_utilities.subprocess_get_output(['sudo', 'update-rc.d', 'rabbitmq-server', 'defaults'])
            else:
                irods_python_ci_utilities.subprocess_get_output(['sudo', 'systemctl', 'enable', 'rabbitmq-server'])

            irods_python_ci_utilities.subprocess_get_output(['sudo', 'service', 'rabbitmq-server', 'start'])

        irods_python_ci_utilities.subprocess_get_output(['sudo', 'rabbitmq-plugins', 'enable', 'rabbitmq_amqp1_0'])

def main():
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory')
    parser.add_option('--built_packages_root_directory')
    parser.add_option('--message_broker', default='apache-activemq-5.14.1', help='MQ server package name that needs to be tested')
    options, _ = parser.parse_args()

    output_root_directory = os.path.join(options.output_root_directory, options.message_broker)
    built_packages_root_directory = options.built_packages_root_directory
    package_suffix = irods_python_ci_utilities.get_package_suffix()
    os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(built_packages_root_directory)

    install_build_prerequisites()
    irods_python_ci_utilities.subprocess_get_output(['sudo', '-EH', 'pip', 'install', 'unittest-xml-reporting==1.14.0', 'python-qpid-proton==0.30.0'])
    install_messaging_package(options.message_broker)

    irods_python_ci_utilities.install_os_packages_from_files(glob.glob(os.path.join(os_specific_directory, 'irods-rule-engine-plugin-audit-amqp*.{0}'.format(package_suffix))))
    irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'python scripts/add_audit_rule_engine_to_rule_engines.py'], check_rc=True)

    time.sleep(10)

    test_audit_log = 'log/test_audit_plugin.log'
    test_output_file = 'log/test_output.log'
    try:
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'python2 scripts/run_tests.py --xml_output --run_s=test_audit_plugin 2>&1 | tee {0}; exit $PIPESTATUS'.format(test_audit_log)], check_rc=True)
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'python2 scripts/run_tests.py --xml_output --run_s=test_resource_types.Test_Resource_Unixfilesystem 2>&1 | tee {0}; exit $PIPESTATUS'.format(test_output_file)], check_rc=True)
    finally:
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            test_output_file = os.path.join('/var/lib/irods', test_output_file)
            if os.path.exists(test_output_file):
                shutil.copy(test_output_file, output_root_directory)
            test_audit_log = os.path.join('/var/lib/irods', test_audit_log)
            if os.path.exists(test_audit_log):
                shutil.copy(test_audit_log, output_root_directory)

if __name__ == '__main__':
    main()
