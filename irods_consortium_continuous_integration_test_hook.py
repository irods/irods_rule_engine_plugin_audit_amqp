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
    return get_build_prerequisites_all()+['default-jre', 'uuid-dev', 'libssl-dev', 'libsasl2-2', 'libsasl2-dev', 'python3-dev']

def get_build_prerequisites_yum():
    return get_build_prerequisites_all()+['java-1.7.0-openjdk-devel', 'libuuid-devel', 'openssl-devel', 'cyrus-sasl-devel', 'python3-devel']

def get_build_prerequisites_dnf():
    return get_build_prerequisites_all()+['java-1.8.0-openjdk-devel', 'libuuid-devel', 'openssl-devel', 'cyrus-sasl-devel', 'python3-devel']

def get_build_prerequisites_zypper():
    return get_build_prerequisites_all()+['java-1.7.0-openjdk-devel']

def get_test_packages():
    dispatch_map = {
        'Almalinux': get_build_prerequisites_dnf,
        'Centos linux': get_build_prerequisites_yum,
        'Centos': get_build_prerequisites_yum,
        'Debian gnu_linux': get_build_prerequisites_apt,
        'Opensuse': get_build_prerequisites_zypper,
        'Rocky linux': get_build_prerequisites_dnf,
        'Ubuntu': get_build_prerequisites_apt
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()


def install_apache_activemq(message_broker):
    version_number = message_broker.split('-')[2]
    tarfile = message_broker + '-bin.tar.gz'
    url = 'http://archive.apache.org/dist/activemq/' + version_number + '/' + tarfile
    activemq_dir = message_broker + '/bin/activemq'

    irods_python_ci_utilities.subprocess_get_output(['wget', '-q', url])
    irods_python_ci_utilities.subprocess_get_output(['tar', 'xfz', tarfile])
    irods_python_ci_utilities.subprocess_get_output([activemq_dir, 'start'])


def install_rabbitmq(message_broker):
    if irods_python_ci_utilities.get_distribution() in ['Ubuntu', 'Debian']:
        irods_python_ci_utilities.subprocess_get_output('curl -s https://packagecloud.io/install/repositories/rabbitmq/rabbitmq-server/script.deb.sh | sudo bash', shell=True)
        irods_python_ci_utilities.subprocess_get_output(['wget', '-q', 'https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'dpkg', '-i', 'erlang-solutions_1.0_all.deb'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'update', '-y'])
        if irods_python_ci_utilities.get_distribution_version_major() == '18':
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'esl-erlang', '-y'])
        else:
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'esl-erlang=1:19.3.6', '-y'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'rabbitmq-server', '-y'])

    if irods_python_ci_utilities.get_distribution() in ['Centos', 'Centos linux', 'Almalinux']:
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


def install_messaging_package(message_broker):
    dispatch_map = {
        'apache-activemq': install_apache_activemq,
        'rabbitmq': install_rabbitmq
    }

    for k, _ in dispatch_map.items():
        if k in message_broker:
            return dispatch_map[k](message_broker)

    # If we reach here, it is an error because the message broker was not found in the map.
    raise ValueError(f'unsupported message broker [{message_broker}]')


def main():
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory', help='Path to the directory where logs and other files are written to.')
    parser.add_option('--built_packages_root_directory', help='Path to directory containing the audit plugin package.')
    parser.add_option('--message_broker', default='apache-activemq-5.14.1', help='MQ server package name that needs to be tested.')
    parser.add_option('--test', metavar='dotted name')
    parser.add_option('--skip-setup', action='store_false', dest='do_setup', default=True)
    options, _ = parser.parse_args()

    built_packages_root_directory = options.built_packages_root_directory
    package_suffix = irods_python_ci_utilities.get_package_suffix()
    os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(built_packages_root_directory)

    if options.do_setup:
        irods_python_ci_utilities.install_os_packages(get_test_packages())

        irods_python_ci_utilities.subprocess_get_output(['sudo', '-EH', 'python3', '-m', 'pip', 'install',
                                                         'python-qpid-proton==0.36.0'])

        install_messaging_package(options.message_broker)

        irods_python_ci_utilities.install_os_packages_from_files(
            glob.glob(os.path.join(os_specific_directory,
                                   f'irods-rule-engine-plugin-audit-amqp*.{package_suffix}')
            )
        )

        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c',
            'python3 scripts/add_audit_rule_engine_to_rule_engines.py'],
            check_rc=True)

    test_audit_log = 'log/test_audit_plugin.log'
    test_output_file = 'log/test_output.log'

    test = options.test or 'test_audit_plugin'

    try:
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c',
            f'python3 scripts/run_tests.py --xml_output --run_s={test} 2>&1 | tee {test_audit_log}; exit $PIPESTATUS'],
            check_rc=True)

    finally:
        if options.output_root_directory:
            output_root_directory = os.path.join(options.output_root_directory, options.message_broker)
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/log/irods', output_root_directory, lambda x: True)
            test_output_file = os.path.join('/var/lib/irods', test_output_file)
            if os.path.exists(test_output_file):
                shutil.copy(test_output_file, output_root_directory)
            test_audit_log = os.path.join('/var/lib/irods', test_audit_log)
            if os.path.exists(test_audit_log):
                shutil.copy(test_audit_log, output_root_directory)

if __name__ == '__main__':
    main()
