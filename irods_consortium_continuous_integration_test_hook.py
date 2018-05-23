from __future__ import print_function

import optparse
import os
import shutil
import glob
import time
import tempfile
import irods_python_ci_utilities


def get_build_prerequisites_all():
    return[]


def get_build_prerequisites_apt():
    if irods_python_ci_utilities.get_distribution_version_major() == '12':
        return['openjdk-7-jre']+get_build_prerequisites_all()

    else:
        return['default-jre']+get_build_prerequisites_all()


def get_build_prerequisites_yum():
    return['java-1.7.0-openjdk-devel']+get_build_prerequisites_all()


def get_build_prerequisites_zypper():
    return['java-1.7.0-openjdk-devel']+get_build_prerequisites_all()


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
    irods_python_ci_utilities.subprocess_get_output(['sudo', '-EH', 'pip', 'install', 'unittest-xml-reporting==1.14.0'])

    if irods_python_ci_utilities.get_distribution() == 'Ubuntu': # cmake from externals requires newer libstdc++ on ub12
        if irods_python_ci_utilities.get_distribution_version_major() == '12':
            irods_python_ci_utilities.install_os_packages(['python-software-properties'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'add-apt-repository', '-y', 'ppa:ubuntu-toolchain-r/test'], check_rc=True)
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'update-java-alternatives', '--set', 'java-1.7.0-openjdk-amd64'])
            irods_python_ci_utilities.install_os_packages(['libstdc++6'])

    irods_python_ci_utilities.install_os_packages(get_build_prerequisites())


def install_qpid_proton():
    local_qpid_proton_dir = tempfile.mkdtemp(prefix='qpid_proton_dir')
    irods_python_ci_utilities.git_clone('https://github.com/apache/qpid-proton.git', '0.17.0', local_qpid_proton_dir)

    if irods_python_ci_utilities.get_distribution() == 'Ubuntu':
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'gcc', 'cmake', 'cmake-curses-gui', 'uuid-dev', '-y'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'libssl-dev', '-y'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'libsasl2-2','libsasl2-dev', '-y'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'swig', 'python-dev', 'ruby-dev', 'libperl-dev', '-y'])
    else:
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'yum', 'install', 'gcc', 'make', 'cmake', 'libuuid-devel', '-y'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'yum', 'install', 'openssl-devel', '-y'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'yum', 'install', 'cyrus-sasl-devel', '-y'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'yum', 'install', 'swig', 'python-devel', 'ruby-devel', 'rubygem-minitest', 'php-devel', 'perl-devel', '-y'])

    qpid_proton_build_dir = local_qpid_proton_dir + '/build'
    if not os.path.exists(qpid_proton_build_dir):
        os.makedirs(qpid_proton_build_dir)
        os.chdir(qpid_proton_build_dir)
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'cmake', '..', '-DCMAKE_INSTALL_PREFIX=/usr', '-DSYSINSTALL_BINDINGS=ON'])
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'make', 'install'])


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
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'esl-erlang=1:19.3.6', '-y'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'apt-get', 'install', 'rabbitmq-server', '-y'])

        if irods_python_ci_utilities.get_distribution() == 'Centos' or irods_python_ci_utilities.get_distribution() == 'Centos linux':
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

    output_root_directory = options.output_root_directory
    built_packages_root_directory = options.built_packages_root_directory
    package_suffix = irods_python_ci_utilities.get_package_suffix()
    os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(built_packages_root_directory)

    irods_python_ci_utilities.install_os_packages_from_files(glob.glob(os.path.join(os_specific_directory, 'irods-rule-engine-plugin-audit-amqp*.{0}'.format(package_suffix))))

    install_build_prerequisites()
    install_messaging_package(options.message_broker)
    install_qpid_proton()

    time.sleep(10)

    try:
        test_output_file = 'log/test_output.log'
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'python2 scripts/run_tests.py --xml_output --run_s=test_audit_plugin 2>&1 | tee {0}; exit $PIPESTATUS'.format(test_output_file)], check_rc=True)
        irods_python_ci_utilities.subprocess_get_output(['sudo', 'su', '-', 'irods', '-c', 'python2 scripts/run_tests.py --xml_output --run_s=test_resource_types.Test_Resource_Unixfilesystem 2>&1 | tee {0}; exit $PIPESTATUS'.format(test_output_file)], check_rc=True)
    finally:
        if output_root_directory:
            irods_python_ci_utilities.gather_files_satisfying_predicate('/var/lib/irods/log', output_root_directory, lambda x: True)
            shutil.copy('/var/lib/irods/log/test_output.log', output_root_directory)


if __name__ == '__main__':
    main()
