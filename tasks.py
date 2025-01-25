# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from getpass import getpass
from invoke import task

import util.microservice_util as ms_util


@task
def init(c):
    """Initialize/setup the development environment."""
    print(("Setting up connection to Cumulocity development environment.\n"
           "(TOTP protected accouts are not supported)"))
    host, tenant, user, password = (
        input("Hostname: "),
        input("Tenant:   "),
        input("Username: "),
        getpass("Password: "),
    )
    print("Optional: device bootstrapping credentials (leave blank to skip).")
    bootstrap_tenant, bootstrap_user, bootstrap_password = (
        input(f"Tenant (default: management):        "),
        input(f"Username (default: devicebootstrap): "),
        getpass("Password: "),
    )

    with open('.env', 'wt') as file:
        file.writelines([
            f'C8Y_BASEURL={host}\n',
            f'C8Y_TENANT={tenant}\n',
            f'C8Y_USER={user}\n',
            f'C8Y_PASSWORD={password}\n',
            '\n',
            f'C8Y_BOOTSTRAP_TENANT={tenant}\n',
            f'C8Y_BOOTSTRAP_USER={user}\n',
            f'C8Y_BOOTSTRAP_PASSWORD={password}\n',
            '\n',
            f'C8Y_DEVICEBOOTSTRAP_TENANT={bootstrap_tenant or "management"}\n',
            f'C8Y_DEVICEBOOTSTRAP_USER={bootstrap_user or "devicebootstrap"}\n',
            f'C8Y_DEVICEBOOTSTRAP_PASSWORD={bootstrap_password or ""}\n',
        ])


@task(help={
    'scope': ("Which source directory to check, can be one of 'c8y_api', "
              "'tests', 'integration_tests' or 'all'. Default: 'all'")
})
def lint(c, scope='all'):
    """Run PyLint."""
    if scope == 'all':
        scope = 'c8y_api c8y_tk tests integration_tests samples'
    c.run(f'pylint --rcfile pylintrc --fail-under=9 {scope}')


@task(help={
    'scope': "Which tests to run, e.g. 'tests.model'"
             "Default: 'tests'",
    'python': ("Whether to run tests within a docker container and which"
               "Python version to use - 3.7 or 3.11. Default: None")
})
def test(c, scope='tests', python=None):

    docker_name = None
    if python == '3.7':
        docker_name = 'buster37'
    elif python == '3.11':
        docker_name = 'bookworm311'
    if docker_name:
        dockerfile = f'{docker_name}.dockerfile'
        cmd_build = f'docker build -f {dockerfile} -t {docker_name} .'
        print(f"Executing '{cmd_build}' ...")
        c.run(cmd_build, pty=True)
        cmd_run = ('docker run --rm -it -v $(pwd):/code --env-file .env '
                   f'{docker_name} bash -c "cd /code '
                   '&& pip show c8y_api || pip install -e . '
                   f'&& pytest -W ignore::DeprecationWarning {scope}"')
        print(f"Executing '{cmd_run}' ...")
        c.run(cmd_run, pty=True)
    else:
        c.run(f"pytest -W ignore::DeprecationWarning {scope}")


@task
def build(c):
    """Build the module.

    This will create a distributable wheel (.whl) file.
    """
    c.run('python -m build')


@task(help={
    'clean': "Whether to clean the output before generation."
})
def build_docs(c, clean=False):
    """Build the documentation (HTML)."""
    dist_dir = 'dist/docs'
    docs_dir = 'docs'
    if clean:
        c.run(f'sphinx-build -M clean "{docs_dir}"  "{dist_dir}"')
    c.run(f'sphinx-build -M html "{docs_dir}"  "{dist_dir}"')


@task(help={
    'sample': "Which sample to build.",
    'name': "Microservice name. Defaults to sample name.",
    "version": "Microservice version. Defaults to '1.0.0'.",
})
def build_ms(c, sample, name=None, version='1.0.0'):
    """Build a Cumulocity microservice binary for upload.

    This will build a ready to deploy Cumulocity microservice from a sample
    file within the `samples` folder. Any sample Python script can be used
    (if it implements microservice logic).

    By default, uses the file name without .py extension as name. The built
    microservice will use a similar name, following Cumulocity guidelines.
    """
    sample_name = ms_util.format_sample_name(sample)
    c.run(f'samples/build.sh {sample_name} {version} {name if name else ""}')


@task(help={
    'sample': "Which sample to register."
})
def register_ms(_, sample):
    """Register a sample as microservice at Cumulocity."""
    ms_util.register_microservice(ms_util.format_sample_name(sample))


@task(help={
    'sample': "Which sample to unregister."
})
def unregister_ms(_, sample):
    """Unregister a sample microservice from Cumulocity."""
    ms_util.unregister_microservice(ms_util.format_sample_name(sample))


@task(help={
    'sample': "Which sample to register."
})
def get_credentials(_, sample):
    """Unregister a sample microservice from Cumulocity."""
    tenant, user, password = ms_util.get_credentials(ms_util.format_sample_name(sample))
    print(f"Tenant:    {tenant}\n"
          f"Username:  {user}\n"
          f"Password:  {password}")


@task(help={
    'sample': "Which sample to create a .env file for."
})
def create_env(_, sample):
    """Create a sample specific .env-{sample_name} file using the
    credentials of a corresponding microservice registered at Cumulocity."""
    sample_name = ms_util.format_sample_name(sample)
    tenant, user, password = ms_util.get_credentials(sample_name)
    with open(f'.env-{sample_name}', 'w', encoding='UTF-8') as f:
        f.write(f'C8Y_TENANT={tenant}\n'
                f'C8Y_USER={user}\n'
                f'C8Y_PASSWORD={password}\n')
